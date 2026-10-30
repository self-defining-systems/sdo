package runtime

import (
	"context"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"path/filepath"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

// Whole-incident observation is never negative application evidence.
func TestSettlementUnconfirmedApplicationIsNeutral(t *testing.T) {
	for _, classification := range []string{"unconfirmed", "failed", "cleared_without_action", "success"} {
		outcome := &contracts.OutcomeRecord{Classification: classification, SourceCommit: "synthetic", PlanApplications: []*contracts.PlanApplication{{ApplicationId: "a", Applied: true, ProgramIds: []string{"p"}, VerificationCompleted: true, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}}}
		if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})["p"]; got.Failures != 0 || got.VerifiedSuccesses != 0 {
			t.Fatalf("%s: observation became evidence: %+v", classification, got)
		}
	}
}

func TestSettlementReleasedReservationOnlySupersededByCausalWrite(t *testing.T) {
	loop, _ := pipelineTestLoop(t, false)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	loop.pendingWrites = map[string]pendingMitigation{"a": {Refs: []sdk.ObjectRef{ref}, Released: true}}
	loop.pendingConflict(context.Background(), []sdk.ObjectRef{{Kind: "ConfigMap", Namespace: "app", Name: "peer"}}, &ToolResponse{})
	if loop.pendingWrites["a"].Superseded {
		t.Fatal("independent cause superseded pending repair")
	}
	loop.pendingConflict(context.Background(), []sdk.ObjectRef{ref}, &ToolResponse{})
	if !loop.pendingWrites["a"].Superseded {
		t.Fatal("same-resource write did not supersede repair")
	}
}

func TestSettlementWatchingResponderSkipsLearningWorker(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result.ResponderSessionID = "11111111-2222-4333-8444-555555555555"
	result.ResolutionPath = "responder"
	result, stages := loop.SettleRecoveryApplications(context.Background(), result, state.PriorStages)
	closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
	closure.Request.PriorStages = stages
	if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
		t.Fatalf("watching session dispatched %d learning workers", calls)
	}
}

// The real stage producer runs all writes first, then observes one shared
// deadline. A logical minute costs milliseconds; no worker/model is involved.
func TestHermeticSavedRepeatWaitsForSharedMinuteWithoutModels(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Detectors = append(loop.Detectors, &pipelineLaggingHealthDetector{remaining: 8})
	start := time.Now()
	now := start
	loop.Executor.Rollouts = &settle2DelayedRollout{base: loop.Executor.Rollouts, now: func() time.Time { return now }, ready: start.Add(time.Minute)}
	stage := &DeterministicResponseStage{ObservationInterval: time.Millisecond, recoveryNow: func() time.Time { return now }, recoveryWait: func(context.Context, time.Duration) error { now = now.Add(15 * time.Second); return nil }}
	dispatcher := &settle2ControllerDispatcher{pipelineScenarioDispatcher: pipelineScenarioDispatcher{loop: loop}, stage: stage}
	config := testControllerConfig()
	config.Application = state.Request.Application
	config.Namespace = "app"
	config.SourceCommit = state.Request.SourceCommit
	config.DeployedCommit = state.Request.DeployedCommit
	config.RepositoryWorktree = t.TempDir()
	config.RepairPolicy = "recorded-actions"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	config.BatchDebounce = time.Millisecond
	config.ResponseTimeout = 3 * time.Minute
	controller, err := NewController(config, []sdk.Detector{pipelineDetector{}}, loop.Provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.SetLifecycleSink(&memoryLifecycleSink{})
	dispatcher.controller = controller
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(c IncidentClosure) { closed <- c }
	for i := 0; i < 3; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("stage not admitted")
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	awaitDispatchCompletionQueued(t, controller)
	if now.Sub(start) != time.Minute {
		t.Fatalf("shared wait=%s", now.Sub(start))
	}
	for i := 60; i < 80; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case closure := <-closed:
		if dispatcher.agentCalls != 0 {
			t.Fatalf("fixing workers=%d", dispatcher.agentCalls)
		}
		derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
		if err != nil {
			t.Fatal(err)
		}
		if derived.Record.Classification != "success" || !coveredRepeatCandidate(derived.Record) {
			t.Fatalf("covered recovery lost: %v", derived.Record)
		}
		if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
			t.Fatalf("learning workers=%d", calls)
		}
	default:
		t.Fatal("independent controller recovery did not close")
	}
}

type settle2ControllerDispatcher struct {
	pipelineScenarioDispatcher
	controller *Controller
	stage      *DeterministicResponseStage
}

func (d *settle2ControllerDispatcher) DeterministicResponseStage() ResponseStage {
	d.stage.NewLoop = func(request IncidentRequest) *ResponderLoop {
		d.loop.Request = request
		d.controller.mu.Lock()
		d.controller.responderLoop = d.loop
		d.controller.mu.Unlock()
		return d.loop
	}
	return d.stage
}

type settle2DelayedRollout struct {
	base  PlanRollouts
	now   func() time.Time
	ready time.Time
}

func (r *settle2DelayedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Observe(ctx, refs)
}
func (r *settle2DelayedRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.now().Before(r.ready) {
		return context.DeadlineExceeded
	}
	return r.base.Wait(ctx, refs)
}

// Execute through the stage, persist/reload its real receipts, then consume the
// recovery projection through outcome derivation, learning and trust.
func TestSettlementProducerConsumerSweep(t *testing.T) {
	for _, session := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			for _, delay := range []time.Duration{0, 45 * time.Second, 90 * time.Second} {
				t.Run(fmt.Sprintf("session=%v/restart=%v/delay=%s", session, restart, delay), func(t *testing.T) {
					loop, state := pipelineTestLoop(t, false)
					loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
					var saved []byte
					loop.saveToolState = func(_ context.Context, b []byte) error { saved = append([]byte(nil), b...); return nil }
					if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
						t.Fatal(err)
					}
					before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
					if err != nil {
						t.Fatal(err)
					}
					if session {
						before.ResponderSessionID = "11111111-2222-4333-8444-555555555555"
						before.ResolutionPath = "responder"
					}
					if restart {
						restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
						restored.ReadView = loop.ReadView
						if err := restored.restoreToolSnapshot(saved); err != nil {
							t.Fatal(err)
						}
						loop = restored
					}
					loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
					result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
					if restart && (len(loop.plans) != 0 || len(loop.applications) != 0) {
						t.Fatal("settlement restored execution authority")
					}
					at := time.Now().Add(delay)
					closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: at, HealthClearedAt: &at, FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: at}}, Frozen: true}
					closure.Request.PriorStages = stages
					derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
					if err != nil {
						t.Fatal(err)
					}
					if derived.Record.Classification != "success" || !coveredRepeatCandidate(derived.Record) {
						t.Fatalf("attributable recovery lost: %v", derived.Record)
					}
					stats := ProgramTrustStats([]*contracts.OutcomeRecord{derived.Record})
					for _, app := range derived.Record.PlanApplications {
						for _, id := range app.ProgramIds {
							if got := stats[id]; got.Failures != 0 || got.VerifiedSuccesses != 1 {
								t.Fatalf("application settlement lost trust for %s: %+v", id, got)
							}
						}
					}
					if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
						t.Fatalf("covered recovery dispatched %d workers", calls)
					}
				})
			}
		}
	}
}

func TestSettlementAdverseEvidenceSweep(t *testing.T) {
	for _, scenario := range []string{"unknown", "truncated", "partial", "rollback", "same-resource", "independent-resource", "inside-apply", "timed-out", "missing-result", "owned-child"} {
		t.Run(scenario, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			app := &loop.applicationsLog[0]
			id := app.ID
			ref, _ := planActionRef(app.Result.Plan.Actions[0])
			switch scenario {
			case "missing-result":
				before.PlanApplications = nil
			case "unknown":
				specs := loop.Programs.List()
				ds := []program.Diagnoser{}
				for _, spec := range specs {
					ds = append(ds, settle2UnknownDiagnoser{spec: spec})
				}
				loop.Programs, err = program.NewRegistry(ds, nil)
				if err != nil {
					t.Fatal(err)
				}
			case "truncated":
				app.Result.ActionReports = nil
				app.Result.PreImages = nil
			case "partial":
				app.Result.ActionReports[0].Status = "partial"
				app.Result.ActionReports[0].FailureReason = "execution_failed"
				app.Error = "synthetic partial execution failure"
			case "rollback":
				app.RolledBack = true
				app.Result.RolledBack = true
			case "owned-child":
				yes := true
				rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "synthetic-child-set", Namespace: "app", UID: "synthetic-child-set", OwnerReferences: []metav1.OwnerReference{{APIVersion: ref.APIVersion, Kind: ref.Kind, Name: ref.Name, UID: "synthetic-deployment", Controller: &yes}}}}
				if _, err := loop.Executor.Client.(TypedPlanClient).Client.AppsV1().ReplicaSets("app").Create(context.Background(), rs, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "synthetic-child"}
				before.RepairActions = append(before.RepairActions, RepairActionReceipt{ActionID: "synthetic-child-correction", Kind: "recorded-action", Resources: []sdk.ObjectRef{pod}, Owners: []ResourceOwners{{Resource: pod, OwnerChain: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: rs.Name}}}}, StartedAt: app.CompletedAt.Add(time.Second), CompletedAt: app.CompletedAt.Add(2 * time.Second), Success: true})
			case "same-resource", "independent-resource":
				if scenario == "independent-resource" {
					ref = sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "peer"}
				}
				before.RepairActions = append(before.RepairActions, RepairActionReceipt{ActionID: "synthetic-later-write", Kind: "recorded-action", Resources: []sdk.ObjectRef{ref}, StartedAt: app.CompletedAt.Add(time.Second), CompletedAt: app.CompletedAt.Add(2 * time.Second), Success: true})
			case "inside-apply":
				app.CompletedAt = time.Now().Add(time.Second)
			case "timed-out":
				app.Error = "rollout_stalled: observation timed out"
				p := loop.pendingWrites[id]
				p.Released = true
				loop.pendingWrites[id] = p
			}
			loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
			result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
			var settled RecordedPlanApplication
			for _, candidate := range result.PlanApplications {
				if candidate.ID == id {
					settled = candidate
				}
			}
			want := scenario == "independent-resource" || scenario == "inside-apply" || scenario == "timed-out" || scenario == "missing-result"
			if got := settled.Result.FinalVerificationBasis == "recovery-settled"; got != want {
				t.Fatalf("settled=%v want=%v result=%+v stages=%v", got, want, settled, stages)
			}
			at := time.Now().Add(3 * time.Second)
			clearAt := at
			if scenario == "inside-apply" {
				clearAt = settled.StartedAt.Add(time.Nanosecond)
			}
			closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: at, HealthClearedAt: &clearAt, FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: at}}, Frozen: true}
			closure.Request.PriorStages = stages
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
			if err != nil {
				t.Fatal(err)
			}
			if want && derived.Record.Classification != "success" {
				t.Fatalf("attributed repair became %s", derived.Record.Classification)
			}
			// The durable producer projection is the trust consumer's only evidence.
			typed, err := incidentResultContract(result)
			if err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{Classification: "success", SourceCommit: state.Request.SourceCommit, PlanApplications: typed.PlanApplications, ProgramRuns: typed.ProgramRuns, PriorStages: stages}
			stats := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})
			for _, programID := range settled.ProgramIDs {
				got := stats[programID]
				if want && got.VerifiedSuccesses != 1 {
					t.Fatalf("settled recovery lost credit: %+v", got)
				}
				if !want && got.VerifiedSuccesses != 0 {
					t.Fatalf("unsettled application earned credit: %+v", got)
				}
				failure := scenario == "partial" || scenario == "rollback"
				if (got.Failures > 0) != failure {
					t.Fatalf("adverse evidence failure=%v stats=%+v", failure, got)
				}
			}
		})
	}
}

type settle2UnknownDiagnoser struct{ spec program.ProgramSpec }

func (p settle2UnknownDiagnoser) Spec() program.ProgramSpec { return p.spec }
func (p settle2UnknownDiagnoser) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Unknown}, nil
}

func TestSettlementRecoveryDeadlineSurvivesRestart(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	start := time.Now()
	now := start
	loop.Executor.Rollouts = &settle2DelayedRollout{base: loop.Executor.Rollouts, now: func() time.Time { return now }, ready: start.Add(2 * time.Minute)}
	var saved []byte
	loop.saveToolState = func(_ context.Context, b []byte) error { saved = append([]byte(nil), b...); return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: 45 * time.Second, recoveryNow: func() time.Time { return now }, recoveryWait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, ObservationInterval: 15 * time.Second}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	deadline := loop.stageProgress[stage.Name()].RecoveryDeadline
	if !deadline.Equal(start.Add(45 * time.Second)) {
		t.Fatalf("deadline=%s", deadline)
	}
	restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	restored.ReadView = loop.ReadView
	if err := restored.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	now = start.Add(time.Minute)
	waits := 0
	stage.NewLoop = func(IncidentRequest) *ResponderLoop { return restored }
	stage.recoveryWait = func(context.Context, time.Duration) error { waits++; return nil }
	state.PriorStages = []*contracts.StageReport{report}
	if _, err := stage.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if waits != 0 || !restored.stageProgress[stage.Name()].RecoveryDeadline.Equal(deadline) {
		t.Fatalf("restart replenished observation: waits=%d deadline=%s", waits, restored.stageProgress[stage.Name()].RecoveryDeadline)
	}
	if len(restored.plans) != 0 || len(restored.applications) != 0 {
		t.Fatal("observation restore recreated execution authority")
	}
}

func TestSettlementCannotReuseProofAfterSupersedingWrite(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result, stages := loop.SettleRecoveryApplications(context.Background(), result, state.PriorStages)
	app := result.PlanApplications[0]
	ref, _ := planActionRef(app.Result.Plan.Actions[0])
	result.RepairActions = append(result.RepairActions, RepairActionReceipt{ActionID: "synthetic-competing-write", Kind: "recorded-action", Resources: []sdk.ObjectRef{ref}, StartedAt: app.CompletedAt.Add(time.Second), CompletedAt: app.CompletedAt.Add(2 * time.Second), Success: true})
	result, stages = loop.SettleRecoveryApplications(context.Background(), result, stages)
	for _, candidate := range result.PlanApplications {
		if candidate.ID == app.ID && candidate.Result.FinalVerificationBasis == "recovery-settled" {
			t.Fatal("reused stale settlement after later corrective write")
		}
	}
	for _, receipt := range result.RepairActions {
		if receipt.ActionID == "sdo-program-"+app.ID && receipt.Success {
			t.Fatal("superseded application retained successful repair receipt")
		}
	}
	for _, stage := range stages {
		for _, attempt := range stage.Mitigations {
			if attempt.GetApplyReceipt().GetApplicationId() == app.ID && attempt.Result == "applied_cleared" {
				t.Fatal("superseded application retained covered work")
			}
		}
	}
}

func TestSettlementSingleCauseRecoversUnknownAndMatchingObservations(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(fmt.Sprintf("health-observed=%v", observed), func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			client := loop.Executor.Client.(TypedPlanClient).Client
			deployment, err := client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// Construct a single-fault opening fixture before any incident write.
			deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
			if _, err := client.AppsV1().Deployments("app").Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			view, err := loop.Provider.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			findings, err := loop.Detectors[0].Detect(context.Background(), view)
			if err != nil || len(findings) != 1 {
				t.Fatalf("single fault: %v %v", findings, err)
			}
			loop.Request.Findings = findings
			loop.findings = findings
			loop.runs = nil
			loop.Request.InitialProgramRuns = loop.InitialDiagnose(context.Background())
			loop.Request.PlaybookCoverage = buildPlaybookCoverage(findings, loop.Request.InitialProgramRuns, loop.lastDiagnosisView, nil)
			state.Request = loop.Request
			state.Findings = findings
			state.Coverage = loop.Request.PlaybookCoverage
			loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if len(loop.applicationsLog) != 1 {
				t.Fatalf("applications=%d", len(loop.applicationsLog))
			}
			loop.applicationsLog[0].Result.VerificationCompleted = observed
			loop.applicationsLog[0].Result.VerificationPassed = false
			loop.Executor.Rollouts = observedDeploymentRollout{client: client}
			result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
			closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
			closure.Request.PriorStages = stages
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
			if err != nil {
				t.Fatal(err)
			}
			if derived.Record.Classification != "success" || !coveredRepeatCandidate(derived.Record) {
				t.Fatalf("single repaired cause lost: %v", derived.Record)
			}
			if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
				t.Fatalf("learning workers=%d", calls)
			}
		})
	}
}

func TestSettlementVerifiedBlockerHandsOffAfterAllWrites(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Executor.Rollouts = settle2BlockedRollout{}
	start := time.Now()
	now := start
	waits := 0
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryNow: func() time.Time { return now }, recoveryWait: func(context.Context, time.Duration) error { waits++; now = now.Add(30 * time.Second); return nil }}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(loop.applicationsLog) != 2 || len(report.Mitigations) != 2 {
		t.Fatal("blocking dependency starved an admitted write")
	}
	if waits != 0 {
		t.Fatalf("verified blocker consumed %d recovery waits", waits)
	}
	for _, attempt := range report.Mitigations {
		if attempt.GetApplyReceipt().GetRolloutPending().GetReasonCode() != "rollout_stalled" {
			t.Fatalf("blocking dependency lost its durable type: %v", attempt)
		}
	}
}

type settle2BlockedRollout struct{}

func (settle2BlockedRollout) Wait(context.Context, []sdk.ObjectRef) error {
	return &rolloutBlockedError{cause: errRolloutStalled}
}
func (r settle2BlockedRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Wait(ctx, refs)
}

func TestSettlementObservationPreservesWriteInterval(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
		t.Fatal(err)
	}
	times := map[string]time.Time{}
	for _, app := range loop.ApplicationEvidence() {
		times[app.ID] = app.CompletedAt
	}
	loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
	loop.Handle(context.Background(), ToolRequest{Command: "status"})
	for _, app := range loop.ApplicationEvidence() {
		if !app.CompletedAt.Equal(times[app.ID]) {
			t.Fatalf("observation rewrote write interval for %s: %s -> %s", app.ID, times[app.ID], app.CompletedAt)
		}
	}
}

func TestSettlementNonpendingHealthObservationIsNeutral(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, false)
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || loop.PlanApplications()[0].Result.VerificationPassed {
		t.Fatal("fixture must produce a landed unconfirmed repair")
	}
	runs := loop.ProgramRuns()
	fillProgramRevisions(context.Background(), runs, loop.Request.RepositoryWorktree, loop.Request.SourceCommit)
	typed, err := incidentResultContract(IncidentResult{PlanApplications: loop.ApplicationEvidence(), ProgramRuns: runs, ProgramFailureSignals: loop.FailureSignals()})
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{Classification: "unconfirmed", SourceCommit: loop.Request.SourceCommit, PlanApplications: typed.PlanApplications, ProgramRuns: typed.ProgramRuns, ProgramFailureSignals: typed.ProgramFailureSignals}
	if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})["service-targetport-mismatch"]; got.Failures != 0 || got.VerifiedSuccesses != 0 {
		t.Fatalf("health observation became adverse execution evidence: %+v signals=%v", got, loop.FailureSignals())
	}
}

func TestSettlementSavedScriptKeepsObservedTransitionProof(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}})
	provider := fastloopObjects{client: client}
	detector := pipelineDetector{}
	learned := scriptVerifyProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{learned}, []program.Mitigator{learned})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	root, commit := acceptedScriptMemory(t, learned.Spec().ID, `kubectl patch service "$SERVICE" --type merge -p "$PATCH"`)
	request := IncidentRequest{IncidentID: "settle-saved-script", Namespace: "app", RepairPolicy: "recorded-actions", Findings: findings, RepositoryWorktree: root, SourceCommit: commit, ResponseDeadline: time.Now().Add(time.Minute)}
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, PlanExecutor{Client: verifyingScriptClient{TypedPlanClient{Client: client, ScriptImage: "script-tools:fixture"}}, Rollouts: &pipelineDeadlineRollout{failure: context.DeadlineExceeded}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider}}, 3)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	state := &IncidentState{Request: request, Findings: findings}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
		t.Fatal(err)
	}
	before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	loop.Executor.Rollouts = generalNoRollouts{}
	result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
	if len(result.PlanApplications) != 1 || result.PlanApplications[0].Result.FinalVerificationBasis != "observed-transition" {
		t.Fatalf("saved script did not settle: applications=%+v stages=%v", result.PlanApplications, stages)
	}
	if result.PlanApplications[0].Result.FinalEquivalenceVerified != nil {
		t.Fatal("opaque script claimed exact field equivalence")
	}
}

func TestSettlementIgnoresConfirmedNoopObservationFields(t *testing.T) {
	loop, _ := pipelineTestLoopWithServices(t, false, "peer")
	client := loop.Executor.Client.(TypedPlanClient).Client
	peer, err := client.CoreV1().Services("app").Get(context.Background(), "peer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	peer.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	if _, err := client.CoreV1().Services("app").Update(context.Background(), peer, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	first := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	second := first
	second.Name = "peer"
	plan := program.Plan{}
	for _, ref := range []sdk.ObjectRef{first, second} {
		plan.Actions = append(plan.Actions, program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: program.FromSource(ref, "spec.ports[0].targetPort"), Reason: "restore accepted port"})
	}
	result, err := loop.Executor.Apply(context.Background(), loop.Request, plan, false)
	if err != nil {
		t.Fatal(err)
	}
	get := func(ref sdk.ObjectRef) *unstructured.Unstructured {
		object, err := loop.Executor.Client.Get(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		return object
	}
	if finalPlanFieldsMatch(result, get) {
		t.Fatal("fixture must expose the old unchanged-object equivalence rejection")
	}
	if !loop.recoveryPlanFieldsMatch(result, get, func(sdk.ObjectRef) bool { return false }) {
		t.Fatal("confirmed unchanged peer prevented exact repair settlement")
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(9090)
	if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if loop.recoveryPlanFieldsMatch(result, get, func(sdk.ObjectRef) bool { return false }) {
		t.Fatal("changed desired fields borrowed an observation-only peer's proof")
	}
}

func TestSettlementRolloutSamplePreservesOrdinaryReadbackLatency(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Executor.Rollouts = settle2SlowObservation{base: loop.Executor.Rollouts}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryWait: func(context.Context, time.Duration) error {
		return fmt.Errorf("ordinary readback was incorrectly timed out before the shared recovery wait")
	}}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range report.Mitigations {
		if attempt.Result != "applied_cleared" {
			t.Fatalf("ordinary readback latency became unsettled repair: %v", attempt)
		}
	}
}

type settle2SlowObservation struct{ base PlanRollouts }

func (r settle2SlowObservation) Wait(context.Context, []sdk.ObjectRef) error {
	return fmt.Errorf("a single observation invoked the serial rollout wait")
}
func (r settle2SlowObservation) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	timer := time.NewTimer(3 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return r.base.Wait(ctx, refs)
}

func TestSettlementAgentApplicationsShareTheRecoveryOwner(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("restart=%v/batch=%v", restart, batch), func(t *testing.T) {
				loop, state := pipelineTestLoop(t, false)
				loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
				var saved []byte
				loop.saveToolState = func(_ context.Context, b []byte) error { saved = append([]byte(nil), b...); return nil }
				var planIDs []string
				for _, match := range stageCandidates(state) {
					planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: match.PlaybookId, Bindings: programBindingsState(match.Bindings), FindingFingerprints: match.targets})
					if planned.Error != "" {
						t.Fatal(planned.Error)
					}
					if batch {
						planIDs = append(planIDs, planned.PlanID)
						continue
					}
					applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
					if applied.ApplicationID == "" {
						t.Fatalf("agent did not write: %+v", applied)
					}
				}
				if batch {
					applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: planIDs})
					if applied.ApplicationID == "" {
						t.Fatalf("agent batch did not write: %+v", applied)
					}
				}
				before := IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: state.Request.IncidentID, Status: IncidentCompleted, ResponderSessionID: "11111111-2222-4333-8444-555555555555", ResolutionPath: "responder", Timing: TimingMetrics{StartedAt: time.Now().Add(-time.Second), CompletedAt: time.Now()}, PlanApplications: loop.ApplicationEvidence(), ProgramRuns: loop.ProgramRuns(), ProgramToolCalls: loop.ToolCalls(), ProgramFailureSignals: loop.FailureSignals()}
				if restart {
					restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
					restored.ReadView = loop.ReadView
					if err := restored.restoreToolSnapshot(saved); err != nil {
						t.Fatal(err)
					}
					loop = restored
				}
				loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
				result, stages := loop.SettleRecoveryApplications(context.Background(), before, nil)
				closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
				closure.Request.PriorStages = stages
				derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
				if err != nil {
					t.Fatal(err)
				}
				if derived.Record.Classification != "success" || !coveredRepeatCandidate(derived.Record) {
					t.Fatalf("agent's saved application bypassed settlement: class=%s stages=%v", derived.Record.Classification, stages)
				}
				if calls := settlementBrokerWorkers(t, loop, closure); calls != 0 {
					t.Fatalf("covered agent application dispatched %d learning workers", calls)
				}
				if restart && (len(loop.plans) > 0 || len(loop.applications) > 0) {
					t.Fatal("settlement restored agent execution authority")
				}
			})
		}
	}
}

func TestSettlementObservesRecordedAPIWritesBeforeDerivation(t *testing.T) {
	for _, kind := range []string{"watching", "own-write", "later-write", "incomplete", "unpaired", "partial"} {
		t.Run(kind, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			before.ResponderSessionID = "11111111-2222-4333-8444-555555555555"
			root := t.TempDir()
			if kind != "incomplete" {
				sub, err := memory.PrepareIncidentSession(root, "", state.Request.IncidentID)
				if err != nil {
					t.Fatal(err)
				}
				store, err := apihistory.Open(filepath.Join(root, sub, "api-history"), state.Request.IncidentID, apihistory.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				app := loop.applicationsLog[0]
				started := app.CompletedAt.Add(time.Nanosecond)
				finished := started.Add(time.Nanosecond)
				verb := "get"
				if kind == "own-write" {
					verb = "patch"
					started = app.StartedAt.Add(time.Nanosecond)
					finished = app.CompletedAt.Add(-time.Nanosecond)
				}
				if kind == "later-write" || kind == "unpaired" || kind == "partial" {
					verb = "patch"
				}
				event := apihistory.Event{RequestID: "synthetic-api", Phase: "start", Started: started, Verb: verb, Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "api"}
				store.Record(event, nil)
				event.Phase = "end"
				event.Finished = finished
				event.Status = 200
				event.Partial = kind == "partial"
				if kind != "unpaired" {
					store.Record(event, nil)
				}
				store.Close()
			}
			loop.RecoverySessionRoot = root
			loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
			result, _ := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
			want := kind == "watching" || kind == "own-write"
			if got := result.PlanApplications[0].Result.FinalVerificationBasis == "recovery-settled"; got != want {
				t.Fatalf("history=%s settlement=%v want=%v", kind, got, want)
			}
		})
	}
}
