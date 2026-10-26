package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

// The incident detector that supplied opening coverage need not be one of the
// health detectors installed in the responder loop. Its opening finding is not
// a fresh post-write observation.
func TestDeterministicResponseStageUsesPostApplyEvidenceForOpeningIncidentFinding(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	for i := range state.Request.Findings {
		if state.Request.Findings[i].PrimaryResource.Kind == "Service" {
			state.Request.Findings[i].Fingerprint = FindingFingerprint(state.Request.Findings[i])
			state.Request.Findings[i].DetectorID = "incident.port"
		}
	}
	state.Findings = append([]sdk.Finding(nil), state.Request.Findings...)
	loop.Request = state.Request
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := report.Mitigations[0]
	if attempt.PostApplyDiagnose.GetVerdict().GetResult() != contracts.Verdict_RESULT_NO_MATCH || !attempt.ApplyReceipt.GetVerificationPassed() {
		t.Fatalf("fixture did not repair: %v", attempt)
	}
	if attempt.Result != "applied_cleared" || len(attempt.RemainingFindingFingerprints) != 0 || len(state.Findings) != 1 {
		t.Fatalf("stale opening finding survived post-apply proof: report=%v findings=%v", report, state.Findings)
	}
}

type pipelineIdentitylessDetector struct{ pipelineDetector }

func (d pipelineIdentitylessDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	for i := range findings {
		findings[i].DetectorID = ""
	}
	return findings, err
}

func TestDeterministicResponseStageHandoffRetainsObservedDetectorIdentity(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	loop.Detectors = []sdk.Detector{pipelineIdentitylessDetector{}}
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	state.PriorStages = []*contracts.StageReport{report}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	if _, err := (AgentResponseStage{Dispatcher: dispatcher}).Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	request := <-dispatcher.requests
	if len(request.Findings) != 1 || request.Findings[0].DetectorID != loop.Detectors[0].Spec().ID {
		t.Fatalf("handoff lost observed detector identity: %+v", request.Findings)
	}
	if _, err := incidentRequestContract(request); err != nil {
		t.Fatalf("handoff rejects its own observation: %v", err)
	}
}

func TestDeterministicResponseStageReflectionReceivesVerifiedRepairCauses(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	started := time.Now().UTC()
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	result, err := responsePipelineResult(state, started, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	closed := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now().UTC(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now().UTC()}}}
	closed.Request.PriorStages = cloneStageReports(state.PriorStages)
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closed, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(derived.Record.ConfirmedRootCauses) != 2 || len(validate.VerifiedReflectionCauses(derived.Record)) != 2 {
		t.Fatalf("verified deterministic repairs lost before reflection: %v", derived.Record)
	}
	// A failed peer must not hide a repaired cause, and the controller's stage
	// evidence must retain real plan/apply calls without a responder session.
	failed := proto.Clone(derived.Record).(*contracts.OutcomeRecord)
	failed.Classification = "failed"
	if got := partialRepairCauses(failed); len(got) != 2 {
		t.Fatalf("stage repairs missing partial reflection proof: %v", got)
	}
}

func TestResponseStageRejoinRetainsDeterministicApplicationEvidence(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	request := state.Request
	request.PreparedAgentTask = "durable-agent-handoff"
	request.PriorStages = cloneStageReports(state.PriorStages)
	dispatcher := &rejoiningStageDispatcher{}
	controller := &Controller{config: ControllerConfig{ResponseStages: []string{"deterministic", "agent"}}, dispatcher: dispatcher}
	result, _, err := controller.runResponsePipeline(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PlanApplications) != 2 || len(result.ConfirmedRootCauses) != 2 || len(result.RepairActions) != 2 || dispatcher.automatic != 0 || result.ResponderSessionID != "resumed-session" {
		t.Fatalf("stage evidence lost on durable rejoin: %+v", result)
	}
}

type pipelineLaggingHealthDetector struct{ remaining int }

func (d *pipelineLaggingHealthDetector) Spec() sdk.DetectorSpec {
	spec := (pipelineDetector{}).Spec()
	spec.ID = "traffic-lag"
	spec.Playbooks = nil
	return spec
}
func (d *pipelineLaggingHealthDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (pipelineDetector{}).Detect(ctx, view)
	if err != nil || len(findings) != 0 {
		return nil, err
	}
	if d.remaining == 0 {
		return nil, nil
	}
	if d.remaining > 0 {
		d.remaining--
	}
	return []sdk.Finding{{RuleID: "lagging-sample", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Summary: "traffic sample is still clearing", Evidence: "previous traffic window still contains errors"}}, nil
}

func TestDeterministicResponseStageWaitsForPostRolloutHealthObservation(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Detectors = append(loop.Detectors, &pipelineLaggingHealthDetector{remaining: 2})
	calls := 0
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}, clearingResponseStage{"agent", &calls}}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || len(state.Findings) != 0 {
		t.Fatalf("handed off while repaired workloads' health sample was clearing: calls=%d stages=%v", calls, state.PriorStages)
	}
}

// pipelineWindowedHealthDetector is a traffic-like health signal judged over a
// declared rolling window: after the repairs it keeps reporting for lag, then
// clears (a negative lag never clears).
type pipelineWindowedHealthDetector struct {
	window, lag time.Duration
	repairedAt  time.Time
}

func (d *pipelineWindowedHealthDetector) Spec() sdk.DetectorSpec {
	spec := (pipelineDetector{}).Spec()
	spec.ID = "traffic-window"
	spec.Playbooks = nil
	return spec
}
func (d *pipelineWindowedHealthDetector) EvaluationWindow(sdk.DetectionContext) time.Duration {
	return d.window
}
func (d *pipelineWindowedHealthDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (pipelineDetector{}).Detect(ctx, view)
	if err != nil || len(findings) != 0 {
		return nil, err
	}
	if d.repairedAt.IsZero() {
		d.repairedAt = time.Now()
	}
	if d.lag >= 0 && time.Since(d.repairedAt) >= d.lag {
		return nil, nil
	}
	return []sdk.Finding{{RuleID: "window-errors", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Summary: "traffic window still holds pre-repair errors", Evidence: "rolling window contains pre-repair samples"}}, nil
}

// Live slot 17: both plans cleared, but the traffic window cleared about one
// second after a fixed post-apply wait, so a responder started for nothing.
// The wait follows the detector's declared window, not the rollout budget.
func TestDeterministicResponseStageWaitFollowsDetectorWindow(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	const fixedWait = 100 * time.Millisecond
	loop.Detectors = append(loop.Detectors, &pipelineWindowedHealthDetector{window: 2 * time.Second, lag: fixedWait + 150*time.Millisecond})
	calls := 0
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, RolloutTimeout: fixedWait, ObservationInterval: 10 * time.Millisecond}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, clearingResponseStage{"agent", &calls}}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || len(state.Findings) != 0 {
		t.Fatalf("handed off before the remaining detector's window rolled over: calls=%d stages=%v", calls, state.PriorStages)
	}
}

func TestDeterministicResponseStageWaitStopsAtCeiling(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Detectors = append(loop.Detectors, &pipelineWindowedHealthDetector{window: time.Hour, lag: -1})
	calls := 0
	const ceiling = 200 * time.Millisecond
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationInterval: 10 * time.Millisecond, ObservationCeiling: ceiling}
	started := time.Now()
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, clearingResponseStage{"agent", &calls}}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > ceiling+2*time.Second {
		t.Fatalf("waited %s, beyond the %s ceiling", elapsed, ceiling)
	}
	if calls != 1 || len(state.PriorStages[0].RemainingFindingFingerprints) != 1 {
		t.Fatalf("a never-clearing signal must reach the responder with its finding: calls=%d stages=%v", calls, state.PriorStages)
	}
}

func TestDeterministicResponseStageCannotClearWithoutIndependentProof(t *testing.T) {
	finding := sdk.Finding{DetectorID: "incident", Fingerprint: "covered", Status: sdk.FindingActive}
	makeAttempt := func() *contracts.StageMitigation {
		return &contracts.StageMitigation{PlaybookId: "repair", ProgramRevision: "revision", ApplyReceipt: &contracts.PlanApplication{Applied: true, VerificationCompleted: true, VerificationPassed: true}, PostApplyDiagnose: &contracts.ProgramRun{ProgramId: "repair", ProgramRevision: "revision", Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}}
	}
	cases := map[string]func(*contracts.StageMitigation, *ToolResponse){
		"not-applied":     func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.Applied = false },
		"dry-run":         func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.DryRun = true },
		"rolled-back":     func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.RolledBack = true },
		"unverified":      func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.VerificationPassed = false },
		"not-observed":    func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.VerificationCompleted = false },
		"rollback-error":  func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.RollbackError = "unconfirmed" },
		"wrong-program":   func(a *contracts.StageMitigation, _ *ToolResponse) { a.PostApplyDiagnose.ProgramId = "peer" },
		"wrong-revision":  func(a *contracts.StageMitigation, _ *ToolResponse) { a.PostApplyDiagnose.ProgramRevision = "peer" },
		"diagnosis-error": func(a *contracts.StageMitigation, _ *ToolResponse) { a.PostApplyDiagnose.Error = "unavailable" },
		"pending-rollout": func(a *contracts.StageMitigation, _ *ToolResponse) {
			a.ApplyReceipt.RolloutPending = &contracts.RolloutPending{}
		},
		"apply-error":       func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.Error = "unconfirmed" },
		"observation-error": func(a *contracts.StageMitigation, _ *ToolResponse) { a.Error = "unavailable" },
		"still-match": func(a *contracts.StageMitigation, _ *ToolResponse) {
			a.PostApplyDiagnose.Verdict.Result = contracts.Verdict_RESULT_MATCH
		},
		"unknown": func(a *contracts.StageMitigation, _ *ToolResponse) {
			a.PostApplyDiagnose.Verdict.Result = contracts.Verdict_RESULT_UNKNOWN
		},
		"fresh-firing": func(_ *contracts.StageMitigation, r *ToolResponse) { r.Findings = []sdk.Finding{finding} },
	}
	state := &IncidentState{Findings: []sdk.Finding{finding}}
	clearVerifiedStageTargets(state, makeAttempt(), []string{"covered"}, ToolResponse{})
	if len(state.Findings) != 0 {
		t.Fatal("positive control did not clear a verified finding")
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			attempt, response := makeAttempt(), ToolResponse{}
			change(attempt, &response)
			state := &IncidentState{Findings: []sdk.Finding{finding}}
			clearVerifiedStageTargets(state, attempt, []string{"covered"}, response)
			if len(state.Findings) != 1 {
				t.Fatalf("%s erased an unverified finding", name)
			}
		})
	}
}

func TestDeterministicResponseStageObservationBudgetPreservesUnresolvedHealth(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Detectors = append(loop.Detectors, &pipelineLaggingHealthDetector{remaining: -1})
	calls := 0
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, RolloutTimeout: time.Millisecond, ObservationInterval: time.Millisecond}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, clearingResponseStage{"agent", &calls}}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(state.PriorStages[0].RemainingFindingFingerprints) != 1 {
		t.Fatalf("observation timeout lost unresolved health: calls=%d stages=%v", calls, state.PriorStages)
	}
}

func TestDeterministicResponseStageRejoinReobservesRetainedIncidentFinding(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	for i := range state.Request.Findings {
		if state.Request.Findings[i].PrimaryResource.Kind == "Service" {
			state.Request.Findings[i].Fingerprint = FindingFingerprint(state.Request.Findings[i])
			state.Request.Findings[i].DetectorID = "incident.port"
		}
	}
	state.Findings = append([]sdk.Finding(nil), state.Request.Findings...)
	loop.Request = state.Request
	var durable []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { durable = append([]byte(nil), payload...); return nil }
	if _, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	fresh := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	fresh.ReadView = loop.ReadView
	if err := fresh.restoreToolSnapshot(durable); err != nil {
		t.Fatal(err)
	}
	replay := &IncidentState{Request: state.Request, Findings: append([]sdk.Finding(nil), state.Request.Findings...), Coverage: state.Coverage}
	if _, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return fresh }}).Run(context.Background(), replay); err != nil {
		t.Fatal(err)
	}
	if len(replay.Findings) != 1 || len(fresh.PlanApplications()) != 0 {
		t.Fatalf("rejoin resurrected stale repaired finding or repeated a write: findings=%v applications=%v", replay.Findings, fresh.PlanApplications())
	}
}

// Replay the escaped SDO evidence failures across complete-checkpoint restarts
// and delayed health observations. The real executor repairs both client-go
// objects; recovery has immutable receipts but no restored write authority.
func TestSimScheduleDeterministicPipelineEvidence(t *testing.T) {
	for _, schedule := range []struct {
		name                string
		restarts, healthLag int
	}{
		{"direct", 0, 0}, {"late-health", 0, 2}, {"restart", 1, 0}, {"late-health-two-restarts", 2, 2},
	} {
		t.Run(schedule.name, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			for i := range state.Request.Findings {
				state.Request.Findings[i].Fingerprint = FindingFingerprint(state.Request.Findings[i])
				state.Request.Findings[i].DetectorID = "incident.mechanism"
			}
			state.Findings = append([]sdk.Finding(nil), state.Request.Findings...)
			loop.Request = state.Request
			loop.Detectors = []sdk.Detector{pipelineIdentitylessDetector{}, &pipelineLaggingHealthDetector{remaining: schedule.healthLag}}
			var durable []byte
			save := func(_ context.Context, payload []byte) error { durable = append([]byte(nil), payload...); return nil }
			loop.saveToolState = save
			for tick := 0; tick <= schedule.restarts; tick++ {
				if tick > 0 {
					fresh := NewResponderLoop(state.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
					fresh.ReadView, fresh.saveToolState = loop.ReadView, save
					if err := fresh.restoreToolSnapshot(durable); err != nil {
						t.Fatal(err)
					}
					loop = fresh
					state = &IncidentState{Request: state.Request, Findings: append([]sdk.Finding(nil), state.Request.Findings...)}
				}
				calls := 0
				if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationInterval: time.Nanosecond}, clearingResponseStage{"agent", &calls}}); err != nil {
					t.Fatal(err)
				}
				if calls != 0 || len(state.Findings) != 0 || len(state.PriorStages) != 1 {
					t.Fatalf("tick=%d lost automatic repair progress: calls=%d stages=%v findings=%v", tick, calls, state.PriorStages, state.Findings)
				}
				if tick > 0 && len(loop.PlanApplications()) != 0 {
					t.Fatalf("tick=%d replayed a completed mutation", tick)
				}
				result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if len(result.ConfirmedRootCauses) != 2 || len(result.PlanApplications) != 2 || len(result.RepairActions) != 2 {
					t.Fatalf("tick=%d lost immutable repair evidence: %+v", tick, result)
				}
			}
		})
	}
}
