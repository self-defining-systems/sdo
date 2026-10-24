package runtime

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A call here would spend the production incident verification budget before
// handing off the still-active second cause.
type slowIncidentVerifier struct{}

func (slowIncidentVerifier) Verify(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(5 * time.Minute):
		return false, nil
	}
}

type ineffectivePortProgram struct{ fastloopPortProgram }

func (ineffectivePortProgram) Spec() program.ProgramSpec {
	spec := (fastloopPortProgram{}).Spec()
	spec.Params[1].Default = 9999
	return spec
}

func installIneffectivePortProgram(t *testing.T, loop *ResponderLoop) {
	t.Helper()
	p := ineffectivePortProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
}

// Both faults are emitted by the same detector. The port program covers only
// the Service finding; the Deployment image fault requires the responder.
func TestPartialFastPathUnrelatedFaultNeverEmitsTrustFailure(t *testing.T) {
	loop, _ := trustedPortLoop(t, true, true, false)
	loop.TryTrustedFastPath(context.Background(), 3)
	if signals := loop.FailureSignals(); len(signals) != 0 {
		t.Fatalf("unrelated active finding penalized service-targetport-mismatch: %v", signals)
	}
}

func TestFastPathCauseVerificationSeparatesUnrelatedFindingsAndUncertainty(t *testing.T) {
	loop, _ := trustedPortLoop(t, true, true, false)
	before := loop.Request.Findings
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": before[0].PrimaryResource}}
	selected := []selectedPlan{{programID: "service-targetport-mismatch", bindings: bindings}}
	scope := fastPathVerificationScope{Baseline: before, Covered: before[:1]}
	for _, tc := range []struct {
		name             string
		change           func(*ToolResponse)
		verified, failed bool
	}{
		{name: "unrelated active finding", verified: true},
		{name: "unrelated program match", verified: true, change: func(r *ToolResponse) {
			r.ProgramRuns = append(r.ProgramRuns, ProgramRun{ProgramID: "other", Verdict: program.Verdict{Result: program.Match}})
		}},
		{name: "own diagnosis still matches", failed: true, change: func(r *ToolResponse) { r.ProgramRuns[0].Verdict.Result = program.Match; r.Findings = before }},
		{name: "new finding on an unrelated resource", verified: true, change: func(r *ToolResponse) {
			fresh := before[1]
			fresh.RuleID = "new-failure"
			r.Findings = append(r.Findings, fresh)
		}},
		{name: "new finding on an applied resource", failed: true, change: func(r *ToolResponse) {
			fresh := before[0]
			fresh.RuleID = "new-failure"
			r.Findings = append(r.Findings, fresh)
		}},
		{name: "new peer symptom names the applied resource", failed: true, change: func(r *ToolResponse) {
			fresh := before[1]
			fresh.RuleID = "new-failure"
			fresh.RelatedResources = []sdk.ObjectRef{before[0].PrimaryResource}
			r.Findings = append(r.Findings, fresh)
		}},
		{name: "covered finding still active", change: func(r *ToolResponse) { r.Findings = before }},
		{name: "unknown bound diagnosis", change: func(r *ToolResponse) { r.ProgramRuns[0].Verdict.Result = program.Unknown }},
		{name: "missing bound diagnosis", change: func(r *ToolResponse) { r.ProgramRuns = nil }},
		{name: "missing detector observation", change: func(r *ToolResponse) { r.DetectorStates = nil }},
		{name: "detector read error", change: func(r *ToolResponse) { r.Error = "synthetic read failure" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := ToolResponse{Findings: append([]sdk.Finding(nil), before[1:]...), DetectorStates: []DetectorEvaluation{{DetectorID: before[0].DetectorID, Status: DetectorEvaluationFiring}}, ProgramRuns: []ProgramRun{{ProgramID: selected[0].programID, Bindings: bindings, Verdict: program.Verdict{Result: program.NoMatch}}}}
			if tc.change != nil {
				tc.change(&response)
			}
			if got := fastPathCauseVerified(selected, scope, response); got != tc.verified {
				t.Errorf("verified = %v, want %v", got, tc.verified)
			}
			if got := fastPathCauseFailed(selected, scope, response); got != tc.failed {
				t.Errorf("failed = %v, want %v", got, tc.failed)
			}
		})
	}
}

type newFindingAfterPortRepair struct{}

func (newFindingAfterPortRepair) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "synthetic-side-effect"}
}
func (newFindingAfterPortRepair) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	for _, service := range view.Services() {
		if service.Spec.Ports[0].TargetPort.IntVal == 8080 {
			return []sdk.Finding{{DetectorID: "synthetic-side-effect", RuleID: "new-finding", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}, nil
		}
	}
	return nil, nil
}

type newFindingAfterImageRepair struct{}

func (newFindingAfterImageRepair) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "synthetic-side-effect"}
}
func (newFindingAfterImageRepair) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Spec.Template.Spec.Containers[0].Image == "good:v1" {
			return []sdk.Finding{{DetectorID: "synthetic-side-effect", RuleID: "new-finding", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "synthetic apply side effect", Evidence: "finding appeared only after image repair", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}}}, nil
		}
	}
	return nil, nil
}

func TestFastPathNewFindingRollsBackAndPenalizesAppliedProgram(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, false)
	loop.Detectors = append(loop.Detectors, newFindingAfterPortRepair{})
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if attempt.Verified || !attempt.RolledBack || !contains(loop.FailureSignals(), "fast_path_failure:service-targetport-mismatch@tree-a") {
		t.Fatalf("new finding was not rejected: %+v signals=%v", attempt, loop.FailureSignals())
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("new finding did not restore preimage: %v %v", service, err)
	}
}

func TestRestartPartialFastPathRechecksWithoutMutationOrTrustPenalty(t *testing.T) {
	ctx := context.Background()
	loop, client := trustedPortLoop(t, true, true, false)
	d := KubernetesJobDispatcher{Client: client, Namespace: "app"}
	attachRecoveryStore(t, d, loop)
	if _, _, resolved, err := d.tryFastPath(ctx, loop.Request, loop); err != nil || resolved || len(loop.fastPathRepairs) != 1 {
		t.Fatalf("initial partial repair: %v %v repairs=%v", resolved, err, loop.fastPathRepairs)
	}
	marker, err := client.CoreV1().ConfigMaps("app").Get(ctx, IncidentJobName(loop.Request.IncidentID)+"-fast-path", metav1.GetOptions{})
	if err != nil || marker.Data["state"] != "partial" || marker.Data["trust_failure_signals"] != "" {
		t.Fatalf("partial repair stored a false failure signal: %v %v", marker, err)
	}
	for restart := 0; restart < 2; restart++ {
		restarted := restartPortLoop(loop)
		attachRecoveryStore(t, d, restarted)
		writes := 0
		restarted.Executor.Client = countingPlanClient{PlanObjectClient: restarted.Executor.Client, writes: &writes}
		if _, summary, resolved, err := d.tryFastPath(ctx, loop.Request, restarted); err != nil || resolved || summary == "" {
			t.Fatalf("restart partial: %v %q %v", resolved, summary, err)
		}
		if writes != 0 || len(restarted.FailureSignals()) != 0 || len(restarted.fastPathRepairs) != 1 || len(restarted.fastPathPrograms) != 1 || len(restarted.PlanApplications()) != 0 {
			t.Fatalf("restart replayed or penalized repair: writes=%d signals=%v repairs=%v apps=%v", writes, restarted.FailureSignals(), restarted.fastPathRepairs, restarted.PlanApplications())
		}
		if restarted.fastPathRepairs[0].VerifiedAt.Before(loop.fastPathRepairs[0].VerifiedAt) {
			t.Fatal("restart reused stale repair verification")
		}
	}
}

func TestHermeticPartialFastPathKeepsRepairAndHandsOffRemainingCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := fastloopClient("good:v1")
		provider := fastloopObjects{client: client}
		view, err := provider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		findings, err := (fastloopDetector{}).Detect(ctx, view)
		if err != nil || len(findings) != 2 {
			t.Fatalf("two-fault fixture: %v %v", findings, err)
		}
		registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
		if err != nil {
			t.Fatal(err)
		}
		request := goldenRequest(t)
		request.Namespace, request.SourceCommit, request.RepairPolicy = "app", "tree-a", "recorded-actions"
		request.ResponseDeadline = time.Now().Add(time.Minute)
		request.Findings = findings
		request.RepositoryWorktree = t.TempDir() + "/proposal"
		loop := NewResponderLoop(request, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{
			Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{},
			Verifier: slowIncidentVerifier{}, // whole-incident health is red
		}, 5)
		loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
			return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
		}
		loop.DiagnosisSettleDelay, loop.CorpusReplayPassed = time.Millisecond, true
		d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
			ServiceAccount: "responder", RepositoryPVC: "repo", RepositoryMountPath: strings.TrimSuffix(request.RepositoryWorktree, "/proposal"),
			ProgramFastPath: true, ProgramTrustThreshold: 1, PollInterval: time.Millisecond,
			Outcomes:    priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "prior", Record: verifiedTrustOutcome(t, "service-targetport-mismatch", "tree-a")}}},
			LoopFactory: func(r IncidentRequest) *ResponderLoop { loop.Request = r; return loop }}
		config := testControllerConfig()
		config.Application, config.Namespace = request.Application, request.Namespace
		config.SourceCommit, config.DeployedCommit = request.SourceCommit, request.DeployedCommit
		config.RepositoryWorktree, config.RepairPolicy = request.RepositoryWorktree, request.RepairPolicy
		config.FiringThreshold, config.ClearThreshold = 1, 1
		start := time.Now()
		controller, err := NewController(config, []sdk.Detector{fastloopDetector{}}, provider, d, start)
		if err != nil {
			t.Fatal(err)
		}
		results := make(chan IncidentResult, 1)
		closures := make(chan IncidentClosure, 1)
		controller.OnResult = func(result IncidentResult) { results <- result }
		controller.OnIncidentClosed = func(closure IncidentClosure) { closures <- closure }
		if err := controller.Step(ctx, start, nil); err != nil {
			t.Fatal(err)
		}
		effect, ok := controller.PendingDispatchEffect()
		if !ok {
			t.Fatal("two-fault incident did not dispatch")
		}
		request = effect.Request
		if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
			t.Fatal(err)
		}
		job := awaitJob(t, client, IncidentJobName(request.IncidentID))
		service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
		if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 8080 {
			t.Fatalf("verified partial repair was rolled back: %v %v", service, err)
		}
		if signals := loop.FailureSignals(); len(signals) != 0 {
			t.Fatalf("unrelated fault penalized the correct program: %v", signals)
		}
		applications := loop.PlanApplications()
		if len(applications) != 1 || applications[0].RolledBack || !applications[0].Result.VerificationPassed {
			t.Fatalf("partial repair evidence: %+v", applications)
		}
		cm, err := client.CoreV1().ConfigMaps("demo").Get(ctx, job.Name+"-request", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		task := new(contracts.AgentTask)
		if err := protojson.Unmarshal([]byte(cm.Data[jobRequestDataKey]), task); err != nil {
			t.Fatal(err)
		}
		incident := new(contracts.IncidentRequest)
		if err := task.Request.UnmarshalTo(incident); err != nil {
			t.Fatal(err)
		}
		if len(incident.Findings) != 1 || incident.Findings[0].PrimaryResource.Kind != "Deployment" {
			t.Fatalf("responder did not get only the remaining cause: %v", incident.Findings)
		}
		if len(incident.FastPathRepairs) != 1 || incident.FastPathRepairs[0].ProgramId != "service-targetport-mismatch" || len(incident.FastPathRepairs[0].CoveredFindings) != 1 || incident.FastPathRepairs[0].CoveredFindings[0].PrimaryResource.Kind != "Service" {
			t.Fatalf("responder missed typed repaired-cause context: %v", incident)
		}
		for _, run := range incident.InitialProgramRuns {
			if run.GetProgramId() == "service-targetport-mismatch" && run.GetVerdict().GetResult() == contracts.Verdict_RESULT_MATCH {
				t.Fatal("handoff invited responder to replay a stale repaired-program Match")
			}
		}
		if err := controller.Step(ctx, start.Add(time.Second), nil); err != nil {
			t.Fatal(err)
		}
		if !controller.incidentOpen || len(closures) != 0 {
			t.Fatal("partial fast-path repair prematurely closed the incident")
		}
		// The responder repairs only the second fault; controller verification
		// must still see the independently healthy incident before completion.
		deployment, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
		if _, err = client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		publishContinuationResult(t, client, job, completedResult(request.IncidentID))
		awaitDispatchCompletionQueued(t, controller)
		for sample := 2; sample < 5; sample++ {
			if err := controller.Step(ctx, start.Add(time.Duration(sample)*time.Second), nil); err != nil {
				t.Fatal(err)
			}
		}
		if len(results) != 1 || len(closures) != 1 {
			t.Fatalf("remaining cause did not close: result=%d closure=%d error=%s", len(results), len(closures), controller.dispatchError)
		}
		finished := <-results
		closure := <-closures
		if controller.incidentOpen || closure.Result == nil || closure.Result.Status != IncidentCompleted || len(closure.FinalDetectorStates) != 1 || closure.FinalDetectorStates[0].Status != DetectorEvaluationClear {
			t.Fatalf("incident did not close after independent verification: %+v", closure)
		}
		if finished.Status != IncidentCompleted || finished.ResolutionPath != "responder" {
			t.Fatalf("remaining cause did not complete: %+v", finished)
		}
		if len(finished.ProgramFailureSignals) != 0 {
			t.Fatalf("completion penalized correct partial repair: %v", finished.ProgramFailureSignals)
		}
		message, err := incidentResultContract(finished)
		if err != nil {
			t.Fatal(err)
		}
		for _, classification := range []string{"success", "failed"} {
			outcome := &contracts.OutcomeRecord{Classification: classification, SourceCommit: "tree-a", ProgramRuns: message.ProgramRuns, PlanApplications: message.PlanApplications, ProgramToolCalls: message.ProgramToolCalls, RepairActions: message.RepairActions, ConfirmedRootCauses: message.ConfirmedRootCauses, ProgramFailureSignals: message.ProgramFailureSignals}
			settleTestOutcome(outcome)
			state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["service-targetport-mismatch"]
			if state.VerifiedSuccesses != 1 || state.Failures != 0 || !state.Trusted {
				t.Fatalf("%s incident penalized verified partial repair: %+v", classification, state)
			}
		}
	})
}
