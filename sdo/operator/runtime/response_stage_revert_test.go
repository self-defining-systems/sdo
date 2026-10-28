package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// memoryLimitProgram diagnoses the image-pull fault correctly but "repairs"
// it by raising the memory limit, a reversible field write that cannot help
// (as an OOM program applied to the wrong cause does).
type memoryLimitProgram struct{ fastloopImageProgram }

func (memoryLimitProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "image-pull-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}, {Name: "memory", ValueKind: program.StringValue, Default: "512Mi"}}}
}
func (memoryLimitProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["deployment"], Path: "spec.template.spec.containers[0].resources.limits.memory", Value: program.FromParam("memory"), Reason: "raise the limit"}}}, nil
}

func stageLoopWithPrograms(t *testing.T, detector sdk.Detector, programs ...interface {
	program.Diagnoser
	program.Mitigator
}) (*ResponderLoop, *IncidentState) {
	t.Helper()
	loop, state := pipelineTestLoop(t, false)
	client := loop.Executor.Client.(TypedPlanClient).Client
	deployment, err := client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deployment.Spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}
	if _, err := client.AppsV1().Deployments("app").Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var diagnosers []program.Diagnoser
	var mitigators []program.Mitigator
	for _, p := range programs {
		diagnosers, mitigators = append(diagnosers, p), append(mitigators, p)
	}
	registry, err := program.NewRegistry(diagnosers, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	loop.Detectors = []sdk.Detector{detector}
	loop.Executor.Verifier = PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: loop.Provider}
	snapshot, err := loop.Provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := state.Request
	request.Findings = findings
	loop.Request, loop.findings = request, findings
	request.InitialProgramRuns = loop.InitialDiagnose(context.Background())
	request.PlaybookCoverage = buildPlaybookCoverage(findings, request.InitialProgramRuns, loop.lastDiagnosisView, nil)
	loop.Request = request
	return loop, &IncidentState{Request: request, Findings: findings, Coverage: request.PlaybookCoverage}
}

func stageAttempt(t *testing.T, report *contracts.StageReport, id string) *contracts.StageMitigation {
	t.Helper()
	for _, attempt := range report.Mitigations {
		if attempt.PlaybookId == id {
			return attempt
		}
	}
	t.Fatalf("no %s attempt: %v", id, report)
	return nil
}

func touched(attempt *contracts.StageMitigation) map[string]bool {
	refs := map[string]bool{}
	for _, ref := range attempt.TouchedObjects {
		refs[ref.Kind+"/"+ref.Name] = true
	}
	return refs
}

// The responder must start from the original fault, not from a hybrid: an
// applied program that cleared none of its findings and still diagnoses the
// same fault has its reversible writes restored before handoff. The program
// that cleared its own fault is kept.
func TestIneffectiveReversibleProgramIsRevertedBeforeHandoff(t *testing.T) {
	ctx := context.Background()
	loop, state := stageLoopWithPrograms(t, pipelineDetector{}, pipelinePortProgram{}, memoryLimitProgram{})
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	client := loop.Executor.Client.(TypedPlanClient).Client
	deployment, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if memory := deployment.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]; memory.String() != "256Mi" {
		t.Fatalf("ineffective write left in place for the responder: memory limit %s", memory.String())
	}
	reverted := stageAttempt(t, report, "image-pull-failure")
	if reverted.Result != "reverted_ineffective" || !reverted.ApplyReceipt.GetRolledBack() || !touched(reverted)["Deployment/api"] {
		t.Fatalf("reverted attempt: %v", reverted)
	}
	cleared := stageAttempt(t, report, "service-targetport-mismatch")
	service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Result != "applied_cleared" || cleared.ApplyReceipt.GetRolledBack() || service.Spec.Ports[0].TargetPort.IntVal != 8080 || !touched(cleared)["Service/api"] {
		t.Fatalf("a program that cleared its finding was reverted: %v port=%v", cleared, service.Spec.Ports[0].TargetPort)
	}
	if len(state.Findings) != 1 || state.Findings[0].RuleID != "image-pull" {
		t.Fatalf("the original image fault goes to the responder: %v", state.Findings)
	}
	// A later pass treats the reverted program like any ineffective one.
	state.PriorStages = []*contracts.StageReport{report}
	applications := len(loop.PlanApplications())
	again, err := RunStageAgain(ctx, state, &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(loop.PlanApplications()) != applications {
		t.Fatalf("a later pass reapplied the reverted program: %v", again)
	}
}

// halfFixDetector links both fixture faults to one program, as a learned
// program that covers a two-part fault does.
type halfFixDetector struct{ pipelineDetector }

func (halfFixDetector) Spec() sdk.DetectorSpec {
	spec := (pipelineDetector{}).Spec()
	spec.Playbooks = []string{"half-fix"}
	return spec
}

func (d halfFixDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	for i := range findings {
		findings[i].Playbooks = []string{"half-fix"}
		findings[i].ParameterBindings = map[string]sdk.ObjectRef{
			"service":    {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"},
			"deployment": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"},
		}
	}
	return findings, err
}

// halfFixProgram diagnoses either fault and repairs only the port.
type halfFixProgram struct{}

func (halfFixProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "half-fix", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "deployment", Kind: "Deployment", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: 8080}}}
}
func (halfFixProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	port, err := (fastloopPortProgram{}).Diagnose(ctx, view, b)
	if err != nil || port.Result == program.Match {
		return port, err
	}
	return (fastloopImageProgram{}).Diagnose(ctx, view, b)
}
func (halfFixProgram) Mitigate(ctx context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	return (fastloopPortProgram{}).Mitigate(ctx, view, b)
}

// A program that cleared one of its two findings made progress, so its write
// stays even though its diagnosis still matches the other fault.
func TestPartialProgramIsKept(t *testing.T) {
	ctx := context.Background()
	loop, state := stageLoopWithPrograms(t, halfFixDetector{}, halfFixProgram{})
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := stageAttempt(t, report, "half-fix")
	service, err := loop.Executor.Client.(TypedPlanClient).Client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Result != "applied_still_matching" || attempt.KeptReason != "partial" || attempt.ApplyReceipt.GetRolledBack() || len(attempt.ClearedFindingFingerprints) != 1 || service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("partial progress was not kept: %v port=%v", attempt, service.Spec.Ports[0].TargetPort)
	}
}

// Deletes, execs and restarts cannot be undone by restoring a preimage. An
// ineffective program with such an effect is kept and says why.
func TestIneffectiveIrreversibleProgramIsKeptAndRecorded(t *testing.T) {
	loop, state, deletes := podRestartLoop(t)
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := stageAttempt(t, report, "pod-restart")
	if deletes.Load() != 1 || attempt.Result != "applied_still_matching" || attempt.KeptReason != "irreversible" || attempt.ApplyReceipt.GetRolledBack() || !touched(attempt)["Pod/api-pod"] {
		t.Fatalf("irreversible attempt: deletes=%d %v", deletes.Load(), attempt)
	}
}
