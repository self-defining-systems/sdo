package runtime

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResponderLoopApplyReturnsLogicalPlaybooksAndBoundedEvidence(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref, Evidence: strings.Repeat("observation", 3000), Playbooks: []string{".sdo/playbooks/health-objective/README.md"}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app", Findings: []sdk.Finding{finding}}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{finding: finding}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if len(response.Findings) != 1 || response.Findings[0].Playbooks[0] != "health-objective" {
		t.Fatalf("physical playbook in tool response: %+v", response.Findings)
	}
	call := loop.ToolCalls()[0]
	if strings.Contains(call.ResponseJSON, ".sdo/") || len(call.ResponseJSON) > 2048 {
		t.Fatalf("unbounded or physical tool evidence: %d bytes", len(call.ResponseJSON))
	}
	var recorded map[string]any
	if err := json.Unmarshal([]byte(call.ResponseJSON), &recorded); err != nil || recorded["truncated"] != true {
		t.Fatalf("missing structured truncation marker: %s, %v", call.ResponseJSON, err)
	}
}

func TestResponderLoopStatusAndDiagnoseUseFreshFindings(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref}
	active := false
	detector := changingLoopDetector{find: func() []sdk.Finding {
		if active {
			return []sdk.Finding{finding}
		}
		return nil
	}}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{detector}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	active = true
	status := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(status.Findings) != 1 || len(status.ProgramRuns) != 1 || status.ProgramRuns[0].Verdict.Result != program.Match {
		t.Fatalf("stale status: %+v", status)
	}
	active = false
	diagnosis := loop.Handle(context.Background(), ToolRequest{Command: "diagnose"})
	if len(diagnosis.Findings) != 0 || len(diagnosis.ProgramRuns) != 0 {
		t.Fatalf("stale diagnosis: %+v", diagnosis)
	}
}

type changingLoopDetector struct{ find func() []sdk.Finding }

func (changingLoopDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "health"} }
func (d changingLoopDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.find(), nil
}

func TestInitialNoMatchIsRediagnosedAfterSettle(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	active := false
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		if active {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app", Findings: []sdk.Finding{finding}}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{finding: finding}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	loop.DiagnosisSettleDelay = time.Millisecond
	if initial := loop.InitialDiagnose(context.Background()); len(initial) != 1 || initial[0].Verdict.Result != program.NoMatch {
		t.Fatalf("initial: %+v", initial)
	}
	active = true
	loop.RetryInitialDiagnose(context.Background())
	if runs := loop.ProgramRuns(); len(runs) != 2 || runs[1].Verdict.Result != program.Match {
		t.Fatalf("retry: %+v", runs)
	}
	status := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(status.ProgramRuns) != 1 || status.ProgramRuns[0].Verdict.Result != program.Match {
		t.Fatalf("settled status: %+v", status)
	}
}

type loopProgram struct {
	candidateProgram
	ref sdk.ObjectRef
}

type loopDetector struct{ finding sdk.Finding }

func (d loopDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "health"} }
func (d loopDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.finding.Status == sdk.FindingActive {
		return []sdk.Finding{d.finding}, nil
	}
	return nil, nil
}

func (p loopProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: p.ref, Path: "spec.ports[0].targetPort", Value: 8080, Reason: "repair target port"}}}, nil
}

func TestResponderLoopAppliesOneBatchThenRefusesExhaustedBudgetAndLogsCalls(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	p := loopProgram{candidateProgram: candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}, ref: ref}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: ref}}}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{finding: sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref, Playbooks: []string{".sdo/playbooks/health-objective/README.md"}}}}, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: false}}, 1)
	loop.Trust = map[string]ProgramTrust{"targetport": {VerifiedSuccesses: 2}}
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
	if status := loop.Handle(context.Background(), ToolRequest{Command: "status"}); status.Trust["targetport"].VerifiedSuccesses != 2 {
		t.Fatalf("status trust: %+v", status)
	}
	plan := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "targetport", Bindings: program.Bindings{"service": ref}})
	if plan.Error != "" || plan.PlanID == "" || len(plan.Patches) != 1 {
		t.Fatalf("plan: %+v", plan)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}})
	if applied.Error != "" || applied.ApplicationID == "" || len(applied.ProgramRuns) == 0 {
		t.Fatalf("apply: %+v", applied)
	}
	if len(applied.Findings) != 1 || applied.Findings[0].Playbooks[0] != "health-objective" || strings.Contains(loop.ToolCalls()[2].ResponseJSON, ".sdo/") {
		t.Fatalf("apply leaked physical playbook path: %+v", applied.Findings)
	}
	denied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}})
	if denied.Error == "" || !denied.Unresolved {
		t.Fatalf("budget: %+v", denied)
	}
	if len(loop.ToolCalls()) != 4 || len(loop.PlanApplications()) != 1 || len(loop.ProgramRuns()) == 0 {
		t.Fatalf("outcome evidence missing: calls=%d apps=%d runs=%d", len(loop.ToolCalls()), len(loop.PlanApplications()), len(loop.ProgramRuns()))
	}
	if signals := loop.FailureSignals(); len(signals) != 1 || signals[0] != "applied_diagnosis_still_match:targetport" {
		t.Fatalf("failure signal: %v", signals)
	}
}

func TestResponderLoopRecordsAdhocCoverageGapAfterVerification(t *testing.T) {
	request := IncidentRequest{IncidentID: "i", Namespace: "app", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}}
	loop := NewResponderLoop(request, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	verified := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if verified.Error != "" || !verified.Healthy {
		t.Fatalf("verify: %+v", verified)
	}
	if len(verified.ProgramRuns) != 0 {
		t.Fatalf("clean verify returned unrelated candidate sweep: %+v", verified.ProgramRuns)
	}
	if signals := loop.FailureSignals(); len(signals) != 1 || signals[0] != "adhoc_coverage_gap" {
		t.Fatalf("coverage signal: %v", signals)
	}
}

func TestCleanVerifyDoesNotSweepUnrelatedPrograms(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, registry, staticProvider{snapshot: snapshot}, []sdk.Detector{loopDetector{}}, PlanExecutor{}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	verified := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if !verified.Healthy || len(verified.ProgramRuns) != 0 || !strings.Contains(verified.Summary, "return the structured incident result immediately") {
		t.Fatalf("clean verify: %+v", verified)
	}
}
