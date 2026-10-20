package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

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
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: ref}}}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: false}}, 1)
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
	if signals := loop.FailureSignals(); len(signals) != 1 || signals[0] != "adhoc_coverage_gap" {
		t.Fatalf("coverage signal: %v", signals)
	}
}
