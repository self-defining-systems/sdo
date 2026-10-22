package runtime

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type sourceRepairProgram struct{}

func (sourceRepairProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "source-repair", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (sourceRepairProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, s := range v.Services() {
		if s.Name == b["service"].Name {
			if s.Spec.Ports[0].TargetPort.IntVal != 8080 {
				return program.Verdict{Result: program.Match}, nil
			}
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}
func (p sourceRepairProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b["service"]}}}, nil
}

func TestCreditEquivalentSourceRepairIgnoresServerDefaultsAndUnownedFields(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	source := planObject(ref, map[string]any{"selector": map[string]any{"app": "api"}, "ports": []any{map[string]any{"port": float64(80), "targetPort": float64(8080)}}})
	before := source.DeepCopy()
	_ = unstructured.SetNestedField(before.Object, "10.0.0.1", "spec", "clusterIP")
	_ = unstructured.SetNestedField(before.Object, "ClusterIP", "spec", "type")
	before.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["targetPort"] = float64(9999)
	before.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["protocol"] = "TCP"
	before.SetLabels(map[string]string{"other-controller": "old"})
	before.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:selector":{"f:app":{}},"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:port":{},"f:targetPort":{}}}}}`)}}})
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}}}
	provider := &changingProvider{snapshot: snapshot}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	registry, err := program.NewRegistry([]program.Diagnoser{sourceRepairProgram{}}, []program.Mitigator{sourceRepairProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "source-credit", Namespace: "app", SourceCommit: "accepted", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: ref}}}, registry, provider, nil, PlanExecutor{Client: client, Source: planFakeSource{source: source}}, 1)
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}
	}
	loop.InitialDiagnose(context.Background())
	snapshot.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	provider.snapshot = snapshot
	repaired := before.DeepCopy()
	repaired.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["targetPort"] = float64(8080)
	repaired.SetLabels(map[string]string{"other-controller": "new"})
	client.objects[ref] = repaired
	loop.CreditEquivalentRepairs(context.Background(), []RepairActionReceipt{{ActionID: "source", Success: true, Resources: []sdk.ObjectRef{ref}}})
	if len(loop.PlanApplications()) != 1 {
		t.Fatalf("equivalent source repair with defaults earned no credit: %+v", loop.PlanApplications())
	}
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{equivalentRepairOutcome(t, loop)}, 1)["source-repair"]; state.VerifiedSuccesses != 1 || !state.Trusted {
		t.Fatalf("equivalent source repair missing bound trust receipt: %+v", state)
	}
}

func equivalentRepairOutcome(t *testing.T, loop *ResponderLoop) *contracts.OutcomeRecord {
	t.Helper()
	runs := loop.ProgramRuns()
	fillProgramRevisions(context.Background(), runs, loop.Request.RepositoryWorktree, loop.Request.SourceCommit)
	result, err := incidentResultContract(IncidentResult{ProgramRuns: runs, ProgramToolCalls: loop.ToolCalls(), PlanApplications: loop.PlanApplications()})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.OutcomeRecord{IncidentId: loop.Request.IncidentID, SourceCommit: loop.Request.SourceCommit, Classification: "success", ProgramRuns: result.ProgramRuns, ProgramToolCalls: result.ProgramToolCalls, PlanApplications: result.PlanApplications}
}
