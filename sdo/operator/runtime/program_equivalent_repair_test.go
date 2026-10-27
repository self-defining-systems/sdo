package runtime

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type sourceRepairProgram struct{}

func TestFinalPlanEquivalenceIncludesFieldsTakenByTheFaultInjector(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"selector": map[string]any{"app": "api"}, "ports": []any{map[string]any{"port": float64(80), "targetPort": float64(9999)}}})
	before.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:selector":{"f:app":{}}}}`)}}})
	preview := PlanApplicationResult{PreImages: []PlanPreImage{{Ref: ref, Object: before}}, Patches: []PlanPatch{{Ref: ref, Patch: []byte(`{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`)}}}
	for _, port := range []float64{8080, 9999, 8081} {
		current := before.DeepCopy()
		current.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["targetPort"] = port
		if got := finalPlanFieldsMatch(preview, func(sdk.ObjectRef) *unstructured.Unstructured { return current }); got != (port == 8080) {
			t.Fatalf("port %v: exact=%t", port, got)
		}
	}
}

func (sourceRepairProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "source-repair", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (sourceRepairProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, s := range v.Services() {
		if s.Name == b.Objects["service"].Name {
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
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["service"]}}}, nil
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
	if state := settledCredits([]*contracts.OutcomeRecord{equivalentRepairOutcome(t, loop)})["source-repair"]; state.VerifiedSuccesses != 1 || state.Failures != 0 {
		t.Fatalf("equivalent source repair missing bound trust receipt: %+v", state)
	}
	// A subsequent repair can clear diagnosis while differing from the plan.
	// Keep the historical receipt, but do not turn that evidence into trust.
	repaired.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["targetPort"] = float64(8081)
	loop.CreditEquivalentRepairs(context.Background(), []RepairActionReceipt{{ActionID: "different-fields", Success: true, Resources: []sdk.ObjectRef{ref}}})
	applications := loop.PlanApplications()
	if len(applications) != 1 || applications[0].Result.FinalEquivalenceVerified == nil || *applications[0].Result.FinalEquivalenceVerified {
		t.Fatalf("different fields retained exact credit: %+v", applications)
	}
	if state := settledCredits([]*contracts.OutcomeRecord{equivalentRepairOutcome(t, loop)})["source-repair"]; state.VerifiedSuccesses != 0 || state.Failures != 0 {
		t.Fatalf("question received a trust judgment: %+v", state)
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
	o := &contracts.OutcomeRecord{IncidentId: loop.Request.IncidentID, SourceCommit: loop.Request.SourceCommit, Classification: "success", ProgramRuns: result.ProgramRuns, ProgramToolCalls: result.ProgramToolCalls, PlanApplications: result.PlanApplications}
	for _, run := range o.GetProgramRuns() {
		if run.GetVerdict().GetResult() == contracts.Verdict_RESULT_MATCH {
			for _, binding := range run.GetBindings() {
				o.ConfirmedRootCauses = append(o.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{binding.GetObject()}})
				o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{Success: true, Resources: []*contracts.ObjectRef{binding.GetObject()}})
			}
		}
	}
	settleTestOutcome(o)
	return o
}

// A create has a typed absent pre-image; final equivalence compares the live
// object with the created object instead of dereferencing a missing one.
func TestFinalPlanEquivalenceComparesCreatedObjects(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "missing"}
	accepted := planObject(ref, map[string]any{})
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"key": "safe"}
	plans := map[string]program.Plan{
		"create-from-source": {Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "restore accepted missing object"}}},
		"general-create-then-patch": {Actions: []program.Action{
			program.ApplyObject{Ref: ref, Mode: "create", Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "missing", "namespace": "app"}, "data": map[string]any{"key": "safe"}}},
			program.PatchObject{Ref: ref, Type: "merge", Patch: []byte(`{"data":{"second":"yes"}}`)},
		}},
	}
	for name, plan := range plans {
		t.Run(name, func(t *testing.T) {
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, plan, false)
			if err != nil || !result.VerificationPassed || client.creates != 1 {
				t.Fatalf("create: %v", err)
			}
			if !result.PreImages[0].Absent || result.PreImages[0].Object != nil {
				t.Fatalf("create pre-image is not typed absent: %+v", result.PreImages[0])
			}
			get := func(ref sdk.ObjectRef) *unstructured.Unstructured { return client.objects[ref].DeepCopy() }
			if !finalPlanFieldsMatch(result, get) {
				t.Fatal("created object did not match its plan")
			}
			client.objects[ref].Object["data"] = map[string]any{"key": "changed"}
			if finalPlanFieldsMatch(result, get) {
				t.Fatal("a different final object earned equivalence")
			}
			delete(client.objects, ref)
			if finalPlanFieldsMatch(result, get) {
				t.Fatal("a missing final object earned equivalence")
			}
		})
	}
	unknown := PlanApplicationResult{PreImages: []PlanPreImage{{Ref: ref}}}
	if finalPlanFieldsMatch(unknown, func(sdk.ObjectRef) *unstructured.Unstructured { return accepted }) {
		t.Fatal("an unknown pre-image earned equivalence")
	}
}

// A panicking deterministic-stage step fails with a typed error and leaves the
// loop locks held for Run's deferred unlocks instead of killing the process.
func TestDeterministicStagePanicFailsOnlyThatStep(t *testing.T) {
	loop := &ResponderLoop{}
	loop.operationMu.Lock()
	loop.mu.Lock()
	err := withoutResponderLocks(loop, "credit-equivalent-repairs", true, func() {
		var missing *unstructured.Unstructured
		_ = missing.Object
	})
	var stepPanic *ResponderStepPanicError
	if !errors.As(err, &stepPanic) || stepPanic.Step != "credit-equivalent-repairs" {
		t.Fatalf("step panic was not typed: %v", err)
	}
	if loop.mu.TryLock() || loop.operationMu.TryLock() {
		t.Fatal("step did not reacquire the loop locks")
	}
	loop.mu.Unlock()
	loop.operationMu.Unlock()
}
