package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Sweep additions across lists and maps, including a list populated by defaults
// at acceptance: pruning only maps cannot restore that last case.
func TestSourceReconcileConvergesAddedConfiguration(t *testing.T) {
	for _, location := range []string{"containers", "env", "affinity", "volumes", "tolerations", "annotations", "accepted-list", "combined"} {
		t.Run(location, func(t *testing.T) {
			source := addedFieldsSource()
			live := addedFieldsLive()
			if location == "accepted-list" {
				_ = unstructured.SetNestedSlice(live.Object, []any{map[string]any{"key": "baseline", "operator": "Exists"}}, "spec", "template", "spec", "tolerations")
				annotations := live.GetAnnotations()
				delete(annotations, acceptedPathsAnnotation)
				live.SetAnnotations(annotations)
				if err := RecordAcceptance(live, source); err != nil {
					t.Fatal(err)
				}
			}
			switch location {
			case "combined":
				containers := append(addedFieldsContainers(live), map[string]any{"name": "sidecar", "image": "proxy:1"})
				containers[0].(map[string]any)["env"] = []any{map[string]any{"name": "ADDED", "value": "bad"}}
				_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
				_ = unstructured.SetNestedMap(live.Object, map[string]any{"nodeAffinity": map[string]any{"preferredDuringSchedulingIgnoredDuringExecution": []any{map[string]any{"weight": int64(1), "preference": map[string]any{"matchExpressions": []any{map[string]any{"key": "bad", "operator": "Exists"}}}}}}}, "spec", "template", "spec", "affinity")
			case "containers":
				_ = unstructured.SetNestedSlice(live.Object, append(addedFieldsContainers(live), map[string]any{"name": "sidecar", "image": "proxy:1"}), "spec", "template", "spec", "containers")
			case "env":
				items := addedFieldsContainers(live)
				items[0].(map[string]any)["env"] = []any{map[string]any{"name": "ADDED", "value": "bad"}}
				_ = unstructured.SetNestedSlice(live.Object, items, "spec", "template", "spec", "containers")
			case "affinity":
				_ = unstructured.SetNestedMap(live.Object, map[string]any{"nodeAffinity": map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{map[string]any{"key": "bad", "operator": "Exists"}}}}}}}, "spec", "template", "spec", "affinity")
			case "volumes":
				_ = unstructured.SetNestedSlice(live.Object, []any{map[string]any{"name": "extra", "emptyDir": map[string]any{}}}, "spec", "template", "spec", "volumes")
			case "tolerations", "accepted-list":
				items, _, _ := unstructured.NestedSlice(live.Object, "spec", "template", "spec", "tolerations")
				_ = unstructured.SetNestedSlice(live.Object, append(items, map[string]any{"key": "added", "operator": "Exists"}), "spec", "template", "spec", "tolerations")
			case "annotations":
				addAnnotation(live, "example.com/fault", "true")
			}
			addAnnotation(live, "deployment.kubernetes.io/revision", "9")
			ref := addedFieldsRef()
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
			if err != nil {
				t.Fatal(err)
			}
			added, err := addedSourceFields(client.objects[ref], source)
			if err != nil || len(added) > 0 {
				t.Fatalf("residual fields: %v %v", added, err)
			}
			if client.objects[ref].GetAnnotations()["deployment.kubernetes.io/revision"] != "9" {
				t.Fatal("controller annotation removed")
			}
			encoded, _ := json.Marshal(result.ActionReports[0])
			var report map[string]any
			_ = json.Unmarshal(encoded, &report)
			reconcile, ok := report["source_reconcile"].(map[string]any)
			if !ok || reconcile["converged"] != true {
				t.Fatalf("missing convergence receipt: %s", encoded)
			}
		})
	}
}

type reconcilingAdmissionClient struct {
	*planFakeClient
	mutate func(*unstructured.Unstructured)
}

func (c reconcilingAdmissionClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.planFakeClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	c.mutate(c.objects[ref])
	return nil
}

func TestSourceReconcilePartialSurvivesReceiptAndResponderResult(t *testing.T) {
	ref := addedFieldsRef()
	client := reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive()}}, mutate: func(live *unstructured.Unstructured) {
		_ = unstructured.SetNestedSlice(live.Object, append(addedFieldsContainers(live), map[string]any{"name": "injected", "image": "proxy:1"}), "spec", "template", "spec", "containers")
	}}
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	var partial *SourceReconcilePartialError
	if !errors.As(err, &partial) || !result.Applied || result.RolledBack || result.VerificationPassed {
		t.Fatalf("partial write classified as success or undone: %+v %v", result, err)
	}
	report := result.ActionReports[0]
	if report.Status != "partial" || report.SourceReconcile == nil || report.SourceReconcile.Converged || len(report.SourceReconcile.ResidualPaths) == 0 {
		t.Fatalf("untyped residual: %+v", report)
	}
	contract, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.Validate(contract); err != nil {
		t.Fatal(err)
	}
	restored := planApplicationState(contract).Result
	if !proto.Equal(restored.ActionReports[0].SourceReconcile, report.SourceReconcile) || restored.ActionReports[0].Status != "partial" {
		t.Fatalf("receipt lost residual: %+v", restored.ActionReports)
	}
	// Exercise the actual admission residual through repair projection,
	// validated persistence, reload and closure without losing timing evidence.
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	c := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	applications := []RecordedPlanApplication{{ID: "reconcile-residual", Result: result, Error: partial.Error(), StartedAt: now, CompletedAt: now.Add(time.Second)}}
	c.pendingClosure.Result.PlanApplications = applications
	c.pendingClosure.Result.RepairActions = ProgramRepairReceipts(applications, nil)
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")
	if err := c.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := c.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	saved, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.Validate(saved); err != nil {
		t.Fatal(err)
	}
	repairs := saved.GetPendingClosure().GetResult().GetRepairActions()
	if len(repairs) != 1 || repairs[0].GetFailureReason() != "source_residual" || !repairs[0].GetStartedAt().AsTime().Equal(now) || !repairs[0].GetCompletedAt().AsTime().Equal(now.Add(time.Second)) {
		t.Fatalf("residual repair evidence lost during persistence: %v", saved.GetPendingClosure())
	}
	if err := c.RestoreState(saved); err != nil {
		t.Fatal(err)
	}
	attemptClosure(t, c)
	if c.closureReceipt == nil {
		t.Fatal("residual never produced closure receipt")
	}
	encoded, err := json.Marshal(MenuRoundExecution{OptionID: "ReconcileFromSource", ActionResults: restored.ActionReports})
	if err != nil || !strings.Contains(string(encoded), "residual_paths") || !strings.Contains(string(encoded), "partial") {
		t.Fatalf("responder lost remaining work: %s %v", encoded, err)
	}
}

func TestSourceReconcileLeavesSubresourceControllerFields(t *testing.T) {
	source := addedFieldsSource()
	live := addedFieldsLive()
	_ = unstructured.SetNestedField(live.Object, int64(4), "spec", "replicas")
	live.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "horizontal-pod-autoscaler", Subresource: "scale", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}}})
	ref := addedFieldsRef()
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	if err != nil {
		t.Fatal(err)
	}
	replicas, _, _ := unstructured.NestedFieldNoCopy(client.objects[ref].Object, "spec", "replicas")
	if !planValuesEqual(replicas, int64(4)) || !result.ActionReports[0].SourceReconcile.Converged {
		t.Fatalf("controller field changed or compared as drift: %v %+v", replicas, result.ActionReports)
	}
}

type sourceRuntimeClient struct{ reconcilingAdmissionClient }

func (sourceRuntimeClient) Exec(context.Context, sdk.ObjectRef, string, []string) (int, error) {
	return 0, nil
}

func TestSourceReconcilePartialRetainedInMixedRuntimePlan(t *testing.T) {
	ref := addedFieldsRef()
	podRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: ref.Namespace, Name: "one"}
	pod := planObject(podRef, map[string]any{})
	pod.SetUID("pod-uid")
	pod.SetResourceVersion("7")
	client := sourceRuntimeClient{reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive(), podRef: pod}}, mutate: func(live *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(live.Object, int64(9), "spec", "replicas")
	}}}
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "command", ValueKind: program.StringValue, Default: "/bin/true"}}}}}
	plan := program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}, program.Exec{Pod: podRef, Container: "app", Command: []program.Value{program.FromParam("command")}}}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, plan, false)
	var partial *SourceReconcilePartialError
	if !errors.As(err, &partial) || !result.Applied || result.RolledBack || len(result.ActionsApplied) != 1 || result.ActionReports[0].Status != "partial" || result.ActionReports[0].SourceReconcile == nil {
		t.Fatalf("mixed plan lost applied partial receipt: applied=%v actions=%d reports=%+v error=%v", result.Applied, len(result.ActionsApplied), result.ActionReports, err)
	}
}

func TestSourceReconcileConvergesTopLevelAcceptedLists(t *testing.T) {
	for _, field := range []string{"subjects", "customEntries"} {
		t.Run(field, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding", Namespace: "test", Name: "binding"}
			source := planObject(ref, map[string]any{})
			delete(source.Object, "spec")
			source.Object["roleRef"] = map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "reader"}
			baseline := map[string]any{"kind": "User", "name": "baseline"}
			live := source.DeepCopy()
			live.Object[field] = []any{baseline}
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			live.Object[field] = []any{baseline, map[string]any{"kind": "User", "name": "added"}}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
			if err != nil {
				t.Fatal(err)
			}
			items, _, _ := unstructured.NestedSlice(client.objects[ref].Object, field)
			if len(items) != 1 || items[0].(map[string]any)["name"] != "baseline" {
				t.Fatalf("baseline lost or addition retained: %v", items)
			}
			report := result.ActionReports[0].SourceReconcile
			if !report.Converged || len(report.ResidualPaths) != 0 {
				t.Fatalf("residual: %v", report)
			}
		})
	}
}
