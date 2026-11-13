package replay

import (
	"context"
	ktesting "k8s.io/client-go/testing"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type passVerifier struct{ reaction *reaction }

func (v passVerifier) Verify(ctx context.Context) (bool, error) { return true, v.reaction.finish(ctx) }

func stuckStatefulSetCluster(t *testing.T) (*cluster, sdk.ObjectRef, sdk.ObjectRef) {
	t.Helper()
	set := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": map[string]any{"name": "store", "namespace": "app", "uid": "uid-store", "resourceVersion": "1"},
		"spec": map[string]any{
			"podManagementPolicy": "OrderedReady",
			"updateStrategy":      map[string]any{"type": "RollingUpdate"},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "synthetic"}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "main", "image": "registry.example/synthetic:1"}}},
			},
		},
	}}
	pod := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "store-0", "namespace": "app", "uid": "uid-store-0", "resourceVersion": "1",
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "store", "uid": "uid-store", "controller": true}}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "main", "image": "registry.example/synthetic:1"}}},
		"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "False"}}},
	}}
	c, err := newCluster(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{set, pod}})
	if err != nil {
		t.Fatal(err)
	}
	return c, sdk.ObjectIdentity(set), sdk.ObjectIdentity(pod)
}

// The offline controller model must not roll a Pod the real StatefulSet
// controller would not: an OrderedReady rolling update waits on an unready Pod.
func TestReplayStatefulSetTemplateChangeLeavesPodTheControllerWillNotRoll(t *testing.T) {
	ctx := context.Background()
	c, setRef, podRef := stuckStatefulSetCluster(t)
	plan := program.Plan{Actions: []program.Action{program.RestartRollout{Ref: setRef, Reason: "restart"}}}
	if err := c.Patch(ctx, setRef, types.MergePatchType, []byte(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"t"}}}}}`)); err != nil {
		t.Fatal(err)
	}
	var effects []string
	w := &reaction{cluster: c, source: &source{}, plan: plan, effects: &effects}
	if err := w.finish(ctx); err != nil {
		t.Fatal(err)
	}
	live, err := c.Get(ctx, podRef)
	if err != nil || live.GetUID() != "uid-store-0" {
		t.Fatalf("stuck Pod = %v, %v; want the original Pod left in place (effects %v)", live, err, effects)
	}
}

// RestartRollout replaces that Pod itself; the controller model then recreates
// it from the new template.
func TestReplayRestartRolloutReplacesStuckStatefulSetPod(t *testing.T) {
	ctx := context.Background()
	c, setRef, podRef := stuckStatefulSetCluster(t)
	plan := program.Plan{Actions: []program.Action{program.RestartRollout{Ref: setRef, Reason: "restart"}}}
	var effects []string
	w := &reaction{cluster: c, source: &source{}, plan: plan, effects: &effects}
	executor := operator.PlanExecutor{Client: c, Rollouts: w, Verifier: passVerifier{w}}
	incident := operator.IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: setRef}}}
	result, err := executor.Apply(ctx, incident, plan, false)
	if err != nil {
		t.Fatalf("Apply = %+v, %v (effects %v)", result, err, effects)
	}
	live, err := c.Get(ctx, podRef)
	if err != nil || live.GetUID() == "uid-store-0" {
		t.Fatalf("Pod = %v, %v; want it replaced", live, err)
	}
	if !strings.Contains(strings.Join(effects, ","), "simulated:pod-recreation:store-0") {
		t.Fatalf("effects = %v, want the controller recreation", effects)
	}
	var reasons []string
	for _, effect := range result.ObjectEffects {
		if effect.Ref == podRef && effect.Operation == "delete" {
			reasons = append(reasons, effect.Reason)
		}
	}
	if len(reasons) != 1 || reasons[0] == "" {
		t.Fatalf("Pod delete receipts = %v, want one with a reason", reasons)
	}
}

// A template change makes the modelled StatefulSet controller publish a new
// updateRevision, so the executor can tell the Pods stuck on the old one.
func TestReplayTemplateChangeAdvancesStatefulSetUpdateRevision(t *testing.T) {
	ctx := context.Background()
	c, setRef, podRef := stuckStatefulSetCluster(t)
	for _, ref := range []sdk.ObjectRef{setRef, podRef} {
		object, _ := c.Get(ctx, ref)
		if ref == setRef {
			_ = unstructured.SetNestedField(object.Object, "store-old", "status", "updateRevision")
		} else {
			object.SetLabels(map[string]string{"controller-revision-hash": "store-old"})
			_ = unstructured.SetNestedSlice(object.Object, []any{map[string]any{"type": "Ready", "status": "False", "lastTransitionTime": "2020-01-01T00:00:00Z"}}, "status", "conditions")
		}
		if _, err := c.fake.Invokes(ktesting.NewUpdateAction(fixtureResource(ref), ref.Namespace, object), &unstructured.Unstructured{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Patch(ctx, setRef, types.MergePatchType, []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"main","image":"registry.example/synthetic:2"}]}}}}`)); err != nil {
		t.Fatal(err)
	}
	set, _ := c.Get(ctx, setRef)
	if revision, _, _ := unstructured.NestedString(set.Object, "status", "updateRevision"); revision == "" || revision == "store-old" {
		t.Fatalf("updateRevision = %q, want a new revision after a template change", revision)
	}
}
