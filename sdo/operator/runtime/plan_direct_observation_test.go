package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

func TestPlanUnwatchedKindsUseDirectCurrentProof(t *testing.T) {
	for _, kind := range []string{"HorizontalPodAutoscaler", "PodDisruptionBudget", "MutatingWebhookConfiguration"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{Namespace: "app", Name: "fixture", Kind: kind}
			path := "spec.maxReplicas"
			switch kind {
			case "HorizontalPodAutoscaler":
				ref.APIVersion = "autoscaling/v2"
			case "PodDisruptionBudget":
				ref.APIVersion = "policy/v1"
				path = "spec.minAvailable"
			default:
				ref.APIVersion = "admissionregistration.k8s.io/v1"
				ref.Namespace = ""
			}
			before := planObject(ref, map[string]any{"maxReplicas": float64(1), "minAvailable": float64(1)})
			before.SetUID("fixture-uid")
			before.SetResourceVersion("1")
			after := before.DeepCopy()
			if kind == "HorizontalPodAutoscaler" {
				after.Object["spec"].(map[string]any)["maxReplicas"] = float64(2)
			} else {
				after.Object["spec"].(map[string]any)["minAvailable"] = float64(2)
			}
			safety := fixturePlanSafety(t, ref, before, after)
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
			var action program.Action = program.SetField{Ref: ref, Path: path, Value: testPlanValue(2)}
			if kind == "MutatingWebhookConfiguration" {
				proof := safety.Objects[ref]
				proof.DeleteAllowed = true
				safety.Objects[ref] = proof
				action = program.DeleteObject{Ref: ref, Reason: "synthetic fixture"}
			}
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, Safety: safety, AcceptedRevision: func() string { return "accepted" }, Observer: &KubernetesCache{informers: map[string]cache.SharedIndexInformer{}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result, err := e.Apply(ctx, IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("direct proof did not discharge unwatched barrier: %v", err)
			}
		})
	}
}
func TestPlanCacheClusterDeleteRequiresObservedAbsence(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "fixture"}
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, 0, cache.Indexers{})
	object := planObject(ref, nil)
	if err := informer.GetStore().Add(object); err != nil {
		t.Fatal(err)
	}
	c := &KubernetesCache{informers: map[string]cache.SharedIndexInformer{ref.APIVersion + "/" + ref.Kind: informer}}
	writes := []ObservedWrite{{Ref: ref, Strict: true, Absent: true}}
	if c.observed(writes) {
		t.Fatal("present cluster-scoped object reported absent")
	}
	_ = informer.GetStore().Delete(object)
	if !c.observed(writes) {
		t.Fatal("absent cluster-scoped object not observed")
	}
}
