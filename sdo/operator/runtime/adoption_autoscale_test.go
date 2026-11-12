package runtime

import (
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type hpaView struct {
	program.ReadView
	hpas []autoscalingv2.HorizontalPodAutoscaler
}

func (v hpaView) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler { return v.hpas }

// A live autoscaler the chart does not render still owns its target's replica
// count: adoption must leave it, so the target is found from the read view as
// well as from the accepted manifests.
func TestAdoptionFindsAutoscaledWorkloadsFromTheLiveViewAndTheAcceptedManifests(t *testing.T) {
	live := autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"},
		Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}},
	}
	c := &Controller{config: ControllerConfig{Namespace: "app"}}
	c.ConfigureDrift(DriftControllerConfig{View: func(s sdk.DetectionContext) program.ReadView {
		return hpaView{ReadView: &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}, hpas: []autoscalingv2.HorizontalPodAutoscaler{live}}
	}})
	rendered := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
		"metadata": map[string]any{"name": "api", "namespace": "app"},
		"spec":     map[string]any{"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "api"}},
	}}
	autoscaled := c.autoscaledTargets(sdktest.Snapshot{NamespaceName: "app"}, []*unstructured.Unstructured{rendered})
	for _, ref := range []sdk.ObjectRef{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"},
		{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "api"},
	} {
		if !autoscaled.Targets(ref) {
			t.Errorf("%s/%s is autoscaled and was not found", ref.Kind, ref.Name)
		}
	}
}

// Adoption applies as sdo-deployer and claims what the deployer applied. A
// field another controller wrote (the Deployment controller's revision
// annotation) is never carried into the applied document, so adoption neither
// owns nor changes it.
func TestAdoptionNeverCarriesFieldsAControllerOwns(t *testing.T) {
	live := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "web", "namespace": "app",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4", "example.com/owner": "team"},
			"managedFields": []any{
				map[string]any{"manager": "sdo-deployer", "operation": "Apply", "fieldsType": "FieldsV1",
					"fieldsV1": map[string]any{"f:metadata": map[string]any{"f:annotations": map[string]any{"f:example.com/owner": map[string]any{}}}}},
				map[string]any{"manager": "kube-controller-manager", "operation": "Update", "fieldsType": "FieldsV1",
					"fieldsV1": map[string]any{"f:metadata": map[string]any{"f:annotations": map[string]any{"f:deployment.kubernetes.io/revision": map[string]any{}}}}},
			},
		},
	}}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "web", "namespace": "app"}}}
	applied := carryOwnedOmissions(carrySourceTreeStamp(source, live), live)
	annotations := applied.GetAnnotations()
	if _, found := annotations["deployment.kubernetes.io/revision"]; found {
		t.Errorf("adoption would apply the controller's revision annotation: %v", annotations)
	}
	if annotations["example.com/owner"] != "team" {
		t.Errorf("the deployer's own annotation was dropped: %v", annotations)
	}
}

// Drift and repair compare the accepted source with live through the same
// comparable fields: an autoscaler's replica count is never a difference, and
// the same object without an autoscaler still shows it.
func TestSourceComparisonIgnoresAutoscalerOwnedReplicas(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	hpa := autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"},
		Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}},
	}
	object := func() map[string]any {
		return map[string]any{"spec": map[string]any{"replicas": int64(5), "paused": false}}
	}
	scaled := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", HPAList: []autoscalingv2.HorizontalPodAutoscaler{hpa}})}
	if _, found, _ := unstructured.NestedFieldNoCopy(scaled.sourceComparableFields(object(), ref), "spec", "replicas"); found {
		t.Error("an autoscaled Deployment's replicas are compared")
	}
	if paths := scaled.sourceDriftPaths([]string{"spec.replicas", "spec.paused"}, ref); len(paths) != 1 || paths[0] != "spec.paused" {
		t.Errorf("drift paths = %v, want only spec.paused", paths)
	}
	plain := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"})}
	if _, found, _ := unstructured.NestedFieldNoCopy(plain.sourceComparableFields(object(), ref), "spec", "replicas"); !found {
		t.Error("replicas of a Deployment no autoscaler targets must stay compared")
	}
}
