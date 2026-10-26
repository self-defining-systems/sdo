package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestSourceDiffRecordingPreservesAbsentAcceptedProbe(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "web", "namespace": "app"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": "web:v1"}}}}},
	}}
	live := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web:v1", ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}}}}}}}}
	live.Annotations = withAcceptance(t, &live, source)
	// Apply preserves externally owned fields, so the dry run includes the probe.
	client := fake.NewSimpleClientset()
	client.PrependReactor("patch", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) { return true, live.DeepCopy(), nil })
	for _, tc := range []struct {
		name   string
		client kubernetes.Interface
	}{{"live", bridgeFake(t, client)}, {"offline", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &sdktest.RecordingReadView{ReadView: LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{live}}, Source: planFakeSource{source: source}, SourceCommit: "accepted", Client: tc.client}}
			diff, err := recorder.DiffAgainstSource(ref)
			if err != nil {
				t.Fatal(err)
			}
			parts := strings.SplitN(strings.TrimPrefix(diff.Text, "accepted: "), "\nobserved: ", 2)
			if len(parts) != 2 {
				t.Fatalf("missing field comparison: %+v", diff)
			}
			var accepted, observed map[string]any
			if err := json.Unmarshal([]byte(parts[0]), &accepted); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(parts[1]), &observed); err != nil {
				t.Fatal(err)
			}
			containers := func(obj map[string]any) []any {
				v, _, err := unstructured.NestedSlice(obj, "spec", "template", "spec", "containers")
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
			if _, ok := containers(accepted)[0].(map[string]any)["readinessProbe"]; ok {
				t.Fatal("accepted recording acquired the live-only probe")
			}
			if _, ok := containers(observed)[0].(map[string]any)["readinessProbe"]; !ok {
				t.Fatal("observed recording lost the live probe")
			}
		})
	}
}

func TestSourceDiffIgnoresLastAppliedAnnotationNoise(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings", "namespace": "app"}, "data": map[string]any{"mode": "stable"}}}
	live := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "stale"}}, Data: map[string]string{"mode": "stable"}}
	live.Annotations = withAcceptance(t, &live, source)
	client := fake.NewSimpleClientset()
	client.PrependReactor("patch", "configmaps", func(ktesting.Action) (bool, kruntime.Object, error) {
		obj := live.DeepCopy()
		obj.Annotations = nil
		return true, obj, nil
	})
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{live}}, Source: planFakeSource{source: source}, SourceCommit: "accepted", Client: bridgeFake(t, client)}
	if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed {
		t.Fatalf("bookkeeping annotation is not source drift: %+v %v", diff, err)
	}
}
