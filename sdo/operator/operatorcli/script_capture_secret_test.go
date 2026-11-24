package operatorcli

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestScriptCaptureDoesNotPersistSecretImages(t *testing.T) {
	ctx := context.Background()
	const path = "/api/v1/namespaces/app/secrets/credentials"
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}
	image := func(canary string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace, "annotations": map[string]any{"kubectl.kubernetes.io/last-applied-configuration": canary}},
			"data":     map[string]any{"key": canary}, "stringData": map[string]any{"key": canary},
		}}
	}
	before, after := image("synthetic-old-canary"), image("synthetic-new-canary")
	recorder := newScriptRecorder(ctx, nil, nil)
	recorder.retain(path, before, after)
	recorded, _ := json.Marshal(recorder.objects[path])
	maps := fake.NewSimpleClientset().CoreV1().ConfigMaps("control")
	store := scriptCaptureStore{maps: maps, namespace: "control", application: "test", jobUID: "job"}
	if _, err := store.save(ctx, path, program.ScriptObjectCapture{Ref: ref, Before: before, After: after}); err != nil {
		t.Fatal(err)
	}
	stored, err := maps.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Items) != 3 {
		t.Fatalf("want a manifest and two images, got %d ConfigMaps", len(stored.Items))
	}
	for _, data := range append([][]byte{recorded}, storedImages(stored.Items)...) {
		if strings.Contains(string(data), "-canary") || strings.Contains(string(data), "last-applied") {
			t.Fatal("script capture retained Secret material")
		}
	}
}

func storedImages(items []corev1.ConfigMap) (images [][]byte) {
	for _, item := range items {
		images = append(images, item.BinaryData["image"])
	}
	return images
}
