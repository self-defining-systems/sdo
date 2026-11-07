package runtime

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestChangedFieldPathsNamesDifferingFieldsWithoutValues(t *testing.T) {
	live := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"generation": int64(1), "resourceVersion": "1", "annotations": map[string]any{"a": "1"}},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "web", "image": "web@sha256:aa", "readOnly": false}}},
		"status":   map[string]any{"ready": int64(1)},
	}}
	preview := live.DeepCopy()
	preview.Object["metadata"].(map[string]any)["generation"] = int64(2)
	preview.Object["metadata"].(map[string]any)["resourceVersion"] = "2"
	preview.Object["status"] = map[string]any{}
	spec := preview.Object["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	spec["image"] = "web:v1"
	delete(spec, "readOnly")
	want := []string{"spec.containers[0].image", "spec.containers[0].readOnly"}
	if got := changedFieldPaths(live, preview); !reflect.DeepEqual(got, want) {
		t.Fatalf("changedFieldPaths = %v, want %v", got, want)
	}
	if got := changedFieldPaths(live, live.DeepCopy()); got == nil || len(got) != 0 {
		t.Fatalf("identical objects = %#v, want empty non-nil", got)
	}
}
