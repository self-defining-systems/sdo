package runtime

import (
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func defaultsTestDeployment(replicas any, mount map[string]any) *unstructured.Unstructured {
	spec := map[string]any{
		"template": map[string]any{"spec": map[string]any{"initContainers": []any{
			map[string]any{"name": "init", "image": "busybox", "volumeMounts": []any{mount}},
		}}},
	}
	if replicas != nil {
		spec["replicas"] = replicas
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "app", "namespace": "ns"},
		"spec":     spec,
	}}
}

// The API server omits false/zero scalars when it renders a source, yet a live
// read can carry them explicitly. A field the source does not set whose live
// value is that zero value is the server's default, not source drift. A zero
// value the API does not default to (replicas 0 where it defaults to 1) is.
func TestDefaultNormalizedAdditionsTreatsZeroValuesOfUnsetFieldsAsDefaults(t *testing.T) {
	source := defaultsTestDeployment(nil, map[string]any{"name": "data", "mountPath": "/data"})
	rendered := defaultsTestDeployment(float64(1), map[string]any{"name": "data", "mountPath": "/data"})
	observed := defaultsTestDeployment(float64(0), map[string]any{"name": "data", "mountPath": "/data", "readOnly": false})
	for _, object := range []*unstructured.Unstructured{rendered, observed} {
		container := object.Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["initContainers"].([]any)[0].(map[string]any)
		container["imagePullPolicy"], container["terminationMessagePath"] = "IfNotPresent", "/dev/termination-log"
	}

	readOnly := "spec.template.spec.initContainers[0].volumeMounts[0].readOnly"
	initPath := "spec.template.spec.initContainers[0]."
	paths := defaultNormalizedAdditions([]string{readOnly, "spec.replicas", initPath + "imagePullPolicy", initPath + "terminationMessagePath"}, observed, source, rendered)
	if slices.Contains(paths, readOnly) {
		t.Errorf("api-omitted false readOnly reported as source drift: %v", paths)
	}
	if !slices.Equal(paths, []string{"spec.replicas"}) {
		t.Errorf("a zero value the API does not default to must remain drift, got %v", paths)
	}
}
