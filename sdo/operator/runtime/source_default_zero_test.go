package runtime

import (
	"encoding/json"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
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

func TestSourceComparisonMatchesKubernetesWireValues(t *testing.T) {
	source := defaultsTestDeployment(int64(1), map[string]any{"name": "data", "mountPath": "/data", "readOnly": false, "subPath": ""})
	live := defaultsTestDeployment(int64(1), map[string]any{"name": "data", "mountPath": "/data"})
	container := func(object *unstructured.Unstructured) map[string]any {
		items, _, _ := unstructured.NestedFieldNoCopy(object.Object, "spec", "template", "spec", "initContainers")
		return items.([]any)[0].(map[string]any)
	}
	declared, observed := container(source), container(live)
	declared["stdin"] = false
	declared["args"] = []any{}
	declared["securityContext"] = map[string]any{"runAsUser": int64(0)}
	observed["securityContext"] = map[string]any{"runAsUser": int64(0)}
	declared["readinessProbe"] = map[string]any{"httpGet": map[string]any{"port": int64(8080)}, "timeoutSeconds": int64(0)}
	observed["readinessProbe"] = map[string]any{"httpGet": map[string]any{"port": int64(8080), "scheme": "HTTP"}, "timeoutSeconds": int64(1)}
	declared["resources"] = map[string]any{"requests": map[string]any{"cpu": "100m"}, "limits": map[string]any{}}
	observed["resources"] = map[string]any{"requests": map[string]any{"cpu": "0.1"}}
	declared["volumeMounts"] = append(declared["volumeMounts"].([]any), map[string]any{"name": "other", "mountPath": "/other"})
	observed["volumeMounts"] = append([]any{map[string]any{"name": "other", "mountPath": "/other"}}, observed["volumeMounts"].([]any)...)

	if paths, _ := sourceResidualPaths(live, source); len(paths) != 0 {
		t.Errorf("wire-equivalent workload has residuals: %v", paths)
	}
	left, right := comparableFields(source.Object), comparableFields(live.Object)
	normalizeSourceWire(left, right, source.GroupVersionKind())
	a, _ := json.Marshal(left)
	b, _ := json.Marshal(right)
	diff := sdk.SourceDiff{Text: sdk.SourceComparisonText(a, b)}
	if changes, err := diff.ContainerChanges(); err != nil || len(changes) != 0 {
		t.Errorf("wire-equivalent containers changed: %v (%v)", changes, err)
	}
	observed["securityContext"] = map[string]any{"runAsUser": int64(1)}
	if paths, _ := sourceResidualPaths(live, source); !slices.Contains(paths, "spec.template.spec.initContainers[0].securityContext.runAsUser") {
		t.Errorf("explicit pointer zero must remain a real difference: %v", paths)
	}
}

func TestSourceComparisonMatchesServicePortDefaults(t *testing.T) {
	service := func(ports ...any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Service",
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
			"spec":     map[string]any{"ports": ports},
		}}
	}
	source := service(
		map[string]any{"port": int64(80), "targetPort": int64(0), "nodePort": int64(0), "protocol": ""},
		map[string]any{"port": int64(80), "protocol": "UDP", "targetPort": "dns"},
	)
	live := service(
		map[string]any{"port": int64(80), "protocol": "UDP", "targetPort": "dns"},
		map[string]any{"port": int64(80), "protocol": "TCP", "targetPort": int64(80)},
	)
	if paths, _ := sourceResidualPaths(live, source); len(paths) != 0 {
		t.Errorf("wire-equivalent Service has residuals: %v", paths)
	}
}
