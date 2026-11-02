package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func simService(targetPort int64) *unstructured.Unstructured {
	return simServiceNamed("api", targetPort)
}

func simServiceNamed(name string, targetPort int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"namespace": "app", "name": name},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": int64(80), "targetPort": targetPort}}},
	}}
}
