package deployment

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestObservedDeploymentRequiresCurrentReadyGeneration(t *testing.T) {
	tree := strings.Repeat("a", 40)
	image := "registry.example/app@sha256:" + strings.Repeat("b", 64)
	base := map[string]any{
		"kind": "Deployment", "metadata": map[string]any{"name": "app", "namespace": "default", "uid": "uid-1", "generation": 3, "annotations": map[string]any{"sdo.dev/source-tree": tree}},
		"spec":   map[string]any{"replicas": 2, "template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"sdo.dev/source-tree": tree}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}},
		"status": map[string]any{"observedGeneration": 3, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2},
	}
	encode := func() []byte {
		b, err := json.Marshal(base)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	got, err := parseObservedWorkload("Deployment/default/app", encode())
	if err != nil || !got.Ready || got.Revision != tree || len(got.Images) != 1 || got.Images[0] != image {
		t.Fatalf("ready deployment rejected: %+v, %v", got, err)
	}
	base["status"].(map[string]any)["observedGeneration"] = 2
	got, err = parseObservedWorkload("Deployment/default/app", encode())
	if err != nil || got.Ready {
		t.Fatalf("stale generation accepted: %+v, %v", got, err)
	}
	base["status"].(map[string]any)["observedGeneration"] = 3
	base["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "registry.example/app:latest"
	if _, err := parseObservedWorkload("Deployment/default/app", encode()); err == nil {
		t.Fatal("mutable observed image accepted")
	}
}
