package deployment

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

func TestApplyTakesOverFieldsOwnedByAnotherManager(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "args")
	script := filepath.Join(dir, "kubectl")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho \"$@\" > "+record+"\ncat >/dev/null\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := (CommandBackend{KubectlBinary: script}).Apply(context.Background(), []byte("kind: ConfigMap")); err != nil {
		t.Fatal(err)
	}
	args, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	// The benchmark (or an operator) may have deployed the workload with kubectl first; SDO adopts it.
	if !strings.Contains(string(args), "--server-side") || !strings.Contains(string(args), "--force-conflicts") {
		t.Fatalf("apply must take over fields owned by other managers: %s", args)
	}
}
