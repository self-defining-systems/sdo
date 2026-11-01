//go:build integration

package memory

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceInventoryBindsAcceptedCommitAndExcludesMemory(t *testing.T) {
	root := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init")
	run("config", "user.email", "test@example.com")
	run("config", "user.name", "Test")
	if err := os.Mkdir(filepath.Join(root, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "deploy.yaml"), []byte("kind: Deployment\nmetadata:\n  name: demo\n  namespace: demo\nspec:\n  template:\n    spec:\n      containers:\n      - name: app\n        image: demo:v1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo", "goal.md"), []byte("first"), 0644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-m", "source")
	accepted := run("rev-parse", "HEAD")
	first, err := SourceInventory(context.Background(), root, accepted)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Resources) != 1 || first.Resources[0].Kind != "Deployment" || first.Resources[0].Source != "deploy.yaml" || !strings.Contains(strings.Join(first.Resources[0].Dependencies, ","), "image:demo:v1") {
		t.Fatalf("unexpected source resources: %+v", first.Resources)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo", "goal.md"), []byte("second"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "deploy.yaml"), []byte("kind: Service\nmetadata:\n  name: different\n"), 0644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-m", "later")
	second, err := SourceInventory(context.Background(), root, accepted)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(first.Resources) != fmt.Sprint(second.Resources) || len(second.Resources) != 1 || second.Resources[0].Kind != "Deployment" {
		t.Fatalf("inventory drifted from accepted source: first=%+v second=%+v", first, second)
	}
	if _, err := SourceInventory(context.Background(), root, strings.Repeat("0", 40)); err == nil {
		t.Fatal("unknown accepted commit was allowed")
	}
}

func TestManifestInventoryIncludesSelectorsAndPodImages(t *testing.T) {
	facts := sourceDependencies(map[string]any{"kind": "Pod", "spec": map[string]any{
		"selector":   map[string]any{"matchLabels": map[string]any{"app": "api"}},
		"containers": []any{map[string]any{"image": "example/api:v1"}},
	}})
	got := strings.Join(facts, ",")
	if !strings.Contains(got, "selector:app=api") || !strings.Contains(got, "image:example/api:v1") {
		t.Fatalf("manifest inventory omitted declared facts: %s", got)
	}
}
