package memory

import (
	"context"
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
	if first.Fingerprint != second.Fingerprint || len(second.Resources) != 1 || second.Resources[0].Kind != "Deployment" {
		t.Fatalf("inventory drifted from accepted source: first=%+v second=%+v", first, second)
	}
	if _, err := SourceInventory(context.Background(), root, strings.Repeat("0", 40)); err == nil {
		t.Fatal("unknown accepted commit was allowed")
	}
}

func TestSourceInventoryStreamsLargeNonManifestAssets(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init")
	git("config", "user.email", "test@example.invalid")
	git("config", "user.name", "Synthetic")
	asset, err := os.Create(filepath.Join(root, "asset.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if err := asset.Truncate(65 << 20); err != nil {
		t.Fatal(err)
	}
	if err := asset.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "service.yaml"), []byte("kind: Service\nmetadata:\n  name: synthetic\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "synthetic large asset")
	facts, err := SourceInventory(context.Background(), root, git("rev-parse", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Resources) != 1 || len(facts.Fingerprint) != 64 {
		t.Fatalf("missing bounded projection: %+v", facts)
	}
	asset, err = os.OpenFile(filepath.Join(root, "asset.bin"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := asset.WriteAt([]byte("changed"), 64<<20); err != nil {
		t.Fatal(err)
	}
	if err := asset.Close(); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "change asset beyond old byte limit")
	after, err := SourceInventory(context.Background(), root, git("rev-parse", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if after.Fingerprint == facts.Fingerprint {
		t.Fatal("large asset content was omitted from fingerprint")
	}
	if err := os.Rename(filepath.Join(root, "asset.bin"), filepath.Join(root, "asset.yaml")); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "synthetic oversized YAML")
	if _, err := SourceInventory(context.Background(), root, git("rev-parse", "HEAD")); err == nil || !strings.Contains(err.Error(), "YAML inventory exceeds") {
		t.Fatalf("YAML parsing lost its input bound: %v", err)
	}
}
