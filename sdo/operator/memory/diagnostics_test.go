package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func TestAcceptedDiagnosticsProjectsPhysicalStorage(t *testing.T) {
	repository := t.TempDir()
	gitCommand(t, repository, "init", "-q")
	root := filepath.Join(repository, ".sdo", "diagnostics")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(repository, layout.DetectorDir(layout.ClassHealth, "objective")), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(repository, layout.DetectorManifestPath(layout.ClassHealth, "objective"))
	if err := os.WriteFile(manifest, []byte("id: objective\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCommand(t, repository, "add", ".")
	gitCommand(t, repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base")
	projection, err := AcceptedDiagnostics(context.Background(), repository)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Directory != root || projection.Fingerprint == "" || projection.AcceptedRevision == "" {
		t.Fatalf("incomplete diagnostics projection: %+v", projection)
	}
	if err := os.WriteFile(manifest, []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := AcceptedDiagnostics(context.Background(), repository); err == nil {
		t.Fatal("dirty diagnostics projection was accepted")
	}
}

func TestPublishProberUsesEdgeOwnedCachePath(t *testing.T) {
	mount := t.TempDir()
	binary := filepath.Join(t.TempDir(), "prober")
	if err := os.WriteFile(binary, []byte("test-prober"), 0o600); err != nil {
		t.Fatal(err)
	}
	published, err := PublishProber(mount, binary)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(filepath.Dir(published)) != filepath.Join(mount, ".sdo-prober") {
		t.Fatalf("unexpected cache location: %s", published)
	}
	if info, err := os.Stat(published); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("published prober permissions: %v %v", info, err)
	}
}

func gitCommand(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
