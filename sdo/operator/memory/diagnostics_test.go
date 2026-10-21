package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestAcceptedDiagnosticsProjectsPhysicalStorage(t *testing.T) {
	repository := t.TempDir()
	gitCommand(t, repository, "init", "-q")
	root := filepath.Join(repository, ".sdo", "diagnostics")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("kind: DetectorManifest\n"), 0o644); err != nil {
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
	if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := AcceptedDiagnostics(context.Background(), repository); err == nil {
		t.Fatal("dirty diagnostics projection was accepted")
	}
}

func TestProgramSourceChangeRequiresControllerRollout(t *testing.T) {
	for _, path := range []string{".sdo/playbooks/fault/diagnose.go", ".sdo/playbooks/fault/mitigate.go", ".sdo/lib/ports/ports.go"} {
		if !RequiresControllerRollout([]string{path}) {
			t.Fatalf("accepted Go program change %s did not require a rebuilt registry", path)
		}
	}
	if RequiresControllerRollout([]string{".sdo/playbooks/fault/README.md"}) {
		t.Fatal("prose-only reflection needlessly restarts the controller")
	}
}

func TestAcceptedControllerFingerprintChangesWithProgramSource(t *testing.T) {
	repository := t.TempDir()
	gitCommand(t, repository, "init", "-q")
	for path, content := range map[string]string{".sdo/diagnostics/manifest.yaml": "kind: DetectorManifest\n", ".sdo/playbooks/fault/diagnose.go": "package fault\n"} {
		full := filepath.Join(repository, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitCommand(t, repository, "add", ".")
	gitCommand(t, repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base")
	before, err := AcceptedDiagnostics(context.Background(), repository)
	if err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(repository, ".sdo/playbooks/fault/diagnose.go")
	if err := os.WriteFile(program, []byte("package fault\n// updated\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCommand(t, repository, "add", ".")
	gitCommand(t, repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "program")
	after, err := AcceptedDiagnostics(context.Background(), repository)
	if err != nil {
		t.Fatal(err)
	}
	if before.Fingerprint == after.Fingerprint {
		t.Fatal("program source did not change compiled controller fingerprint")
	}
}

func TestAcceptedDiagnosticsRejectsDirtyProgramMemory(t *testing.T) {
	repository := t.TempDir()
	gitCommand(t, repository, "init", "-q")
	for path, content := range map[string]string{".sdo/diagnostics/manifest.yaml": "kind: DetectorManifest\n", ".sdo/playbooks/fault/diagnose.go": "package fault\n", ".sdo/lib/helpers/helpers.go": "package helpers\n", ".sdo/corpus/case-1/labels.json": "{}\n"} {
		full := filepath.Join(repository, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitCommand(t, repository, "add", ".")
	gitCommand(t, repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base")
	for _, path := range []string{".sdo/playbooks/fault/diagnose.go", ".sdo/lib/helpers/helpers.go", ".sdo/corpus/case-1/labels.json"} {
		full := filepath.Join(repository, path)
		if err := os.WriteFile(full, []byte("changed"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := AcceptedDiagnostics(context.Background(), repository); err == nil {
			t.Fatalf("dirty %s accepted", path)
		}
		gitCommand(t, repository, "checkout", "--", path)
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
