package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func healthTestGit(t *testing.T, root string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return string(out)
}
func healthWrite(t *testing.T, root, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}
func healthCommit(t *testing.T, root, message string) {
	t.Helper()
	healthTestGit(t, root, "add", ".")
	healthTestGit(t, root, "commit", "-qm", message)
}
func TestHealthRevisionColdWarmAndMemoryOnly(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	healthTestGit(t, root, "init", "-q")
	healthTestGit(t, root, "config", "user.email", "test@example.com")
	healthTestGit(t, root, "config", "user.name", "Test")
	healthWrite(t, root, "app.yaml", "jaeger")
	if err := WriteGoal(root, "app", "Keep the intended application healthy."); err != nil {
		t.Fatal(err)
	}
	healthWrite(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "old detector")
	healthCommit(t, root, "seed without provenance")
	state, err := AcceptedHealthRevision(ctx, root)
	if err != nil || !state.NeedsRefresh {
		t.Fatalf("unproven seed trusted: %+v %v", state, err)
	}
	healthWrite(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "judge detector")
	healthCommit(t, root, "judge\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed")
	state, err = AcceptedHealthRevision(ctx, root)
	if err != nil || state.NeedsRefresh {
		t.Fatalf("accepted judge stale: %+v %v", state, err)
	}
	healthWrite(t, root, ".sdo/playbooks/incident/README.md", "memory")
	healthCommit(t, root, "memory-only")
	memoryOnly, err := AcceptedHealthRevision(ctx, root)
	if err != nil || memoryOnly.NeedsRefresh {
		t.Fatalf("memory-only requires judge: %+v %v", memoryOnly, err)
	}
	healthWrite(t, root, "app.yaml", "collector")
	healthCommit(t, root, "developer swaps tracing")
	cold, err := AcceptedHealthRevision(ctx, root)
	if err != nil || !cold.NeedsRefresh {
		t.Fatalf("cold source advance trusted: %+v %v", cold, err)
	}
	if state.SourceInputs == cold.SourceInputs {
		t.Fatal("warm source advance not detected")
	}
	healthWrite(t, root, ".sdo/outcomes/test.json", "{}")
	healthCommit(t, root, "outcome")
	again, err := AcceptedHealthRevision(ctx, root)
	if err != nil || again.RefreshID() != cold.RefreshID() {
		t.Fatalf("refresh identity changed with memory: %+v %v", again, err)
	}
	if err := WriteGoal(root, "app", "Check user requests."); err != nil {
		t.Fatal(err)
	}
	healthCommit(t, root, "new objective")
	objective, err := AcceptedHealthRevision(ctx, root)
	if err != nil || objective.RefreshID() == cold.RefreshID() {
		t.Fatalf("objective absent from identity: %+v %v", objective, err)
	}
	healthWrite(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "different detector")
	healthCommit(t, root, "detector advance")
	detector, err := AcceptedHealthRevision(ctx, root)
	if err != nil || detector.RefreshID() == objective.RefreshID() {
		t.Fatalf("detector revision absent from identity: %+v %v", detector, err)
	}
}
