package builder

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

const playbookProgramFixture = "../../../tests/fixtures/sdo/playbook_program_app/.sdo"

// Acceptance and activation apply one rule to authored playbook tests: they
// are informational. A tree whose authored playbook test panics passes Check,
// so the memory host built from that accepted tree must build too; otherwise
// the broker publishes a commit the controller can never activate.
func TestAcceptedMemoryWithFailingPlaybookTestActivates(t *testing.T) {
	app := t.TempDir()
	if err := os.CopyFS(filepath.Join(app, ".sdo"), os.DirFS(playbookProgramFixture)); err != nil {
		t.Fatal(err)
	}
	playbook := filepath.Join(app, ".sdo", "playbooks", "service-targetport-mismatch")
	if err := os.WriteFile(filepath.Join(playbook, "diagnose_test.go"), []byte("package targetport\n\nimport \"testing\"\n\nfunc TestPanics(t *testing.T) { panic(\"synthetic\") }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.FakeGo(t, testutil.GoRule{Verb: "test", Covers: "playbooks/service-targetport-mismatch", Output: "panic: synthetic\n--- FAIL: TestPanics"})
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo", "diagnostics"), OperatorRoot: "..", Replay: "off"})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if err := workspace.Check(context.Background()); err != nil {
		t.Fatalf("acceptance rejected an informational playbook test failure: %v", err)
	}
	if len(workspace.advisoryTestErrors) == 0 {
		t.Fatal("acceptance did not report the failing playbook test as advisory")
	}
	if _, err := workspace.BuildMemoryHost(context.Background()); err != nil {
		t.Fatalf("accepted memory did not activate: %v", err)
	}
}
