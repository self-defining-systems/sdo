package operatorcli

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	"strings"
	"testing"
)

func TestInstallExistingAdoptionRequiresExplicitOptIn(t *testing.T) {
	args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--controller-image", "controller:test", "--responder-image", "agent:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "credentials", "--model", "model", "--adopt-existing-memory"}
	if _, err := ParseInstallExistingLaunch(args); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	gitIn(t, root, "init", "-q")
	gitIn(t, root, "config", "user.name", "T")
	gitIn(t, root, "config", "user.email", "t@example.com")
	gitIn(t, root, "commit", "--allow-empty", "-qm", "base")
	if err := os.MkdirAll(filepath.Join(root, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/goal.md"), []byte("unaccepted memory\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := acceptedHead(context.Background(), root); err == nil {
		t.Fatal("default install accepted uncommitted memory")
	}
}

func TestAdoptionCompilesAsIsAndLaterValidationStillRejects(t *testing.T) {
	root := t.TempDir()
	fixture := "../../../tests/fixtures/sdo/playbook_program_app"
	if err := os.CopyFS(root, os.DirFS(fixture)); err != nil {
		t.Fatal(err)
	}
	gitIn(t, root, "init", "-q")
	gitIn(t, root, "config", "user.name", "T")
	gitIn(t, root, "config", "user.email", "t@example.com")
	gitIn(t, root, "add", ".")
	gitIn(t, root, "commit", "-qm", "seed")
	if err := os.RemoveAll(filepath.Join(root, ".sdo/corpus")); err != nil {
		t.Fatal(err)
	}
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	commit, err := prepareInstallExistingRepository(context.Background(), InstallExistingConfig{Repository: root, OperatorRoot: operator, AdoptExistingMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	message := gitIn(t, root, "show", "-s", "--format=%B", commit)
	if !strings.Contains(message, "SDO-Origin: adopted-existing") || !strings.Contains(message, "SDO-Validation: skipped") {
		t.Fatal(message)
	}
	// Changing adopted program bytes must pass normal corpus validation.
	if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/diagnose.go"), []byte("package targetport\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.BuildWorkspace(context.Background(), builder.Config{ApplicationRoot: root, DiagnosticsRoot: filepath.Join(root, ".sdo/diagnostics"), OperatorRoot: operator}); err == nil {
		t.Fatal("later validator accepted program without corpus")
	}
	gitIn(t, root, "restore", "--", ".sdo/playbooks/service-targetport-mismatch/diagnose.go")
	b, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposal(context.Background(), "later")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo/goal.md"), []byte("unauthorized change\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "later", broker.Responder, broker.ProposalPhase); err == nil {
		t.Fatal("adoption bypassed later broker ownership")
	}
}
