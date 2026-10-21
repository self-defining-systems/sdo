package broker_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

func TestAcceptedCorpusUsesAcceptedBaseCommit(t *testing.T) {
	root := copyParityTree(t)
	runGit(t, root, "init", "-q")
	runGit(t, root, "config", "user.name", "T")
	runGit(t, root, "config", "user.email", "t@example.com")
	parityAcceptedProgram(t, root)
	recordCorpusOutcome(t, root)
	base, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	parityLabel(t, root, func(label map[string]any) { delete(label, "provenance") })
	runGit(t, root, "add", "-f", ".sdo")
	runGit(t, root, "commit", "-qm", "candidate erases accepted provenance")

	diagnostics, err := validate.Check(context.Background(), validate.Request{
		Root: root, Base: strings.TrimSpace(string(base)), Actor: validate.Responder, Phase: validate.ReflectionPhase,
		Publication: &validate.PublicationContext{BaseCommit: strings.TrimSpace(string(base)), IncidentID: "incident"}, Decision: &contracts.ReflectionDecision{LearningDecision: "updated"},
	}, func(context.Context, validate.Request) ([]validate.Diagnostic, error) {
		t.Fatal("candidate executed after rewriting accepted evidence")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := rules(nil, diagnostics).broker; len(got) != 1 || got[0] != validate.RuleCorpusImmutable {
		t.Fatalf("accepted base was not protected across candidate commits: %v", diagnostics)
	}
}

func TestCorpusRequiresProvisionedCaptureContext(t *testing.T) {
	for _, name := range []string{"missing directory", "absent directory", "file instead of directory", "missing incident", "empty provisioned context"} {
		t.Run(name, func(t *testing.T) {
			root := copyParityTree(t)
			runGit(t, root, "init", "-q")
			runGit(t, root, "config", "user.name", "T")
			runGit(t, root, "config", "user.email", "t@example.com")
			runGit(t, root, "add", "-A")
			runGit(t, root, "commit", "-qm", "base")
			recordCorpusOutcome(t, root)
			parityProgram(t, root)
			request := validate.Request{Root: root, Base: "HEAD", Actor: validate.Responder, Phase: validate.ReflectionPhase,
				Publication: &validate.PublicationContext{BaseCommit: "HEAD", IncidentID: "incident"}, Decision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}},
				CaptureDirectory: filepath.Join(filepath.Dir(root), "captures"), IncidentID: "incident"}
			ctx := context.Background()
			switch name {
			case "missing directory":
				request.CaptureDirectory = ""
			case "absent directory":
				request.CaptureDirectory = filepath.Join(t.TempDir(), "absent")
			case "file instead of directory":
				request.CaptureDirectory = filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(request.CaptureDirectory, []byte("capture"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing incident":
				request.IncidentID = ""
			case "empty provisioned context":
				// Caller-supplied paths cannot replace missing trusted context.
				request.CaptureRoot = t.TempDir()
				ctx = validate.WithCaptureSource(ctx, "", "")
			}
			diagnostics, err := validate.Check(ctx, request, func(context.Context, validate.Request) ([]validate.Diagnostic, error) {
				t.Fatal("candidate executed without provisioned capture context")
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := rules(nil, diagnostics).broker; len(got) != 1 || got[0] != validate.RuleCaptureUnavailable {
				t.Fatalf("missing capture context did not produce a named rejection: %v", diagnostics)
			}
		})
	}
}

func recordCorpusOutcome(t *testing.T, root string) {
	t.Helper()
	repository, err := broker.New(root, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.RecordOutcome(context.Background(), "incident", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
}
