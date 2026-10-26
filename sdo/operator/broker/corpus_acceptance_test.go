package broker_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

func TestAcceptedCorpusUsesAcceptedBaseCommit(t *testing.T) {
	root := copyParityTree(t)
	testutil.InitGit(t, root)
	runGit(t, root, "config", "user.name", "T")
	runGit(t, root, "config", "user.email", "t@example.com")
	parityAcceptedProgram(t, root)
	recordCorpusOutcome(t, root, false)
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

func TestSessionFixtureRequiresAnIndependentHealthyReference(t *testing.T) {
	for _, name := range []string{"missing directory", "absent directory", "file instead of directory", "missing incident", "empty provisioned context", "healthy reference only"} {
		t.Run(name, func(t *testing.T) {
			root := copyParityTree(t)
			testutil.InitGit(t, root)
			runGit(t, root, "config", "user.name", "T")
			runGit(t, root, "config", "user.email", "t@example.com")
			runGit(t, root, "add", "-A")
			runGit(t, root, "commit", "-qm", "base")
			recordCorpusOutcome(t, root, true)
			parityProgram(t, root)
			if err := os.CopyFS(filepath.Join(root, ".sdo/diagnostics/detectors/incidents/service-targetport-mismatch"), os.DirFS("../../../tests/fixtures/sdo/service_port_detector")); err != nil {
				t.Fatal(err)
			}
			request := validate.Request{Root: root, Base: "HEAD", Actor: validate.Responder, Phase: validate.ReflectionPhase,
				Publication: &validate.PublicationContext{BaseCommit: "HEAD", IncidentID: "incident"}, Decision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}},
				CaptureDirectory: filepath.Join(filepath.Dir(root), "captures"), IncidentID: "incident"}
			ctx := context.Background()
			switch name {
			case "healthy reference only":
				request.CaptureDirectory = t.TempDir()
				put(t, request.CaptureDirectory, "healthy/healthy.json", `{"namespace":"demo"}`, 0644)
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
			compiled := false
			diagnostics, err := validate.Check(ctx, request, func(context.Context, validate.Request) ([]validate.Diagnostic, error) {
				compiled = true
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if name == "healthy reference only" {
				if len(diagnostics) != 0 || !compiled {
					t.Fatalf("session-authored positive fixture rejected despite trusted healthy reference: %v", diagnostics)
				}
			} else if len(diagnostics) != 1 || diagnostics[0].Rule != validate.RuleDetectorHealthyBaseline || compiled {
				t.Fatalf("missing independent healthy reference did not fail closed: %v compiled=%v", diagnostics, compiled)
			}
		})
	}
}

func recordCorpusOutcome(t *testing.T, root string, verified bool) {
	t.Helper()
	repository, err := broker.New(root, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}
	if verified {
		outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: "verified service port repair", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}}}
	}
	parityFindingRepairProof(outcome)
	if _, err := repository.RecordOutcome(context.Background(), "incident", outcome); err != nil {
		t.Fatal(err)
	}
}
