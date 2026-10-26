package broker_test

import (
	"context"
	"errors"
	"os/exec"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
)

func TestOperatorImportContract(t *testing.T) {
	for _, name := range []string{"valid", "missing operator corpus", "claims capture", "health detector", "responder import", "validator rejects"} {
		t.Run(name, func(t *testing.T) {
			root := copyParityTree(t)
			testutil.InitGit(t, root)
			runGit(t, root, "config", "user.name", "T")
			runGit(t, root, "config", "user.email", "t@example.com")
			runGit(t, root, "add", "-A")
			runGit(t, root, "commit", "-qm", "accepted base")
			validatorCalled := false
			repo, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error {
				validatorCalled = true
				if name == "validator rejects" {
					return errors.New("independent replay rejected candidate")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			proposal, err := repo.CreateProposalForPhase(context.Background(), "operator-1", broker.Phase("import"))
			if err != nil {
				t.Fatal(err)
			}
			parityProgram(t, proposal.Path)
			parityLabel(t, proposal.Path, func(labels map[string]any) { delete(labels, "provenance") })
			if name != "missing operator corpus" {
				put(t, proposal.Path, ".sdo/corpus/targetport-mismatch/operator.json", `{"schema_version":1,"origin":"operator","description":"Hand-written service port fault"}`, 0644)
				put(t, proposal.Path, ".sdo/corpus/targetport-mismatch/accepted.json", `[]`, 0644)
			}
			if name == "claims capture" {
				parityLabel(t, proposal.Path, func(labels map[string]any) { labels["provenance"] = map[string]string{"incident_id": "fake"} })
			}
			if name == "health detector" {
				put(t, proposal.Path, ".sdo/diagnostics/detectors/health/ready/detector.go", "package detector\n", 0644)
			}
			actor := broker.Human
			if name == "responder import" {
				actor = broker.Responder
			}
			receipt, err := repo.Publish(context.Background(), "operator-1", actor, broker.Phase("import"))
			if name != "valid" && name != "missing operator corpus" {
				if err == nil {
					t.Fatal("invalid import accepted")
				}
				if name == "validator rejects" && !validatorCalled {
					t.Fatal("did not reach independent validator")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := memory.ReadProgramLearningEvidence(context.Background(), root, receipt.Commit)
			if err != nil || len(evidence) != 0 {
				t.Fatalf("import earned learning credit: %v %v", evidence, err)
			}
			if receipt.Commit == "" || !validatorCalled {
				t.Fatal("missing validated receipt")
			}
			message, err := exec.Command("git", "-C", root, "show", "-s", "--format=%B", receipt.Commit).Output()
			if err != nil || !strings.Contains(string(message), "SDO-Origin: operator") || !strings.Contains(string(message), "SDO-Actor: human") {
				t.Fatalf("missing operator provenance: %s %v", message, err)
			}
		})
	}
}
