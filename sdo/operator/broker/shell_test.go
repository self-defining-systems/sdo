package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestShellBrokerAcceptsReflectionWithoutCompileAndPreservesOwnership(t *testing.T) {
	root := repository(t)
	ctx := context.Background()
	b, err := New(root, t.TempDir(), nil, ObservationPolicy{CodePath: "shell"})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "shell-incident", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(proposal.Path, ".sdo/diagnostics/detectors/incidents/dns/check.sh")
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("exit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	receipt, err := b.PublishReflection(ctx, "shell-incident", &contracts.ReflectionDecision{LearningDecision: "updated"})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || git(t, root, "rev-parse", "HEAD") != receipt.Commit {
		t.Fatal("reflection was not accepted")
	}
	if required, err := b.RequiresControllerRollout(ctx, proposal.BaseCommit, receipt.Commit, []string{".sdo/diagnostics/detectors/incidents/dns/check.sh"}); err != nil || required {
		t.Fatalf("shell update requires rebuild: %v %v", required, err)
	}
	protected, err := b.CreateProposalForPhase(ctx, "denied", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	file = filepath.Join(protected.Path, ".sdo/diagnostics/detectors/health/health/check.sh")
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("exit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishReflection(ctx, "denied", &contracts.ReflectionDecision{LearningDecision: "updated"}); err == nil {
		t.Fatal("responder changed health detectors")
	}
}
