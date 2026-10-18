package deployment

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type recordedStrategy struct {
	tree string
}

func (strategy *recordedStrategy) ApplyAndVerify(_ context.Context, _ string, tree string, _ proto.Message) (Observation, error) {
	strategy.tree = tree
	return Observation{Revision: tree, Images: []string{"example@sha256:" + strings.Repeat("a", 64)}, ResourceIDs: []string{"Deployment/default/example"}, Ready: true}, nil
}

func candidateRepository(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if output, err := exec.Command("git", append([]string{"-C", path}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(path, "app.txt"), []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "-A"}, {"commit", "-qm", "base"}} {
		if output, err := exec.Command("git", append([]string{"-C", path}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	return path
}

func TestRegistryBindsVerifiedReceiptToUncommittedCandidateTree(t *testing.T) {
	checkout := candidateRepository(t)
	if err := os.WriteFile(filepath.Join(checkout, "app.txt"), []byte("proposal"), 0o644); err != nil {
		t.Fatal(err)
	}
	strategy := &recordedStrategy{}
	registry, err := NewRegistry(Registration{ID: "raw-manifests", Config: &contracts.RawManifestStrategy{}, Apply: strategy.ApplyAndVerify})
	if err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawManifestStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := registry.ApplyAndVerify(context.Background(), checkout, &contracts.DeploymentPlan{StrategyId: "raw-manifests", StrategyConfig: config})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProposalTreeDigest == "" || receipt.ProposalTreeDigest != strategy.tree || receipt.ObservedRevision != strategy.tree || !receipt.Ready {
		t.Fatalf("wrong authoritative receipt: %+v", receipt)
	}
	if status, err := exec.Command("git", "-C", checkout, "status", "--porcelain").CombinedOutput(); err != nil || !strings.HasPrefix(string(status), " M app.txt") {
		t.Fatalf("candidate index was mutated: %q, %v", status, err)
	}
}

func TestRegistryRejectsUnknownStrategyAndWrongAnyType(t *testing.T) {
	strategy := &recordedStrategy{}
	registry, err := NewRegistry(Registration{ID: "raw-manifests", Config: &contracts.RawManifestStrategy{}, Apply: strategy.ApplyAndVerify})
	if err != nil {
		t.Fatal(err)
	}
	checkout := candidateRepository(t)
	if _, err := registry.ApplyAndVerify(context.Background(), checkout, &contracts.DeploymentPlan{StrategyId: "unknown"}); err == nil {
		t.Fatal("unknown strategy accepted")
	}
	wrong, err := anypb.New(&contracts.SourceDeploymentRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ApplyAndVerify(context.Background(), checkout, &contracts.DeploymentPlan{StrategyId: "raw-manifests", StrategyConfig: wrong}); err == nil {
		t.Fatal("wrong strategy config type accepted")
	}
}
