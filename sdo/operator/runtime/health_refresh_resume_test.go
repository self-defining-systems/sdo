package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

func TestRV6InterruptedProposalWithMemoryAdvance(t *testing.T) {
	ctx := context.Background()
	root, _, _, _, c := tracingFixture(t)
	before, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Broker.CreateProposalForPhase(ctx, before.RefreshID(), broker.InitialPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Broker.CreateProposalForPhase(ctx, before.RefreshID()+"-base-"+before.Commit, broker.InitialPhase); err != nil {
		t.Fatal(err)
	}
	tracingMemoryAdvance(t, root)
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "memory-only")
	after, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if before.RefreshID() != after.RefreshID() {
		t.Fatal("fixture changed refresh ID")
	}
	if _, err := c.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, after); err != nil {
		t.Fatalf("interrupted refresh cannot resume after memory-only advance: %v", err)
	}
}

func tracingMemoryAdvance(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, ".sdo/playbooks/base/README.md")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(content, []byte("\nMemory-only guidance.\n")...), 0644); err != nil {
		t.Fatal(err)
	}
}
