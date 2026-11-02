package runtime

import (
	"context"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"testing"
)

func TestRV6NoopJudgeAfterSourceAdvance(t *testing.T) {
	ctx := context.Background()
	root, _, _, _, c := tracingFixture(t)
	tracingRefresh(t, root, c)
	tracingWrite(t, root, "README.md", "documentation only")
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "developer documentation")
	state, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := c.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, state)
	if err != nil {
		t.Fatal(err)
	}
	after, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || receipt.Commit == state.Commit || len(receipt.ChangedPaths) != 0 || after.JudgeCommit != receipt.Commit {
		t.Fatalf("missing durable no-op acceptance: %+v %+v", receipt, after)
	}
	if nativeGit(t, root, "rev-parse", state.Commit+"^{tree}") != nativeGit(t, root, "rev-parse", receipt.Commit+"^{tree}") {
		t.Fatal("unchanged validation edited the accepted tree")
	}
	if after.NeedsRefresh {
		t.Fatalf("successful unchanged judge leaves health permanently unverified; receipt=%+v", receipt)
	}
}
