//go:build integration

package broker

import (
	"context"
	"testing"

	"sdo.dev/operator/memory"
)

// The base check reports what is wrong with the accepted tree before any agent
// edit, and nothing for a valid base: the empty diff is what the agent is for.
func TestCheckBaseReportsAnInvalidBaseAndAcceptsAValidOne(t *testing.T) {
	ctx := context.Background()
	w := newOnboardedWorld(t, false)
	// The judge's base is the accepted tree after the Go-written schema and architecture.
	head := gitOut(t, w.accepted, "rev-parse", "HEAD")
	if err := memory.WriteSchema(w.accepted); err != nil {
		t.Fatal(err)
	}
	if err := memory.WriteArchitecture(w.accepted, "test", head, "synthetic inventory"); err != nil {
		t.Fatal(err)
	}
	gitOut(t, w.accepted, "add", "-A")
	gitOut(t, w.accepted, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "synthetic schema and architecture")
	if _, err := w.broker.CreateProposalForPhase(ctx, "judge-clean", InitialPhase); err != nil {
		t.Fatal(err)
	}
	if got, err := w.broker.CheckBase(ctx, "judge-clean", HealthJudge, InitialPhase); err != nil || len(got) != 0 {
		t.Fatalf("valid base: diagnostics %v, err %v", got, err)
	}

	writeFiles(t, w.accepted, map[string]string{".sdo/stray.txt": "not an sdo artifact\n"})
	gitOut(t, w.accepted, "add", "-A")
	gitOut(t, w.accepted, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "synthetic invalid base")
	if _, err := w.broker.CreateProposalForPhase(ctx, "judge-bad", InitialPhase); err != nil {
		t.Fatal(err)
	}
	got, err := w.broker.CheckBase(ctx, "judge-bad", HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("an invalid base reported no diagnostics")
	}
	for _, d := range got {
		if d.GetRule() == "diff.empty" {
			t.Fatalf("the empty-diff diagnostic is not a base defect: %v", got)
		}
	}
}
