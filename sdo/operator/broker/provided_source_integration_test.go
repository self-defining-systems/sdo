//go:build integration

package broker

import (
	"context"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// A provided deployment (the human's source is already accepted) publishes no
// source diff: the verified-source publish records the evidence on an empty
// accepted commit, and a repeat finds that commit instead of publishing again.
func TestPublishVerifiedSourceAcceptsAProvidedSourceWithNoDiff(t *testing.T) {
	w := newOnboardedWorld(t, false)
	ctx := context.Background()
	head := gitOut(t, w.accepted, "rev-parse", "HEAD")
	receipt, found, err := w.broker.AcceptedCommitAt(ctx, "onboard", InitialPhase, head)
	if err != nil || !found {
		t.Fatalf("onboarding commit is not the accepted head: found=%v err=%v", found, err)
	}
	if len(receipt.ChangedPaths) != 0 {
		t.Fatalf("provided source changed paths %v, want none", receipt.ChangedPaths)
	}
	if _, ok, err := w.broker.SourceEvidence(ctx, "onboard"); err != nil || !ok {
		t.Fatalf("no source evidence recorded: ok=%v err=%v", ok, err)
	}
	if gitOut(t, w.accepted, "show", "HEAD:"+layout.GoalFile) == "" {
		t.Fatal("accepted tree lost the goal")
	}
}
