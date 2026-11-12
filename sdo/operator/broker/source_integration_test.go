//go:build integration

package broker

import (
	"context"
	"os"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/validate"
)

func commitInCheckout(t *testing.T, checkout, message string, paths ...string) {
	t.Helper()
	testutil.ConfigureGit(t, checkout)
	testutil.Git(t, checkout, append([]string{"add"}, paths...)...)
	testutil.Git(t, checkout, "commit", "-qm", message)
}

func responderCheckout(t *testing.T, w *patchWorld, id string) Proposal {
	t.Helper()
	seedIfNeeded(t, w)
	p, err := w.broker.CreateProposalForPhase(context.Background(), id, ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Committing only the files the checkout materialized (the deployment patches
// applied) keeps nothing: the kept change is measured from the view, not from
// the base commit.
func TestCommittingTheMaterializedViewKeepsNothing(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	w.addPatch(t, "deployer", replaceLine("one", "ONE"))
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-view", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	commitInCheckout(t, p.Path, "commit the checkout as it is", "-A", "--", ".", ":(exclude).sdo")
	change, err := SourceChangeOf(ctx, p.Path, p.BaseCommit)
	if err != nil || len(change.Kept) != 0 || len(change.Scratch) != 0 {
		t.Fatalf("change = %+v, %v; the patched files must be neither kept nor scratch", change, err)
	}
}

// A partial commit keeps what the agent changed and leaves the patch-produced
// working files alone: they are not scratch.
func TestAPartialCommitKeepsOnlyTheAgentsFiles(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	w.addPatch(t, "deployer", replaceLine("one", "ONE"))
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-partial", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "agent edit\n", "notes.txt": "scratch\n"})
	commitInCheckout(t, p.Path, "edit app.txt", "app.txt")
	change, err := SourceChangeOf(ctx, p.Path, p.BaseCommit)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(change.Kept, ",") != "app.txt" || strings.Join(change.Scratch, ",") != "notes.txt" {
		t.Fatalf("kept %v, scratch %v; want app.txt kept and only notes.txt scratch", change.Kept, change.Scratch)
	}
	if strings.Join(change.Subjects, ",") != "edit app.txt" {
		t.Fatalf("subjects = %v", change.Subjects)
	}
}

// A commit to a file that a deployment patch also changes would apply the
// series a second time: it is refused.
func TestACommitToAPatchedFileIsRefused(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	w.addPatch(t, "deployer", replaceLine("one", "ONE"))
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-overlap", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	vendor, err := os.ReadFile(p.Path + "/lib/vendor.txt")
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"lib/vendor.txt": string(vendor) + "more\n"})
	commitInCheckout(t, p.Path, "extend the vendored file", "lib/vendor.txt")
	_, err = w.broker.Publish(ctx, "inc-overlap", Responder, ProposalPhase)
	if got := rulesOf(t, err); len(got) != 1 || got[0] != validate.RuleSourcePolicy {
		t.Fatalf("rules = %v (%v), want source.policy", got, err)
	}
}

// Rewriting history below the recorded base is source.history.
func TestAResetBelowTheBaseIsRefused(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	p := responderCheckout(t, w, "inc-history")
	testutil.ConfigureGit(t, p.Path)
	testutil.Git(t, p.Path, "checkout", "-q", "--orphan", "rewritten")
	testutil.Git(t, p.Path, "commit", "-qm", "unrelated", "--allow-empty")
	_, err := w.broker.Publish(ctx, "inc-history", Responder, ProposalPhase)
	if got := rulesOf(t, err); len(got) != 1 || got[0] != validate.RuleSourceHistory {
		t.Fatalf("rules = %v (%v), want source.history", got, err)
	}
}

// Secret data in a committed file is refused; an unrelated edit to a file
// that already holds placeholder Secret data is not.
func TestSecretDataIsRefusedPerAddedLine(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	writeFiles(t, w.accepted, map[string]string{"deploy/config.yaml": placeholderSecret})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "test setup: a manifest with placeholder Secret data")

	p, err := w.broker.CreateProposalForPhase(ctx, "inc-secret-ok", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"deploy/config.yaml": strings.Replace(placeholderSecret, "mode: fast", "mode: slow", 1)})
	commitInCheckout(t, p.Path, "slow mode", "deploy/config.yaml")
	if _, err := w.broker.StageCandidate(ctx, "inc-secret-ok", Responder, ProposalPhase); err != nil {
		t.Fatalf("an edit outside the Secret was refused: %v", err)
	}

	q, err := w.broker.CreateProposalForPhase(ctx, "inc-secret-bad", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, q.Path, map[string]string{"deploy/config.yaml": strings.Replace(placeholderSecret, "REPLACE_ME", "hunter2", 1)})
	commitInCheckout(t, q.Path, "set the password", "deploy/config.yaml")
	_, err = w.broker.StageCandidate(ctx, "inc-secret-bad", Responder, ProposalPhase)
	if got := rulesOf(t, err); len(got) != 1 || got[0] != validate.RuleSecretData {
		t.Fatalf("rules = %v (%v), want memory.secret-data", got, err)
	}
}

// The detection designer holds no source grant: a committed app file is denied.
func TestADesignerCannotPublishSource(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-designer", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "designer\n"})
	commitInCheckout(t, p.Path, "designer edit", "app.txt")
	_, err = w.broker.Publish(ctx, "inc-designer", HealthJudge, InitialPhase)
	if got := rulesOf(t, err); len(got) == 0 {
		t.Fatalf("a designer's source commit was accepted (%v)", err)
	}
}

// A commit that merely carries a broker trailer, made by anyone but the
// broker, never satisfies the idempotency lookup.
func TestAForgedTrailerDoesNotSatisfyTheAcceptedLookup(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	testutil.Git(t, w.accepted, "commit", "-q", "--allow-empty", "-m", "looks accepted\n\nSDO-Incident: inc-forged\nSDO-Phase: proposal")
	if got, found, err := w.broker.AcceptedCommit(ctx, "inc-forged", ProposalPhase); err != nil || found {
		t.Fatalf("a forged trailer was trusted: %+v %v %v", got, found, err)
	}
}
