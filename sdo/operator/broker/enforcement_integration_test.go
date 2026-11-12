//go:build integration

package broker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/patchset"
)

// seedMemory completes the minimal memory the shared check requires, as a plain
// test-setup commit on the accepted repository.
func seedMemory(t *testing.T, w *patchWorld) {
	t.Helper()
	writeFiles(t, w.accepted, map[string]string{
		".sdo/schema-version": "8\n",
		".sdo/arch.md":        "---\nschema_version: 1\ngenerated_at_commit: base\napplication: demo\n---\nsynthetic\n",
	})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "test setup: memory")
}

func rulesOf(t *testing.T, err error) []string {
	t.Helper()
	var failure *validate.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("error %v is not a validation failure", err)
	}
	var rules []string
	for _, d := range failure.Diagnostics {
		rules = append(rules, d.Rule)
	}
	return rules
}

// Agents write only .sdo/: the candidate tree ignores scratch app edits and
// sees a captured edit only as its patch file.
func TestProposalTreeDigestCoversOnlyTheMemoryDirectory(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-tree", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	clean, err := ProposalTreeDigest(ctx, p.Path)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "scratch\n"})
	if got, err := ProposalTreeDigest(ctx, p.Path); err != nil || got != clean {
		t.Fatalf("scratch edit moved the candidate tree: %s vs %s, %v", got, clean, err)
	}
	// A local commit of app files does not move the base either.
	testutil.Git(t, p.Path, "add", "app.txt")
	testutil.ConfigureGit(t, p.Path)
	testutil.Git(t, p.Path, "commit", "-qm", "local")
	if got, err := ProposalTreeDigest(ctx, p.Path); err != nil || got != clean {
		t.Fatalf("a local app commit moved the candidate tree: %s vs %s, %v", got, clean, err)
	}
	if _, err := patchset.Create(ctx, p.Path, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: "inc-tree", Reason: patchReason}); err != nil {
		t.Fatal(err)
	}
	if got, err := ProposalTreeDigest(ctx, p.Path); err != nil || got == clean {
		t.Fatalf("a patch file did not move the candidate tree: %s, %v", got, err)
	}
}

// The responder keeps source by committing it. An uncommitted app-file edit is
// scratch: ScratchFeedback asks for a decision, and a publication carries
// nothing and moves nothing. The committed file is published (as app source,
// not as a patch) with the agent's subject in the message and the broker's
// trailers.
func TestResponderPublishesCommittedSourceAndNeverScratch(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedMemory(t, w)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-resp", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree fixed\n", "probe.sh": "echo debug\n"})
	feedback, err := w.broker.ScratchFeedback(ctx, "inc-resp", ProposalPhase)
	if err != nil || !strings.Contains(feedback, "app.txt") || !strings.Contains(feedback, "probe.sh") || strings.Contains(feedback, UncapturedRule) {
		t.Fatalf("ScratchFeedback = %q, %v", feedback, err)
	}
	head := w.head(t)
	receipt, err := w.broker.Publish(ctx, "inc-resp", Responder, ProposalPhase)
	if err != nil || receipt.Commit != "" || w.head(t) != head {
		t.Fatalf("publishing scratch = %+v, %v; the head moved: %v", receipt, err, w.head(t) != head)
	}

	testutil.ConfigureGit(t, p.Path)
	testutil.Git(t, p.Path, "add", "app.txt")
	testutil.Git(t, p.Path, "commit", "-qm", "fix the off-by-one in app.txt")
	if feedback, err := w.broker.ScratchFeedback(ctx, "inc-resp", ProposalPhase); err != nil || strings.Contains(feedback, "app.txt") || !strings.Contains(feedback, "probe.sh") {
		t.Fatalf("ScratchFeedback after the commit = %q, %v", feedback, err)
	}
	receipt, err = w.broker.Publish(ctx, "inc-resp", Responder, ProposalPhase)
	if err != nil || receipt.Commit == "" {
		t.Fatalf("publish of committed source: %+v, %v", receipt, err)
	}
	changed := strings.Fields(gitOut(t, w.accepted, "diff", "--name-only", head, receipt.Commit))
	if len(changed) != 1 || changed[0] != "app.txt" {
		t.Fatalf("accepted commit changed %v, want only the committed app.txt", changed)
	}
	if got := gitOut(t, w.accepted, "show", receipt.Commit+":app.txt"); !strings.Contains(got, "three fixed") {
		t.Fatalf("the accepted app file is %q", got)
	}
	message := gitOut(t, w.accepted, "log", "-1", "--format=%B", receipt.Commit)
	for _, want := range []string{"- fix the off-by-one in app.txt", "SDO-Incident: inc-resp", "SDO-Actor: responder", "SDO-Phase: proposal"} {
		if !strings.Contains(message, want) {
			t.Fatalf("commit message lacks %q:\n%s", want, message)
		}
	}
}

// A hand-made proposal diff touching an app file is denied for every agent
// actor by the shared ownership policy, whatever the publish path filters.
func TestPublishNeverCarriesAppFiles(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-app", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree deployed\n"})
	_, err = w.broker.PublishVerifiedSource(ctx, "inc-app", advanceEvidence(t, p.Path, "app"), deploymentReportFor(t, advanceEvidence(t, p.Path, "app"), p.BaseCommit))
	if err == nil {
		t.Fatal("a deployer publication with an uncaptured app edit was accepted")
	}
	if got := rulesOf(t, err); len(got) != 1 || got[0] != validate.RulePatchUncaptured {
		t.Fatalf("rules = %v", got)
	}
}
