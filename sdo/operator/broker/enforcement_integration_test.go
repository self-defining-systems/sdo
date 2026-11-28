//go:build integration

package broker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/validate"
)

// seedMemory completes the minimal memory the shared check requires, as a plain
// test-setup commit on the accepted repository.
func seedMemory(t *testing.T, w *patchWorld) {
	t.Helper()
	writeFiles(t, w.accepted, map[string]string{
		".sdo/schema-version": "15\n",
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

// The candidate tree is the committed tree with the working .sdo/ in place:
// scratch app edits never move it, a local commit of app files does.
func TestProposalTreeDigestIsTheCommitWithWorkingMemory(t *testing.T) {
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
	testutil.Git(t, p.Path, "add", "app.txt")
	testutil.ConfigureGit(t, p.Path)
	testutil.Git(t, p.Path, "commit", "-qm", "local")
	committed, err := ProposalTreeDigest(ctx, p.Path)
	if err != nil || committed == clean {
		t.Fatalf("a local app commit did not move the candidate tree: %s, %v", committed, err)
	}
	writeFiles(t, p.Path, map[string]string{".sdo/deployment/plan.yaml": "images: []\n"})
	if got, err := ProposalTreeDigest(ctx, p.Path); err != nil || got == committed {
		t.Fatalf("a working memory edit did not move the candidate tree: %s, %v", got, err)
	}
}

// The responder keeps source by committing it. An uncommitted app-file edit is
// scratch: the cleanup message names it, and a publication carries
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
	scratch, err := w.broker.UncapturedPaths(ctx, "inc-resp", ProposalPhase)
	if err != nil || strings.Join(scratch, ",") != "app.txt,probe.sh" {
		t.Fatalf("UncapturedPaths = %v, %v", scratch, err)
	}
	head := w.head(t)
	receipt, err := w.broker.Publish(ctx, "inc-resp", Responder, ProposalPhase)
	if err != nil || receipt.Commit != "" || w.head(t) != head {
		t.Fatalf("publishing scratch = %+v, %v; the head moved: %v", receipt, err, w.head(t) != head)
	}

	testutil.ConfigureGit(t, p.Path)
	testutil.Git(t, p.Path, "add", "app.txt")
	testutil.Git(t, p.Path, "commit", "-qm", "fix the off-by-one in app.txt")
	if scratch, err := w.broker.UncapturedPaths(ctx, "inc-resp", ProposalPhase); err != nil || strings.Join(scratch, ",") != "probe.sh" {
		t.Fatalf("UncapturedPaths after the commit = %v, %v", scratch, err)
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

// The deployer commits app files: a verified-source publication carries what
// it committed and never its scratch, bound to the deployed tree.
func TestDeployerPublishesCommittedSourceBoundToTheDeployedTree(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-app", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree deployed\n", "notes.txt": "scratch\n"})
	commitInCheckout(t, p.Path, "deploy app.txt", "app.txt")
	head := w.head(t)
	evidence := advanceEvidence(t, p.Path, "app")
	receipt, err := w.broker.PublishVerifiedSource(ctx, "inc-app", evidence, deploymentReportFor(t, evidence, p.BaseCommit))
	if err != nil {
		t.Fatalf("a deployer publication of committed source: %v", err)
	}
	changed := strings.Fields(gitOut(t, w.accepted, "diff", "--name-only", head, receipt.Commit))
	if strings.Join(changed, ",") != "app.txt" {
		t.Fatalf("accepted commit changed %v, want only the committed app.txt", changed)
	}
	if tree := gitOut(t, w.accepted, "rev-parse", receipt.Commit+"^{tree}"); tree != evidence.BaseTree {
		t.Fatalf("accepted tree %s, deployed tree %s", tree, evidence.BaseTree)
	}
}

// An uncommitted app edit is never part of a deployer's publication: evidence
// that names it (a tree with the scratch file) is refused.
func TestDeployerScratchIsNeverPublished(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-scratch", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree deployed\n"})
	evidence := advanceEvidence(t, p.Path, "app")
	head := w.head(t)
	receipt, err := w.broker.PublishVerifiedSource(ctx, "inc-scratch", evidence, deploymentReportFor(t, evidence, p.BaseCommit))
	if err != nil {
		t.Fatal(err)
	}
	if changed := strings.TrimSpace(gitOut(t, w.accepted, "diff", "--name-only", head, receipt.Commit)); changed != "" {
		t.Fatalf("scratch reached the accepted tree: %q", changed)
	}
}
