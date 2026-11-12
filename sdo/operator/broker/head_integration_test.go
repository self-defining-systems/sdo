//go:build integration

package broker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

// An engineer's commit that changes source is a head to deploy; recording its
// deployment makes it the newest deployment without a trailer on the commit,
// and a memory-only commit on top of it needs nothing more.
func TestAForeignHeadIsDeployedAndRecordedWithoutATrailer(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("a head that was deployed needs nothing: needed=%v err=%v", needed, err)
	}
	writeFiles(t, w.accepted, map[string]string{"app.txt": "one\ntwo\nthree by an engineer\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "an engineer changes the app")
	head := w.head(t)

	change, needed, err := w.broker.HeadToDeploy(ctx)
	if err != nil || !needed || change.Commit != head {
		t.Fatalf("HeadToDeploy = %+v %v %v, want %s", change, needed, err, head)
	}
	evidence := advanceEvidenceFor(t, w.accepted, "engineered", true)
	if err := w.broker.RecordHeadDeployment(ctx, head, evidence); err != nil {
		t.Fatalf("RecordHeadDeployment: %v", err)
	}
	if message := gitOut(t, w.accepted, "log", "-1", "--format=%B", head); strings.Contains(message, "SDO-") {
		t.Fatalf("SDO added a trailer to someone else's commit: %q", message)
	}
	accepted, found, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil || !found || accepted.Evidence.AcceptedCommit != head || accepted.Evidence.EffectiveTree != evidence.EffectiveTree {
		t.Fatalf("accepted evidence = %+v found=%v err=%v, want the head's deployment", accepted.Evidence, found, err)
	}
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("a deployed head needs nothing: needed=%v err=%v", needed, err)
	}
	// A broker memory commit on top changes no source: still nothing to deploy.
	seedMemory(t, w)
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("a memory-only head needs nothing: needed=%v err=%v", needed, err)
	}
}

// Evidence of another tree is never recorded for a commit.
func TestHeadDeploymentEvidenceMustBindToTheCommit(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	before := w.head(t)
	writeFiles(t, w.accepted, map[string]string{"app.txt": "changed\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "an engineer changes the app")
	stale := advanceEvidenceFor(t, w.accepted, "engineered", true)
	writeFiles(t, w.accepted, map[string]string{"app.txt": "changed again\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "and again")
	if err := w.broker.RecordHeadDeployment(ctx, w.head(t), stale); err == nil {
		t.Fatal("evidence of the previous tree was recorded for the new head")
	}
	_ = before
}

// A responder's committed source that conflicts with a commit pushed to the
// branch is a typed conflict, nothing is accepted, and HoldSource keeps the
// agent's commits on the remote side branch sdo-held/<incident>.
func TestAConflictingSourceChangeIsKeptOnTheSideBranch(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	seedIfNeeded(t, w)
	remote := t.TempDir() + "/remote.git"
	testutil.Git(t, "", "init", "-q", "--bare", remote)
	testutil.Git(t, w.accepted, "push", "-q", remote, "HEAD:refs/heads/sdo-source")
	if err := w.broker.SetUpstream(Upstream{URL: remote, Branch: "sdo-source"}); err != nil {
		t.Fatal(err)
	}
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-conflict", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree by SDO\n"})
	commitInCheckout(t, p.Path, "SDO changes the third line", "app.txt")

	engineer := t.TempDir() + "/engineer"
	testutil.Git(t, "", "clone", "-q", "--branch", "sdo-source", remote, engineer)
	testutil.ConfigureGit(t, engineer)
	writeFiles(t, engineer, map[string]string{"app.txt": "one\ntwo\nthree by an engineer\n"})
	testutil.Git(t, engineer, "add", ".")
	testutil.Git(t, engineer, "commit", "-qm", "an engineer changes the same line")
	testutil.Git(t, engineer, "push", "-q", "origin", "HEAD:refs/heads/sdo-source")

	_, err = w.broker.StageCandidate(ctx, "inc-conflict", Responder, ProposalPhase)
	var conflict *MemoryConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("StageCandidate = %v, want a MemoryConflict", err)
	}
	branch, err := w.broker.HoldSource(ctx, "inc-conflict")
	if err != nil || branch != "sdo-held/inc-conflict" {
		t.Fatalf("HoldSource = %q, %v", branch, err)
	}
	held := gitOut(t, remote, "rev-parse", "refs/heads/sdo-held/inc-conflict")
	if got := gitOut(t, remote, "show", held+":app.txt"); !strings.Contains(got, "three by SDO") {
		t.Fatalf("the side branch holds %q, want the agent's change", got)
	}
	if got := gitOut(t, remote, "show", "refs/heads/sdo-source:app.txt"); !strings.Contains(got, "three by an engineer") {
		t.Fatal("the source branch was touched")
	}
}
