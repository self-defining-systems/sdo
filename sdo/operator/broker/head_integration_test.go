//go:build integration

package broker

import (
	"context"
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

// A commit pushed to the branch SDO syncs with (an engineer's, or the benchmark
// replayer's) is fast-forwarded into the accepted repository and becomes the
// head to deploy: the replay needs nothing from SDO but the branch.
func TestACommitPushedToTheSourceBranchBecomesTheHeadToDeploy(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	remote := t.TempDir() + "/remote.git"
	testutil.Git(t, "", "init", "-q", "--bare", remote)
	testutil.Git(t, w.accepted, "push", "-q", remote, "HEAD:refs/heads/sdo-source")
	if err := w.broker.SetUpstream(Upstream{URL: remote, Branch: "sdo-source"}); err != nil {
		t.Fatal(err)
	}
	engineer := t.TempDir() + "/engineer"
	testutil.Git(t, "", "clone", "-q", "--branch", "sdo-source", remote, engineer)
	testutil.ConfigureGit(t, engineer)
	writeFiles(t, engineer, map[string]string{"app.txt": "one\ntwo\nthree replayed\n"})
	testutil.Git(t, engineer, "add", ".")
	testutil.Git(t, engineer, "-c", "user.name=bench-replayer", "-c", "user.email=bench-replayer@localhost", "commit", "-qm", "replayed\n\nReplayed-From: c1")
	testutil.Git(t, engineer, "push", "-q", "origin", "HEAD:refs/heads/sdo-source")
	pushed := gitOut(t, engineer, "rev-parse", "HEAD")

	change, needed, err := w.broker.HeadToDeploy(ctx)
	if err != nil || !needed || change.Commit != pushed {
		t.Fatalf("HeadToDeploy = %+v %v %v, want the pushed commit %s", change, needed, err, pushed)
	}
	if got := w.head(t); got != pushed {
		t.Fatalf("the accepted head is %s: the sync did not fast-forward onto the pushed commit %s", got, pushed)
	}
}

// A local history and a branch that both moved are never merged: the local
// history stays accepted, it is preserved on a side branch, and the source
// branch is left as the engineer pushed it.
func TestADivergedBranchIsHeldNotMerged(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	remote := t.TempDir() + "/remote.git"
	testutil.Git(t, "", "init", "-q", "--bare", remote)
	testutil.Git(t, w.accepted, "push", "-q", remote, "HEAD:refs/heads/sdo-source")
	if err := w.broker.SetUpstream(Upstream{URL: remote, Branch: "sdo-source"}); err != nil {
		t.Fatal(err)
	}
	// A local accepted commit that never reached the remote.
	writeFiles(t, w.accepted, map[string]string{"local.txt": "local\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "local history")
	local := w.head(t)
	// An engineer pushes meanwhile.
	engineer := t.TempDir() + "/engineer"
	testutil.Git(t, "", "clone", "-q", "--branch", "sdo-source", remote, engineer)
	testutil.ConfigureGit(t, engineer)
	writeFiles(t, engineer, map[string]string{"engineer.txt": "engineer\n"})
	testutil.Git(t, engineer, "add", ".")
	testutil.Git(t, engineer, "commit", "-qm", "engineer history")
	testutil.Git(t, engineer, "push", "-q", "origin", "HEAD:refs/heads/sdo-source")
	pushed := gitOut(t, engineer, "rev-parse", "HEAD")

	if _, _, err := w.broker.HeadToDeploy(ctx); err != nil {
		t.Fatal(err)
	}
	if w.head(t) != local {
		t.Fatal("diverged histories were merged into the accepted head")
	}
	if got := gitOut(t, remote, "rev-parse", "refs/heads/sdo-source"); got != pushed {
		t.Fatal("the source branch was touched")
	}
	if got := gitOut(t, remote, "rev-parse", "refs/heads/sdo-held/diverged-"+local[:12]); got != local {
		t.Fatalf("the side branch holds %s, want the local history %s", got, local)
	}
}
