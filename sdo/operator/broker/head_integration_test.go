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
