//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

// brokerMemoryCommit commits a memory-only change the way the broker publishes
// one (its committer, an SDO-Actor trailer), like a health refresh.
func brokerMemoryCommit(t *testing.T, w *patchWorld, name string) string {
	t.Helper()
	writeFiles(t, w.accepted, map[string]string{".sdo/" + name: "synthetic\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "-c", "user.name=SDO Commit Broker", "-c", "user.email="+brokerCommitterEmail,
		"commit", "-qm", "sdo(test): "+name+"\n\nSDO-Actor: health_judge\nSDO-Phase: initial")
	return w.head(t)
}

func seededWorld(t *testing.T) *patchWorld {
	t.Helper()
	w := newPatchWorld(t, false)
	if err := os.RemoveAll(filepath.Join(w.broker.workspaceRoot, "source-evidence")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := w.broker.AcceptedSourceEvidence(context.Background()); err != nil || found {
		t.Fatalf("the world must be seeded (no recorded deployment): found=%v err=%v", found, err)
	}
	return w
}

// A seeded install's reference is the source it adopted, not SDO's newest
// commit: a broker memory commit published on top of an engineer's push before
// it was deployed (a health refresh racing the push) carries the push's source
// and must not make it look deployed (GitLab commit series on EKS: 10.5.5 was
// pushed, a health refresh landed on it 33 s later, and SDO never deployed it).
// Once deployed and recorded, that head needs nothing more.
func TestASeededInstallDeploysAPushEvenUnderSDOsOwnLaterCommit(t *testing.T) {
	ctx := context.Background()
	w := seededWorld(t)
	brokerMemoryCommit(t, w, "before-the-push")
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("a seeded install's own head needs nothing: needed=%v err=%v", needed, err)
	}
	writeFiles(t, w.accepted, map[string]string{"app.txt": "one\ntwo\nthree by an engineer\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "an engineer changes the app")
	head := brokerMemoryCommit(t, w, "health-refresh")

	change, needed, err := w.broker.HeadToDeploy(ctx)
	if err != nil || !needed || change.Commit != head {
		t.Fatalf("HeadToDeploy = %+v %v %v, want the head %s that carries the undeployed push", change, needed, err, head)
	}
	evidence := advanceEvidenceFor(t, w.accepted, "engineered", true)
	if err := w.broker.RecordHeadDeployment(ctx, head, evidence); err != nil {
		t.Fatalf("RecordHeadDeployment: %v", err)
	}
	accepted, found, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil || !found || accepted.Evidence.AcceptedCommit != head {
		t.Fatalf("accepted evidence = %+v found=%v err=%v, want the deployment of %s", accepted.Evidence, found, err, head)
	}
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("a deployed head of SDO's own commit needs nothing (else it redeploys every poll): needed=%v err=%v", needed, err)
	}
}

// The adopted reference survives a controller restart (a new broker on the
// same workspace) that happens after the push and SDO's commit on it.
func TestASeededInstallsReferenceSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	w := seededWorld(t)
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("a seeded install's own head needs nothing: needed=%v err=%v", needed, err)
	}
	writeFiles(t, w.accepted, map[string]string{"app.txt": "one\ntwo\nthree by an engineer\n"})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "an engineer changes the app")
	head := brokerMemoryCommit(t, w, "health-refresh")
	restarted, err := New(w.accepted, w.broker.workspaceRoot, w.broker.validator, testObservation())
	if err != nil {
		t.Fatal(err)
	}
	change, needed, err := restarted.HeadToDeploy(ctx)
	if err != nil || !needed || change.Commit != head {
		t.Fatalf("after a restart HeadToDeploy = %+v %v %v, want %s", change, needed, err, head)
	}
}

// With no engineer push, SDO's own commits keep moving the reference exactly as
// before, so a seeded install never deploys its own memory commits.
func TestASeededInstallNeverDeploysItsOwnCommits(t *testing.T) {
	ctx := context.Background()
	w := seededWorld(t)
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("needed=%v err=%v", needed, err)
	}
	brokerMemoryCommit(t, w, "one")
	brokerMemoryCommit(t, w, "two")
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("SDO's own memory commits need no deployment: needed=%v err=%v", needed, err)
	}
}
