//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/rendered"
)

func pushApp(t *testing.T, w *patchWorld, text string) string {
	t.Helper()
	writeFiles(t, w.accepted, map[string]string{"app.txt": text})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "an engineer changes the app")
	return w.head(t)
}

func targetOf(t *testing.T, b *Broker) Target {
	t.Helper()
	target, err := b.TryTarget(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return target
}

// A head that is written to the cluster becomes the target and leaves the
// accepted deployment alone: across a restart, until it is accepted.
func TestAHeldHeadIsTheTargetUntilItIsAccepted(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	initial := targetOf(t, w.broker)
	if initial.Unaccepted || initial.Commit == "" {
		t.Fatalf("without a hold the target is the accepted deployment, got %+v", initial)
	}
	head := pushApp(t, w, "one\ntwo\nthree by an engineer\n")
	evidence := advanceEvidenceFor(t, w.accepted, "engineered", true)
	if got := targetOf(t, w.broker); got != initial {
		t.Fatalf("a push alone moved the target to %+v", got)
	}
	write := rendered.TargetWrite{Phase: "main", Pins: rendered.ReceiptPins{EffectiveTree: evidence.EffectiveTree}}
	if err := w.broker.RecordTarget(ctx, head, write); err != nil {
		t.Fatal(err)
	}
	if got := targetOf(t, w.broker); got != (Target{Commit: head, Unaccepted: true, Incomplete: true, Interrupted: "main", RefusesAll: true}) {
		t.Fatalf("before the apply finished: %+v", got)
	}
	write.Applied = true
	if err := w.broker.RecordTarget(ctx, head, write); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(w.accepted, w.broker.workspaceRoot, w.broker.validator, testObservation())
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string]*Broker{"same": w.broker, "restarted": restarted} {
		// While another process holds the broker lock, the recorded target is
		// still the answer, with the error that makes the controller retry.
		if err := w.broker.locked(func() error {
			got, err := b.TryTarget(ctx)
			if err == nil || got != (Target{Commit: head, Unaccepted: true}) {
				t.Errorf("%s broker, locked: %+v, %v", name, got, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := targetOf(t, b); got != (Target{Commit: head, Unaccepted: true}) {
			t.Fatalf("%s broker: %+v", name, got)
		}
	}
	accepted, _, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil || accepted.Evidence.AcceptedCommit != initial.Commit {
		t.Fatalf("a hold moved the accepted deployment: %+v %v", accepted.Evidence.AcceptedCommit, err)
	}
	// Memory commits on the held head change neither.
	seedMemory(t, w)
	if got := targetOf(t, w.broker); got.Commit != head {
		t.Fatalf("a memory commit moved the target: %+v", got)
	}
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || !needed {
		t.Fatalf("a held head is still undeployed: %v %v", needed, err)
	}
	if err := w.broker.RecordHeadDeployment(ctx, head, evidence); err != nil {
		t.Fatal(err)
	}
	if got := targetOf(t, w.broker); got != (Target{Commit: head}) {
		t.Fatalf("accepted later: %+v", got)
	}
	if stored, err := w.broker.DeploymentTarget(); err != nil || stored != nil {
		t.Fatalf("an accepted head keeps a target record: %+v %v", stored, err)
	}
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("an accepted head is to deploy again: %v %v", needed, err)
	}
}

// An engineer who reverts a held head gets the accepted source back as a whole
// forward deployment, though its source equals the accepted deployment's.
func TestARevertOfAHeldHeadIsDeployed(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	initial := targetOf(t, w.broker)
	held := pushApp(t, w, "one\ntwo\nthree by an engineer\n")
	heldEvidence := advanceEvidenceFor(t, w.accepted, "engineered", true)
	if err := w.broker.RecordTarget(ctx, held, rendered.TargetWrite{Phase: "main", Applied: true, Pins: rendered.ReceiptPins{EffectiveTree: heldEvidence.EffectiveTree}}); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, w.accepted, "revert", "--no-edit", held)
	revert := w.head(t)
	change, needed, err := w.broker.HeadToDeploy(ctx)
	if err != nil || !needed || change.Commit != revert {
		t.Fatalf("HeadToDeploy = %+v %v %v, want the revert", change, needed, err)
	}
	evidence := advanceEvidenceFor(t, w.accepted, "engineered", true)
	write := rendered.TargetWrite{Phase: "main", Later: []string{"post"}, Applied: true, Pins: rendered.ReceiptPins{EffectiveTree: evidence.EffectiveTree}}
	if err := w.broker.RecordTarget(ctx, revert, write); err != nil {
		t.Fatal(err)
	}
	stored, err := w.broker.DeploymentTarget()
	if err != nil || stored.Commit != revert || stored.AcceptedCommit != initial.Commit || stored.Previous == nil || stored.Previous.Commit != held {
		t.Fatalf("target of the partial revert = %+v, %v; its unreached phase runs the held head", stored, err)
	}
	// Interrupted here, the revert is retried.
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || !needed {
		t.Fatalf("an interrupted revert is not retried: %v %v", needed, err)
	}
	if err := w.broker.RecordHeadDeployment(ctx, revert, evidence); err != nil {
		t.Fatal(err)
	}
	if got := targetOf(t, w.broker); got != (Target{Commit: revert}) {
		t.Fatalf("accepted revert: %+v", got)
	}
	if _, needed, err := w.broker.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("accepted revert is to deploy again: %v %v", needed, err)
	}
}

// A branch forced back to the accepted commit while a target is recorded is to
// deploy: the cluster runs another revision.
func TestTheAcceptedHeadIsToDeployWhileAnotherRevisionIsTheTarget(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	held := pushApp(t, w, "one\ntwo\nthree by an engineer\n")
	evidence := advanceEvidenceFor(t, w.accepted, "engineered", true)
	if err := w.broker.RecordTarget(ctx, held, rendered.TargetWrite{Phase: "main", Applied: true, Pins: rendered.ReceiptPins{EffectiveTree: evidence.EffectiveTree}}); err != nil {
		t.Fatal(err)
	}
	accepted, _, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, w.accepted, "reset", "-q", "--hard", accepted.Evidence.AcceptedCommit)
	if change, needed, err := w.broker.HeadToDeploy(ctx); err != nil || !needed || change.Commit != accepted.Evidence.AcceptedCommit {
		t.Fatalf("HeadToDeploy = %+v %v %v", change, needed, err)
	}
}

// An accepted repair supersedes a target: the record is dropped, so a read
// without the lock never serves it, and a record left by a crash is ignored
// and dropped at redelivery. A target recorded after the repair is kept.
func TestAnAcceptedRepairSupersedesTheTarget(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	tree := strings.Repeat("a", 40)
	record := func(commit string) {
		t.Helper()
		if err := w.broker.RecordTarget(ctx, commit, rendered.TargetWrite{Phase: "main", Applied: true, Pins: rendered.ReceiptPins{EffectiveTree: tree}}); err != nil {
			t.Fatal(err)
		}
	}
	candidate := stageResponderPatch(t, w, "inc-super")
	record(strings.Repeat("b", 40))
	path := filepath.Join(w.broker.workspaceRoot, targetFile)
	stale, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := w.broker.AcceptDeployed(ctx, candidate, advanceEvidenceFor(t, candidate.Path, "repaired", true))
	if err != nil {
		t.Fatal(err)
	}
	if stored, err := w.broker.DeploymentTarget(); err != nil || stored != nil {
		t.Fatalf("superseded target still recorded: %+v %v", stored, err)
	}
	if err := os.WriteFile(path, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := targetOf(t, w.broker); got != (Target{Commit: receipt.Commit}) {
		t.Fatalf("a superseded record is the target: %+v", got)
	}
	redeliver := func() {
		t.Helper()
		if deployed, err := w.broker.FinalizeAcceptedDeployment(ctx, "inc-super"); err != nil || !deployed {
			t.Fatalf("redelivery: %v %v", deployed, err)
		}
	}
	redeliver()
	if stored, err := w.broker.DeploymentTarget(); err != nil || stored != nil {
		t.Fatalf("redelivery kept the superseded record: %+v %v", stored, err)
	}
	newer := strings.Repeat("c", 40)
	record(newer)
	redeliver()
	if got := targetOf(t, w.broker); got.Commit != newer || !got.Unaccepted {
		t.Fatalf("redelivery dropped a newer target: %+v", got)
	}
}
