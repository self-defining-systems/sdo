//go:build integration

package broker

import (
	"context"
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
	if got := targetOf(t, w.broker); got != (Target{Commit: head, Unaccepted: true, Incomplete: true}) {
		t.Fatalf("before the apply finished: %+v", got)
	}
	write.Applied = true
	if err := w.broker.RecordTarget(ctx, head, write); err != nil {
		t.Fatal(err)
	}
	restarted := *w.broker
	for name, b := range map[string]*Broker{"same": w.broker, "restarted": &restarted} {
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
