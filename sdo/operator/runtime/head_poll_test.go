package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
)

// A hold is about the head's effective source: another commit with the same
// source is still held, a changed source is not, and the hold survives a new
// reader (a controller restart).
func TestHeadHoldIsKeyedByEffectiveSource(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	holds := ConfigMapHeadHolds{Client: client, Namespace: "sdo-system", Now: func() time.Time { return time.Unix(1, 0) }}
	refused := broker.HeadChange{Commit: "a1", EffectiveTree: "t1", PatchesDigest: "d1"}
	if _, held, err := holds.Held(ctx, refused); err != nil || held {
		t.Fatalf("nothing is held yet: held=%v err=%v", held, err)
	}
	if err := holds.Hold(ctx, refused, lifecycle.HoldRollout, "never became ready"); err != nil {
		t.Fatal(err)
	}
	restarted := ConfigMapHeadHolds{Client: client, Namespace: "sdo-system"}
	later := broker.HeadChange{Commit: "b2", EffectiveTree: "t1", PatchesDigest: "d1"}
	if detail, held, err := restarted.Held(ctx, later); err != nil || !held || detail == "" {
		t.Fatalf("the same source on a later commit must stay held: %q %v %v", detail, held, err)
	}
	fixed := broker.HeadChange{Commit: "c3", EffectiveTree: "t2", PatchesDigest: "d1"}
	if _, held, err := restarted.Held(ctx, fixed); err != nil || held {
		t.Fatalf("a changed source supersedes the hold: held=%v err=%v", held, err)
	}
	// A second hold replaces the first.
	if err := restarted.Hold(ctx, fixed, lifecycle.HoldHealth, "detectors fire"); err != nil {
		t.Fatal(err)
	}
	if _, held, _ := restarted.Held(ctx, refused); held {
		t.Fatal("the earlier hold was not replaced")
	}
}
