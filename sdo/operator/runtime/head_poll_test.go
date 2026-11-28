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
	refused := broker.HeadChange{Commit: "a1", EffectiveTree: "t1"}
	if _, held, err := holds.Held(ctx, refused); err != nil || held {
		t.Fatalf("nothing is held yet: held=%v err=%v", held, err)
	}
	if err := holds.Hold(ctx, refused, lifecycle.HoldRollout, "never became ready"); err != nil {
		t.Fatal(err)
	}
	restarted := ConfigMapHeadHolds{Client: client, Namespace: "sdo-system"}
	later := broker.HeadChange{Commit: "b2", EffectiveTree: "t1"}
	if detail, held, err := restarted.Held(ctx, later); err != nil || !held || detail == "" {
		t.Fatalf("the same source on a later commit must stay held: %q %v %v", detail, held, err)
	}
	fixed := broker.HeadChange{Commit: "c3", EffectiveTree: "t2"}
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

// The gate reports the controller's own reason first, then every added check
// (a pending onboarding), and is never busy when it has none.
func TestSourceGateReadsTheControllerThenTheAddedChecks(t *testing.T) {
	gate := &SourceGate{}
	if gate.Busy() != "" {
		t.Fatal("a gate with no checks is never busy")
	}
	gate.Add(func() string { return "namespace onboarding is pending" })
	if got := gate.Busy(); got != "namespace onboarding is pending" {
		t.Fatalf("busy = %q", got)
	}
	gate.Set(func() string { return "an incident is open" })
	if got := gate.Busy(); got != "an incident is open" {
		t.Fatalf("the controller's reason comes first: %q", got)
	}
}

// With no onboarding staged the check says nothing.
func TestAbsentOnboardingIsNotBusy(t *testing.T) {
	if reason := onboardingPending(fake.NewSimpleClientset(), "sdo-system", "app")(); reason != "" {
		t.Fatalf("reason = %q", reason)
	}
}

// The notice of a deployed head is a ConfigMap beside the hold: the commit, its effective source and the verdict,
// replaced by the next deployment; it clears a hold on the same source (the refusal is over) and keeps one on another.
func TestADeployedHeadLeavesANoticeAndEndsItsHold(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	holds := ConfigMapHeadHolds{Client: client, Namespace: "sdo-system", Now: func() time.Time { return time.Unix(1, 0) }}
	change := broker.HeadChange{Commit: "a1", EffectiveTree: "t1"}
	if err := holds.Hold(ctx, change, lifecycle.HoldHealth, "pending"); err != nil {
		t.Fatal(err)
	}
	if err := holds.Deployed(ctx, change, lifecycle.HeadResult{Outcome: lifecycle.HeadDeployed, Commit: "a1"}); err != nil {
		t.Fatal(err)
	}
	notice, err := holds.Deployment(ctx)
	if err != nil || notice == nil || notice.Commit != "a1" || notice.EffectiveTree != "t1" || notice.At == "" {
		t.Fatalf("notice = %+v, err = %v", notice, err)
	}
	if _, held, _ := holds.Held(ctx, change); held {
		t.Fatal("the deployment ended the hold on the same source")
	}
	next := broker.HeadChange{Commit: "b2", EffectiveTree: "t2"}
	if err := holds.Hold(ctx, next, lifecycle.HoldRollout, "stuck"); err != nil {
		t.Fatal(err)
	}
	if err := holds.Deployed(ctx, change, lifecycle.HeadResult{Outcome: lifecycle.HeadDeployed, Commit: "a1"}); err != nil {
		t.Fatal(err)
	}
	if _, held, _ := holds.Held(ctx, next); !held {
		t.Fatal("a deployment of another source keeps the hold on this one")
	}
}
