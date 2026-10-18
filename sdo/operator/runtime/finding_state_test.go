package runtime

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

var stateNow = time.Date(2026, 9, 28, 9, 0, 0, 0, time.UTC)

func TestFindingStateRequiresTwoFiringsAndTwoClears(t *testing.T) {
	tracker := NewFindingStateTracker(2, 2)
	finding := stateFinding("fault-a")

	if change := tracker.Observe(stateNow, "detector-a", []sdk.Finding{finding}); len(change.Activated) != 0 {
		t.Fatalf("first sample should be transient: %#v", change)
	}
	if change := tracker.Observe(stateNow, "detector-a", []sdk.Finding{finding, finding}); len(change.Activated) != 1 {
		t.Fatalf("second deduplicated sample should activate once: %#v", change)
	}
	if change := tracker.Observe(stateNow, "detector-a", nil); len(change.Cleared) != 0 {
		t.Fatalf("first clear should not resolve: %#v", change)
	}
	if change := tracker.Observe(stateNow, "detector-a", nil); !equalStrings(change.Cleared, []string{FindingStateKey("detector-a", "fault-a")}) {
		t.Fatalf("second clear should resolve: %#v", change)
	}
}

func TestFindingStateSuppressesTransientFiring(t *testing.T) {
	tracker := NewFindingStateTracker(2, 2)
	tracker.Observe(stateNow, "detector-a", []sdk.Finding{stateFinding("transient")})
	tracker.Observe(stateNow, "detector-a", nil)

	if change := tracker.Observe(stateNow, "detector-a", []sdk.Finding{stateFinding("transient")}); len(change.Activated) != 0 {
		t.Fatalf("nonconsecutive firing activated: %#v", change)
	}
}

func TestFindingStateUsesPerDetectorPersistence(t *testing.T) {
	tracker := NewFindingStateTracker(9, 9)
	tracker.SetPolicy("fast", 1, 1, 0)
	tracker.SetPolicy("slow", 3, 2, 0)

	if changes := tracker.Observe(stateNow, "fast", []sdk.Finding{stateFinding("fast-fault")}); len(changes.Activated) != 1 {
		t.Fatalf("fast detector did not use its firing threshold: %#v", changes)
	}
	if changes := tracker.Observe(stateNow, "fast", nil); len(changes.Cleared) != 1 {
		t.Fatalf("fast detector did not use its clear threshold: %#v", changes)
	}
	for sample := 1; sample < 3; sample++ {
		if changes := tracker.Observe(stateNow, "slow", []sdk.Finding{stateFinding("slow-fault")}); len(changes.Activated) != 0 {
			t.Fatalf("slow detector activated on sample %d: %#v", sample, changes)
		}
	}
	if changes := tracker.Observe(stateNow, "slow", []sdk.Finding{stateFinding("slow-fault")}); len(changes.Activated) != 1 {
		t.Fatalf("slow detector did not activate on its third sample: %#v", changes)
	}
}

// TestFindingStateMinDurationSuppressesBriefStall mirrors a kind-worker
// data-plane stall: traffic-health reaches its firing evaluation count (2)
// within about 0.5s because the traffic window is re-evaluated on every
// 500ms poll, but the real stall clears within about 3s. A minimum-duration
// policy must keep the finding pending for the whole burst.
func TestFindingStateMinDurationSuppressesBriefStall(t *testing.T) {
	tracker := NewFindingStateTracker(9, 9)
	tracker.SetPolicy("traffic-health", 2, 2, 9*time.Second)
	finding := stateFinding("traffic-stall")

	for _, offset := range []time.Duration{0, 500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second, 2500 * time.Millisecond, 3 * time.Second} {
		now := stateNow.Add(offset)
		if changes := tracker.Observe(now, "traffic-health", []sdk.Finding{finding}); len(changes.Activated) != 0 {
			t.Fatalf("3s stall activated at offset %s: %#v", offset, changes)
		}
	}
	if !tracker.HasPendingDetector("traffic-health") {
		t.Fatal("stall should still be pending, not cleared or forgotten")
	}

	// The prober recovers; the finding stops being observed and never fires.
	if changes := tracker.Observe(stateNow.Add(3500*time.Millisecond), "traffic-health", nil); len(changes.Activated) != 0 {
		t.Fatalf("recovery must not activate: %#v", changes)
	}
}

// TestFindingStateMinDurationFiresSustainedFault mirrors a real fault: the
// same evaluation-count/poll cadence as the brief stall, but the condition
// never clears. It must still fire, within the min duration plus about one
// more poll.
func TestFindingStateMinDurationFiresSustainedFault(t *testing.T) {
	tracker := NewFindingStateTracker(9, 9)
	tracker.SetPolicy("traffic-health", 2, 2, 9*time.Second)
	finding := stateFinding("traffic-fault")

	const poll = 500 * time.Millisecond
	activatedAt := time.Duration(-1)
	for i := 0; i < 25; i++ {
		offset := time.Duration(i) * poll
		now := stateNow.Add(offset)
		changes := tracker.Observe(now, "traffic-health", []sdk.Finding{finding})
		if len(changes.Activated) > 0 {
			activatedAt = offset
			break
		}
	}
	if activatedAt < 0 {
		t.Fatal("sustained fault never activated")
	}
	if activatedAt < 9*time.Second {
		t.Fatalf("activated before the 9s minimum duration elapsed: %s", activatedAt)
	}
	if activatedAt > 9*time.Second+poll {
		t.Fatalf("activated more than one poll after the minimum duration: %s", activatedAt)
	}
}

func stateFinding(fingerprint string) sdk.Finding {
	return sdk.Finding{
		DetectorID: fingerprint,
		RuleID:     fingerprint, Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "fault", Evidence: "evidence",
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: fingerprint},
		Fingerprint:     fingerprint,
	}
}
