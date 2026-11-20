//go:build integration

package bench

import (
	"testing"
	"time"
)

// A lane's reset must outlive the longest the hooks' reset can take (a suspended controller resumed and given its
// start-up bound, then a second pass), so the manager never kills it between the hook stopping the controller and
// restarting it; and the verb's limit is stated, not the default hour.
func TestResetHookHasAStatedLimitAboveTheHooksWorstCase(t *testing.T) {
	got, ok := hookTimeouts["reset"]
	if !ok {
		t.Fatal("reset has no stated hook limit")
	}
	if got < 25*time.Minute || got > time.Hour {
		t.Fatalf("reset limit = %s, want a stated bound above the hooks' 25 minute worst case", got)
	}
}

// swap restores the golden copy up to twice after the reset a moved lane gets (three resets at worst): its limit is
// stated and above that.
func TestSwapHookHasAStatedLimitAboveThreeResets(t *testing.T) {
	got, ok := hookTimeouts["swap"]
	if !ok {
		t.Fatal("swap has no stated hook limit")
	}
	if got < 45*time.Minute || got > time.Hour {
		t.Fatalf("swap limit = %s, want a stated bound of at least 45 minutes", got)
	}
}

// snapshot may reset the lane after a late incident (copy, 5 minutes, the put-back of up to 35 minutes, 5 more): its
// limit is stated and above that worst case, not the default hour.
func TestSnapshotHookHasAStatedLimitAboveACopyAndAReset(t *testing.T) {
	got, ok := hookTimeouts["snapshot"]
	if !ok {
		t.Fatal("snapshot has no stated hook limit")
	}
	if got <= time.Hour || got > 2*time.Hour {
		t.Fatalf("snapshot limit = %s, want a stated bound above the default hour (a copy, a reset and a put-back) and at most 2 h", got)
	}
}
