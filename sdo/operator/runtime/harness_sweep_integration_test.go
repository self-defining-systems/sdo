//go:build integration

package runtime

import (
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/runtime/sim"
)

// harnessWallBudget caps the nightly sweep so it stays inside the project's
// five-minute limit; PR CI runs a fixed three-seed prefix and needs no cap.
const harnessWallBudget = 200 * time.Second

// TestHarness plays each seed through the production controller loop and checks
// that the controller is reliable (every incident ends) and manages its
// detectors well (each runs within a few times its own interval).
func TestHarness(t *testing.T) {
	deadline := time.Now().Add(harnessWallBudget)
	seeds := simSeeds(t, 64, simPRRuns)
	for _, seed := range seeds {
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			if len(seeds) > simPRRuns && time.Now().After(deadline) {
				t.Skip("wall budget spent")
			}
			synctest.Test(t, func(t *testing.T) {
				log, cfg := runHarness(t, seed)
				if testing.Verbose() {
					counts := map[string]int{}
					for _, e := range log {
						counts[fmt.Sprintf("%T", e)]++
					}
					t.Logf("log entries: %v", counts)
				}
				for _, err := range sim.Check(log, cfg) {
					t.Errorf("%v\nreplay: SDO_SIM_SEED=%d go test -tags=integration -run '^TestHarness$' ./runtime", err, seed)
				}
			})
		})
	}
}
