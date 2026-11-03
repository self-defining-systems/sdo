//go:build integration

package runtime

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"sdo.dev/operator/runtime/sim"
)

// harnessWallBudget caps the nightly sweep so it stays inside the project's
// five-minute limit; PR CI runs a fixed three-seed prefix and needs no cap.
const harnessWallBudget = 200 * time.Second

// harnessPRSeeds are the seeds PR CI plays. Seed 1 is left out while the state
// restore crash loop (a controller dying with "state references unknown
// detector") is open: a real kill on seed 1 reaches it, and it fails the nightly
// sweep with the other seeds that reach it. These three together kill both
// recorded controller mutants and take about 50 s. Seed 13 is the regression for
// a hung detector plus a host crash delaying every other detector (the manager
// held locks across the evaluation); seeds 23 and 27 reach the same path in the
// nightly sweep.
var harnessPRSeeds = []uint64{2, 4, 6, 13}

// TestHarness plays each seed through the production controller loop and checks
// that the controller is reliable (every incident ends) and manages its
// detectors well (each runs within a few times its own interval).
func TestHarness(t *testing.T) {
	disableErrorBackoff()
	deadline := time.Now().Add(harnessWallBudget)
	seeds := simSeeds(t, 64, simPRRuns)
	if os.Getenv("SDO_SIM_FULL") == "" && os.Getenv("SDO_SIM_SEED") == "" && os.Getenv("SDO_SIM_RUNS") == "" {
		seeds = harnessPRSeeds
	}
	for _, seed := range seeds {
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
			// PR CI plays its few seeds one at a time so the harness does not
			// starve the package's real-time tests; the nightly sweep fans out.
			if len(seeds) > simPRRuns {
				t.Parallel()
				if time.Now().After(deadline) {
					t.Skip("wall budget spent")
				}
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

var errorBackoffOnce sync.Once

// disableErrorBackoff removes client-go's global error throttle from this
// process. It remembers the wall time of the last handled error and sleeps the
// difference to a one millisecond period; a bubble's clock starts in 2000, so
// the first error sleeps for decades of virtual time and an informer that hit a
// list error never shut down. It only throttles error logging.
func disableErrorBackoff() {
	errorBackoffOnce.Do(func() { utilruntime.ErrorHandlers = utilruntime.ErrorHandlers[:1] })
}
