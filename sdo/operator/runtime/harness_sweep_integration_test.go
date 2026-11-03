//go:build integration

package runtime

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"sdo.dev/operator/runtime/sim"
)

// harnessWallBudget stops seeds that have not started when it is spent.
const harnessWallBudget = 200 * time.Second

// harnessPRSeeds are the seeds PR CI plays. Seeds 1 to 3 together kill both
// recorded controller mutants and take about 50 s. Seed 13 is the regression for
// a hung detector plus a host crash delaying every other detector (the manager
// held locks across the evaluation); seeds 23 and 27 reach the same path in the
// nightly sweep. Seed 35 kills the mutant that ends the process on a transient
// API error (startup reads under errors), and 19 the one that never ends a hung
// agent (its first dispatch hangs, and nothing but the controller can stop it).
var harnessPRSeeds = []uint64{1, 2, 3, 13, 19, 35}

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
	// The seeds fan out inside one group that this serial test waits for, so
	// no other test of the package runs beside them: the children the process
	// has had waited for during the group are the harness's own, and a
	// simulation runs none.
	before := waitedChildren(t)
	t.Run("seeds", func(t *testing.T) {
		for _, seed := range seeds {
			t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
				t.Parallel()
				if time.Now().After(deadline) {
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
	})
	if spawned := waitedChildren(t) - before; spawned != 0 {
		t.Errorf("the harness spawned a process (its children incurred %d page faults): a deterministic simulation runs none. "+
			"Inject the edge through RunOptions; find the caller with `strace -f -e trace=execve -o /tmp/spawns go test -tags=integration -run '^TestHarness$' ./runtime`", spawned)
	}
}

// waitedChildren is the page-fault count of every child process this process
// has waited for. Any process that ran at all adds to it.
func waitedChildren(t *testing.T) int64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &usage); err != nil {
		t.Fatal(err)
	}
	return int64(usage.Minflt) + int64(usage.Majflt)
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
