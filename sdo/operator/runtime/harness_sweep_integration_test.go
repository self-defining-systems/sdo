//go:build integration

package runtime

import (
	"fmt"
	"maps"
	"os"
	"slices"
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

// The seeds PR CI plays are registered, not listed: a PR that pins a regression
// calls registerPRSeed from an init next to its scenario or fix, so no two PRs
// edit the same line.
// Each seed costs CI time, so keep the set small.
var prSeeds = map[uint64]string{}

// registerPRSeed adds a seed to the PR CI set with the reason it is there. A
// seed registered twice is a mistake and panics.
func registerPRSeed(seed uint64, why string) {
	if why == "" {
		panic(fmt.Sprintf("harness seed %d registered without a reason", seed))
	}
	if prev, dup := prSeeds[seed]; dup {
		panic(fmt.Sprintf("harness seed %d registered twice (%q and %q)", seed, prev, why))
	}
	prSeeds[seed] = why
}

// harnessPRSeeds returns the registered seeds in ascending order.
func harnessPRSeeds() []uint64 {
	return slices.Sorted(maps.Keys(prSeeds))
}

// TestHarness plays each seed through the production controller loop and checks
// that the controller is reliable (every incident ends) and manages its
// detectors well (each runs within a few times its own interval).
func TestHarness(t *testing.T) {
	disableErrorBackoff()
	deadline := time.Now().Add(harnessWallBudget)
	seeds := simSeeds(t, 64, simPRRuns)
	if os.Getenv("SDO_SIM_FULL") == "" && os.Getenv("SDO_SIM_SEED") == "" && os.Getenv("SDO_SIM_RUNS") == "" {
		seeds = harnessPRSeeds()
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

// TestPRSeedRegistry checks the set PR CI plays is sorted, reasoned and cannot
// take a seed twice, so registrations from separate PRs never silently merge.
func TestPRSeedRegistry(t *testing.T) {
	seeds := harnessPRSeeds()
	if len(seeds) == 0 || !slices.IsSorted(seeds) {
		t.Fatalf("PR seeds %v: want a non-empty ascending set", seeds)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("registering a seed twice did not panic")
		}
	}()
	registerPRSeed(seeds[0], "again")
}
