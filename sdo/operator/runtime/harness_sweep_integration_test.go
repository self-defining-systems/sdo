//go:build integration

package runtime

import (
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"

	"sdo.dev/operator/runtime/sim"
)

// TestHarness plays each seed through the production controller loop and checks
// that the controller is reliable and manages its detectors well.
func TestHarness(t *testing.T) {
	sweepSimSeedsPR(t, 64, 3, func(t *testing.T, seed uint64) {
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

var _ = strconv.Itoa
