//go:build integration

package runtime

import (
	"os"
	"strconv"
	"testing"
)

// simPRRuns is the fixed seed prefix a sweep runs outside the full profile.
const simPRRuns = 3

// simRuns returns the run count for a campaign: its declared full count when
// SDO_SIM_FULL is set (nightly), otherwise at most pr runs (a fixed prefix of
// the same seed sequence, so a PR failure replays under the full profile).
func simRuns(full, pr int) int {
	if os.Getenv("SDO_SIM_FULL") != "" || full <= pr {
		return full
	}
	return pr
}

// simSeeds returns the seeds to sweep; SDO_SIM_SEED replays one.
func simSeeds(t *testing.T, defaultRuns, prRuns int) []uint64 {
	t.Helper()
	if raw := os.Getenv("SDO_SIM_SEED"); raw != "" {
		seed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid SDO_SIM_SEED %q: %v", raw, err)
		}
		return []uint64{seed}
	}
	runs := simRuns(defaultRuns, prRuns)
	if raw := os.Getenv("SDO_SIM_RUNS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid SDO_SIM_RUNS %q", raw)
		}
		runs = parsed
	}
	seeds := make([]uint64, runs)
	for i := range seeds {
		seeds[i] = uint64(i) + 1
	}
	return seeds
}
