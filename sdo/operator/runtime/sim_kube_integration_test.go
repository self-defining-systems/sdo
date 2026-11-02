//go:build integration

// Deterministic simulation of the Kubernetes API for the incident loop.
//
// simKube wraps the in-package API-server model (serverPlanClient: defaulting,
// quantity normalization, resourceVersion preconditions) with a seeded fault
// schedule, and simCache serves the stale reads an informer cache gives a
// caller right after a write. Every fault decision comes from one seeded RNG
// consumed in call order, so a failing seed replays exactly:
//
//	SDO_SIM_SEED=<n> go test ./sdo/operator/runtime -run TestSim
//
// SDO_SIM_RUNS changes the number of seeds swept; a sweep also stops at
// simWallBudget so no profile exceeds the project's five-minute cap.
package runtime

import (
	"os"
	"strconv"
	"testing"
	"time"


)

// simSeeds returns the seeds to sweep; SDO_SIM_SEED replays one.
func simSeeds(t *testing.T, defaultRuns int) []uint64 {
	t.Helper()
	if raw := os.Getenv("SDO_SIM_SEED"); raw != "" {
		seed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid SDO_SIM_SEED %q: %v", raw, err)
		}
		return []uint64{seed}
	}
	runs := defaultRuns
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

// sweepSimSeeds runs body for each seed until the seeds or the wall budget run out.
func sweepSimSeeds(t *testing.T, body func(t *testing.T, seed uint64)) {
	t.Helper()
	sweepSimSeedsN(t, simDefaultRuns, body)
}

// sweepSimSeedsN is sweepSimSeeds with a per-test default run count, for
// simulations whose single run is expensive.
func sweepSimSeedsN(t *testing.T, defaultRuns int, body func(t *testing.T, seed uint64)) {
	t.Helper()
	// Sweeps are bounded by wall time, not work; running them in parallel with
	// each other keeps a shard from paying every sweep's budget back to back.
	t.Parallel()
	deadline := time.Now().Add(simWallBudget)
	ran := 0
	seeds := simSeeds(t, defaultRuns)
	for _, seed := range seeds {
		if ran > 0 && time.Now().After(deadline) {
			break
		}
		ran++
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) { body(t, seed) })
	}
	t.Logf("sim sweep ran %d/%d seed(s)", ran, len(seeds))
}

