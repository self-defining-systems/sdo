// Package runtime chaos suite.
//
// These tests attack the timing/ordering bug class: defects that only surface
// when the schedule around incident handling lines up a certain way (a fault
// injected before the prober is ready to observe it, a responder dispatched
// before the controller resumes, a detector evaluation that lands late). The
// harness randomizes that schedule from a seed and asserts that the runtime's
// invariants hold regardless of the interleaving.
//
// Determinism contract: every run is driven by an explicit, monotonic clock
// and a seeded RNG. The controller's two nondeterministic seams -- its wall
// clock (`now`) and its dispatch-retry jitter (`dispatchJitter`) -- are pinned
// to the harness clock and RNG, so a failing seed replays the exact schedule.
// Set SDO_CHAOS_SEED=<n> to replay one seed; SDO_CHAOS_RUNS=<n> to change the
// number of seeds swept (default chaosDefaultRuns). The sweep is additionally
// bounded by chaosWallBudget so no profile exceeds the project's 5-minute cap.
//
// This file is test-only. It adds no production control flow and reuses the
// in-package fakes defined in controller_test.go (recordingDispatcher,
// staticProvider, testControllerConfig, ...).
package runtime

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const (
	// chaosDefaultRuns is the seed sweep per dimension when SDO_CHAOS_SEED is
	// unset. Kept small enough that the whole suite stays well under the
	// 5-minute property/fuzz cap even under -race.
	chaosDefaultRuns = 128
	// chaosWallBudget caps one dimension's seed sweep. A sweep stops early once
	// this elapses, reporting how many seeds it actually ran, so a slow machine
	// or -race never pushes the suite past the cap.
	chaosWallBudget = 45 * time.Second
	// chaosMaxSteps bounds the evaluation steps of a single scheduled incident
	// so a wedged schedule fails loudly instead of hanging.
	chaosMaxSteps = 80
)

// chaosStart is the fixed epoch every scheduled incident begins from. A fixed
// start plus a seeded clock makes derived values (such as the incident id,
// `app-<now.UnixNano()>`) replay exactly.
var chaosStart = time.Unix(0, 0).UTC()

// chaosSeeds returns the seeds to sweep for one dimension. A single explicit
// seed (SDO_CHAOS_SEED) replays a reported failure; otherwise seeds 1..runs.
func chaosSeeds(t *testing.T) []uint64 {
	t.Helper()
	if raw := os.Getenv("SDO_CHAOS_SEED"); raw != "" {
		seed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid SDO_CHAOS_SEED %q: %v", raw, err)
		}
		return []uint64{seed}
	}
	runs := chaosDefaultRuns
	if raw := os.Getenv("SDO_CHAOS_RUNS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid SDO_CHAOS_RUNS %q", raw)
		}
		runs = parsed
	}
	seeds := make([]uint64, runs)
	for i := range seeds {
		seeds[i] = uint64(i) + 1
	}
	return seeds
}

// newChaosRNG builds a deterministic RNG for one seed. Two streams are derived
// from the same seed so orthogonal perturbations do not alias.
func newChaosRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

// sweepChaosSeeds runs body for each seed until the seeds are exhausted or the
// wall budget elapses, so every dimension shares one bounded, replayable loop.
// body receives the seed; a failing subtest names the seed, so the exact
// schedule replays with SDO_CHAOS_SEED=<n>.
func sweepChaosSeeds(t *testing.T, body func(t *testing.T, seed uint64)) {
	t.Helper()
	seeds := chaosSeeds(t)
	deadline := time.Now().Add(chaosWallBudget)
	ran := 0
	for _, seed := range seeds {
		if ran > 0 && time.Now().After(deadline) {
			break
		}
		ran++
		seed := seed
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
			body(t, seed)
		})
	}
	if ran == 0 {
		t.Fatal("chaos sweep ran no seeds")
	}
	t.Logf("chaos sweep ran %d/%d seed(s)", ran, len(seeds))
}

// chaosToggle is a tiny goroutine-safe boolean. The driver mutates it from the
// test goroutine between synchronous Step calls, so no real contention arises,
// but it keeps the race detector quiet about the detector reading it.
type chaosToggle struct {
	mu sync.Mutex
	on bool
}

func (c *chaosToggle) set(on bool) {
	c.mu.Lock()
	c.on = on
	c.mu.Unlock()
}

func (c *chaosToggle) get() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.on
}

// chaosDetector is a controllable detector. The harness flips its finding on
// and off (and can make it return an error, modeling an unreachable prober or a
// dropped evaluation) independently of how many evaluations the perturbed
// schedule happens to run -- unlike a fixed sequence, it never desyncs when the
// clock jitter changes the step count. It is deterministic and reads only its
// toggles, never an LLM or a verdict.
type chaosDetector struct {
	spec     sdk.DetectorSpec
	severity sdk.FindingSeverity
	active   chaosToggle
	erroring chaosToggle
}

func (d *chaosDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d *chaosDetector) Detect(_ context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.erroring.get() {
		return nil, errChaosEvaluation
	}
	if !d.active.get() {
		return nil, nil
	}
	finding := stateFinding(d.spec.ID)
	finding.Severity = d.severity
	return []sdk.Finding{finding}, nil
}

// errChaosEvaluation stands in for a detector whose backing signal is
// unavailable (an unreachable prober, a dropped snapshot). Per the architecture
// such an error blocks closure but must never open an incident.
var errChaosEvaluation = errChaos("chaos: detector signal unavailable")

type errChaos string

func (e errChaos) Error() string { return string(e) }

func newChaosHealthDetector(id string) *chaosDetector {
	return &chaosDetector{
		severity: sdk.SeverityCritical,
		spec: sdk.DetectorSpec{
			ID: id, Interval: time.Second,
			Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
			Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
			Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical},
			OriginatingCommit: "health-objective",
		},
	}
}

func newChaosIncidentDetector(id string) *chaosDetector {
	return &chaosDetector{
		severity: sdk.SeverityWarning,
		spec: sdk.DetectorSpec{
			ID: id, Interval: time.Second,
			Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
			Persistence:         sdk.PersistencePolicy{Firing: 2, Clearing: 2},
			Batching:            sdk.BatchingPolicy{Severity: sdk.SeverityWarning},
			OriginatingIncident: "incident-0",
			OriginatingCommit:   "commit-0",
		},
	}
}

// chaosController bundles a controller with the fakes and the mutable clock the
// driver advances. The clock pointer backs the controller's `now` seam so every
// internal time read matches the step's explicit time.
type chaosController struct {
	controller *Controller
	dispatcher *recordingDispatcher
	detector   *chaosDetector
	now        time.Time
}

// newChaosController builds a single-health-detector controller with its
// nondeterministic seams pinned to the harness clock and RNG.
func newChaosController(t *testing.T, seed uint64) *chaosController {
	t.Helper()
	detector := newChaosHealthDetector("health")
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 8)}
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, chaosStart,
	)
	if err != nil {
		t.Fatalf("new chaos controller: %v", err)
	}
	cc := &chaosController{controller: controller, dispatcher: dispatcher, detector: detector, now: chaosStart}
	jitter := newChaosRNG(seed ^ 0x5eed5eed5eed5eed)
	controller.now = func() time.Time { return cc.now }
	controller.dispatchJitter = jitter.Float64
	return cc
}

// stepAt advances the harness clock to now and runs one evaluation pass.
func (cc *chaosController) stepAt(t *testing.T, now time.Time) {
	t.Helper()
	cc.now = now
	if err := cc.controller.StepEvents(context.Background(), now, nil); err != nil {
		t.Fatalf("step at %s: %v", now, err)
	}
}

// executeReadyDispatch runs the pending dispatch effect if one is ready and not
// yet executed, returning the dispatched incident id. The controller dispatches
// through a durable effect (crash-replay seam): the effect appears after the
// batch is cut, and executing it is the moment the responder "launches".
func (cc *chaosController) executeReadyDispatch(t *testing.T) (string, bool) {
	t.Helper()
	effect, ok := cc.controller.PendingDispatchEffect()
	if !ok {
		return "", false
	}
	if err := cc.controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute dispatch effect: %v", err)
	}
	request := awaitRequest(t, cc.dispatcher.requests)
	awaitDispatchCompletionQueued(t, cc.controller)
	return request.IncidentID, true
}

// chaosLifecycle is the observable outcome of one scheduled incident. The
// invariants are asserted against these fields, never against private state.
type chaosLifecycle struct {
	dispatchCount int
	incidentID    string
	dispatched    bool
	closed        bool
	steps         int
}

// chaosSchedule holds the seed-chosen timing of one incident. It is a pure
// value derived from the seed, so re-deriving it from the same seed replays the
// identical schedule.
type chaosSchedule struct {
	stepJitterMS []int
	injectAtStep int
	dispatchLag  int
	recoverLag   int
}

// randomSchedule draws a bounded schedule from a fresh RNG for the seed. It
// pre-draws the per-step jitter so the clock advance is a pure function of the
// seed and step index (no live RNG draws during the drive).
func randomSchedule(seed uint64) chaosSchedule {
	rng := newChaosRNG(seed)
	jitter := make([]int, chaosMaxSteps)
	for i := range jitter {
		jitter[i] = rng.IntN(500)
	}
	return chaosSchedule{
		stepJitterMS: jitter,
		injectAtStep: 1 + rng.IntN(4),
		dispatchLag:  rng.IntN(3),
		recoverLag:   1 + rng.IntN(3),
	}
}

// driveIncident runs one full incident lifecycle under a seeded schedule:
// quiet, fault injection, firing, dispatch (after a chosen lag), responder
// completion, recovery (after a chosen lag), and closure. It is the shared body
// every timing dimension perturbs.
func driveIncident(t *testing.T, cc *chaosController, sched chaosSchedule) chaosLifecycle {
	t.Helper()
	now := chaosStart
	result := chaosLifecycle{}
	dispatchReadyStep := -1
	recoverTarget := -1

	for step := 0; step < chaosMaxSteps; step++ {
		now = now.Add(time.Second + time.Duration(sched.stepJitterMS[step])*time.Millisecond)
		if step == sched.injectAtStep {
			cc.detector.active.set(true)
		}
		if recoverTarget >= 0 && step >= recoverTarget {
			cc.detector.active.set(false)
		}
		cc.stepAt(t, now)
		result.steps = step + 1

		if !result.dispatched {
			if _, ready := cc.controller.PendingDispatchEffect(); ready {
				if dispatchReadyStep < 0 {
					dispatchReadyStep = step
				}
				if step-dispatchReadyStep >= sched.dispatchLag {
					id, ok := cc.executeReadyDispatch(t)
					if ok {
						result.dispatched = true
						result.incidentID = id
						recoverTarget = step + sched.recoverLag
					}
				}
			}
		}
		if result.dispatched && !cc.controller.IncidentOpen() {
			result.closed = true
			break
		}
	}
	result.dispatchCount = cc.dispatcher.callCount()
	return result
}

// TestChaosExactlyOnceDispatchUnderRandomizedCadence is the first dimension:
// the evaluation cadence and the dispatch lag are randomized, and the invariant
// is that a single fault produces exactly one dispatch (no lost incident, no
// double dispatch) and a clean closure. Replaying the seed from scratch must
// yield an identical dispatch count and incident id -- the determinism contract
// that lets any failing seed reproduce.
func TestChaosExactlyOnceDispatchUnderRandomizedCadence(t *testing.T) {
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		first := driveIncident(t, newChaosController(t, seed), randomSchedule(seed))
		if !first.dispatched {
			t.Fatalf("fault never dispatched within %d steps: %+v", chaosMaxSteps, first)
		}
		if first.dispatchCount != 1 {
			t.Fatalf("expected exactly one dispatch, got %d: %+v", first.dispatchCount, first)
		}
		if !first.closed {
			t.Fatalf("incident never closed after recovery: %+v", first)
		}

		// Rebuild controller and schedule from the same seed: the observable
		// outcome must be identical so a failing seed reproduces exactly.
		second := driveIncident(t, newChaosController(t, seed), randomSchedule(seed))
		if second != first {
			t.Fatalf("replay diverged: first=%+v second=%+v", first, second)
		}
	})
}
