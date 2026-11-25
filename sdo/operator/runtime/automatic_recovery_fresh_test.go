package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

const buyTestID = memory.TrafficDetectorPrefix + "shop"

// buyTest is user test "buy" of workload shop, which samples every interval
// and fails at the offsets from the stage's write that failing accepts.
type buyTest struct {
	every    time.Duration
	failing  func(time.Duration) bool
	silent   func(time.Duration) bool
	unproven bool
	start    time.Time
}

func (b *buyTest) window(now time.Time) map[string]traffic.Window {
	workload := traffic.Workload{Name: "shop", Purpose: traffic.PurposeHealthProbe, RatePerSecond: 1, SampleInterval: traffic.Duration(b.every),
		SLO: traffic.SLO{MaxAge: traffic.Duration(max(30*time.Second, 4*b.every))}, Scenarios: []traffic.WorkloadScenario{{ID: "buy", Weight: 1}}}
	var samples []traffic.Sample
	for at := b.start.Add(-10 * b.every); !at.After(now); at = at.Add(b.every) {
		offset := at.Sub(b.start)
		switch {
		case b.silent != nil && b.silent(offset):
		case b.failing(offset):
			samples = append(samples, traffic.Sample{At: at, Latency: time.Second, Status: 500, Outcome: traffic.OutcomeError, Requests: 1})
		default:
			samples = append(samples, traffic.Sample{At: at, Latency: time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK, Requests: 1})
		}
	}
	return map[string]traffic.Window{"shop": {Workload: workload, ObservedAt: now,
		Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "buy", Target: traffic.Target{Service: "api"}}, Samples: samples, Qualified: !b.unproven}}}}
}

// stuckUserTest keeps the test's alert firing whatever its samples say, as a
// detector's window and persistence do after a repair.
type stuckUserTest struct{ unavailable bool }

func (*stuckUserTest) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: buyTestID, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}, Interval: time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "seed"}
}

func (d *stuckUserTest) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.unavailable {
		return nil, context.DeadlineExceeded
	}
	return []sdk.Finding{{DetectorID: buyTestID, RuleID: traffic.RuleIDPrefix + "buy", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Summary: "buy violates its SLO", Evidence: "synthetic"}}, nil
}

// freshWorld is the applied restart of Deployment api with user test buy
// joined to the alert set.
func freshWorld(t *testing.T, test *buyTest, policy string) (*Controller, *settlingWorld, string) {
	t.Helper()
	if test.every == 0 {
		test.every = 2 * time.Second
	}
	detector := &stuckUserTest{}
	c, world, _ := automaticRecoveryWorld(t, settlingCase{}, policy, detector)
	test.start = world.clock.Now()
	world.probes = test.window
	found, _ := detector.Detect(t.Context(), nil)
	if !c.joinAlertSet(found[0], test.start) {
		t.Fatal("the user test did not join")
	}
	return c, world, findingKey(found[0])
}

// handoffAfter steps until the agent is handed the incident or limit passes.
func handoffAfter(t *testing.T, c *Controller, world *settlingWorld, start time.Time, limit time.Duration) (time.Duration, bool) {
	t.Helper()
	for world.clock.Now().Sub(start) < limit {
		autoStep(t, c, world.clock, time.Second)
		if autoReport(c).GetHandoffSelected() {
			return world.clock.Now().Sub(start), true
		}
	}
	return limit, false
}

// The writes settled, had their effect lag, and the user test still fails on
// samples taken after it: the agent starts, once, long before the bound.
func TestAUserTestThatStillFailsAfterTheEffectLagHandsOff(t *testing.T) {
	test := &buyTest{failing: func(time.Duration) bool { return true }}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	at, ok := handoffAfter(t, c, world, test.start, 90*time.Second)
	report := autoReport(c)
	if !ok || at < stageEffectLag+2*test.every || at > stageEffectLag+6*test.every {
		t.Fatalf("handoff = %v at %s", ok, at)
	}
	if !slices.Contains(report.GetHandoffKeys(), key) || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED {
		t.Fatalf("report = %v", report)
	}
	if c.learnedPassAllowedLocked() || c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
		t.Fatalf("the stage may still write, or dispatch state = %s", c.dispatchState)
	}
	c.mu.Lock()
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.mu.Unlock()
	stepSeconds(t, c, world, 60)
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_RUNNING || world.restarts.Load() != 1 {
		t.Fatalf("dispatch state = %s, %d writes", c.dispatchState, world.restarts.Load())
	}
}

// What is not failing evidence, while the alert itself stays active: none of
// it starts the agent before the bound.
func TestNoFreshFailureIsNoHandoff(t *testing.T) {
	always := func(time.Duration) bool { return true }
	before := func(limit time.Duration) func(time.Duration) bool {
		return func(at time.Duration) bool { return at < limit }
	}
	for name, test := range map[string]*buyTest{
		"users recovered, the alert clears slowly": {failing: before(0)},
		"the application held the failure a while": {failing: before(44 * time.Second)},
		"failures then no samples":                 {failing: always, silent: func(at time.Duration) bool { return at > 44*time.Second }},
		"two failures are no evaluation":           {failing: always, silent: func(at time.Duration) bool { return at >= 50*time.Second }},
		"recovered just now":                       {failing: before(50 * time.Second)},
		"an isolated failure now and then":         {failing: func(at time.Duration) bool { return at%(20*time.Second) == 0 }},
		"every 120 seconds":                        {failing: always, every: 2 * time.Minute},
		"an unproven test":                         {failing: always, unproven: true},
	} {
		t.Run(name, func(t *testing.T) {
			c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
			bound := autoReport(c).GetRecoveryDeadline().AsTime().Sub(test.start)
			if at, ok := handoffAfter(t, c, world, test.start, bound-time.Second); ok {
				t.Fatalf("handed off at %s, bound %s", at, bound)
			}
			if _, ok := handoffAfter(t, c, world, test.start, bound+time.Second); !ok {
				t.Fatal("the bound did not hand off")
			}
		})
	}
}

// A failing alert whose user test is not known to be failing now (its
// detector could not evaluate) waits.
func TestAFailingSampleNeedsTheAlertFiringNow(t *testing.T) {
	test := &buyTest{failing: func(time.Duration) bool { return true }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.detectors[buyTestID].(*stuckUserTest).unavailable = true
	if at, ok := handoffAfter(t, c, world, test.start, 90*time.Second); ok {
		t.Fatalf("handed off at %s", at)
	}
}

// Samples during the stage's own rollout say nothing: the lag runs from its
// settle, and a rollout that resumes starts it over.
func TestTheEffectLagRunsFromTheSettle(t *testing.T) {
	test := &buyTest{failing: func(time.Duration) bool { return true }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stretchDeadline(c, 10*time.Minute)
	setUpdatedReplicas(t, world, 0)
	stepSeconds(t, c, world, 30)
	setUpdatedReplicas(t, world, 1)
	stepSeconds(t, c, world, 20)
	setUpdatedReplicas(t, world, 0)
	stepSeconds(t, c, world, 10)
	setUpdatedReplicas(t, world, 1)
	at, ok := handoffAfter(t, c, world, test.start, 3*time.Minute)
	if want := time.Minute + stageEffectLag; !ok || at < want || at > want+15*time.Second {
		t.Fatalf("handoff = %v at %s, want just after %s", ok, at, want)
	}
}

// stretchDeadline gives the report an inflated estimate, as a startup history
// that counted outages does.
func stretchDeadline(c *Controller, d time.Duration) {
	c.currentIncidentRequest.ResponseDeadline = time.Time{}
	report := autoReport(c)
	report.RecoveryDeadline = contractTimestamp(lastStageReceipt(report).Add(d))
}

// A rollout that never settles, and settled writes whose alert never gets new
// evidence, end at their own caps however long the estimate.
func TestAnInflatedEstimateIsCapped(t *testing.T) {
	for _, tc := range []struct {
		name    string
		settled bool
		want    time.Duration
	}{
		{"never settles", false, stageMinimumLag + stageStartupBound + 5*time.Second},
		{"no new evidence", true, stageSettledBound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := &buyTest{failing: func(time.Duration) bool { return !tc.settled }}
			c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
			stretchDeadline(c, 28*time.Minute)
			if !tc.settled {
				setUpdatedReplicas(t, world, 0)
			}
			at, ok := handoffAfter(t, c, world, test.start, 20*time.Minute)
			if !ok || at < tc.want-time.Second || at > tc.want+2*time.Second {
				t.Fatalf("handoff = %v at %s, want %s", ok, at, tc.want)
			}
			if exit := autoReport(c).GetRecoveryWait().GetExitReason(); exit != contracts.RecoveryWait_EXIT_REASON_DEADLINE {
				t.Fatalf("exit = %s", exit)
			}
		})
	}
}

// wait and escalate keep the bound they computed, whatever the evidence.
func TestAFailingUserTestLeavesTheOtherPoliciesAlone(t *testing.T) {
	for _, policy := range []string{settings.AfterPlaybookWait, settings.AfterPlaybookEscalate} {
		test := &buyTest{failing: func(time.Duration) bool { return true }}
		c, world, _ := freshWorld(t, test, policy)
		stretchDeadline(c, 15*time.Minute)
		setUpdatedReplicas(t, world, 0)
		for world.clock.Now().Sub(test.start) < 15*time.Minute-time.Second {
			autoStep(t, c, world.clock, time.Second)
		}
		if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
			t.Fatalf("%s: ended early at %s", policy, c.escalatedAt)
		}
	}
}

// A restarted controller observes the settle anew: later, never earlier.
func TestARestartObservesTheSettleAnew(t *testing.T) {
	test := &buyTest{failing: func(time.Duration) bool { return true }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stepSeconds(t, c, world, 20)
	c.stageSettledAt = time.Time{}
	at, ok := handoffAfter(t, c, world, test.start, 90*time.Second)
	if want := 20*time.Second + stageEffectLag; !ok || at < want {
		t.Fatalf("handoff = %v at %s, want after %s", ok, at, want)
	}
}

// A later write starts the effect lag over even when nothing showed unsettled.
func TestALaterWriteStartsTheEffectLagOver(t *testing.T) {
	test := &buyTest{failing: func(time.Duration) bool { return true }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stepSeconds(t, c, world, 30)
	c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, RepairActionReceipt{ActionID: "synthetic", Success: true, CompletedAt: world.clock.Now()})
	at, ok := handoffAfter(t, c, world, test.start, 94*time.Second)
	if want := 30*time.Second + stageEffectLag; !ok || at < want {
		t.Fatalf("handoff = %v at %s, want after %s", ok, at, want)
	}
}

// The caps never cut a mutation in flight or the stability extension short.
func TestTheCapsWaitForAMutationAndTheStabilityExtension(t *testing.T) {
	for name, hold := range map[string]func(*testing.T, *Controller){
		"mutation in flight": func(t *testing.T, c *Controller) {
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			lease, err := gate.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if err := lease.Finish(true); err != nil {
				t.Fatal(err)
			}
			c.responderLoop.MutationGate = gate
		},
		"stability extension": func(_ *testing.T, c *Controller) {
			autoReport(c).StabilityDeadline = autoReport(c).GetRecoveryDeadline()
		},
	} {
		t.Run(name, func(t *testing.T) {
			test := &buyTest{failing: func(time.Duration) bool { return false }}
			c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
			stretchDeadline(c, 15*time.Minute)
			hold(t, c)
			if at, ok := handoffAfter(t, c, world, test.start, 15*time.Minute-time.Second); ok {
				t.Fatalf("handed off at %s", at)
			}
		})
	}
}
