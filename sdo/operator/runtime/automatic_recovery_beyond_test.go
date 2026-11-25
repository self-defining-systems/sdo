package runtime

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// storeDown is a Kubernetes condition on StatefulSet store, which no program targets.
type storeDown struct {
	down        atomic.Bool
	unavailable atomic.Bool
}

func (*storeDown) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: memory.WorkloadControllersDetectorID, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "StatefulSet"}}, Interval: time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "seed"}
}

func (d *storeDown) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.unavailable.Load() {
		return nil, context.DeadlineExceeded
	}
	if !d.down.Load() {
		return []sdk.Finding{}, nil
	}
	return []sdk.Finding{{DetectorID: memory.WorkloadControllersDetectorID, RuleID: "statefulset-unready", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}, Summary: "store has no ready replica", Evidence: "ready=0 desired=1"}}, nil
}

// storeMap is the accepted map of components api, queue and store, with the
// given edges; without api when the map does not place it.
func storeMap(edges string, withAPI bool) gitRead {
	api := "  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\n"
	if !withAPI {
		api = ""
	}
	return fakeArchitecture("# Manifest inventory\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" + api +
		"  - {name: queue, workload: Deployment/queue, services: [queue], holds_data: false}\n" +
		"  - {name: store, workload: StatefulSet/store, services: [store], holds_data: false}\n" +
		"edges: [" + edges + "]\noperations: []\nprobes: []\n```\n<!-- sdo:application-map:end -->\n")
}

// beyondWorld is the stage's restart of Deployment api applied, with the store
// healthy. api calls the store, so the write to api cannot repair the store.
func beyondWorld(t *testing.T, tc settlingCase, policy string) (*Controller, *settlingWorld, *storeDown, *IncidentClosure) {
	t.Helper()
	store := &storeDown{}
	c, world, closed := automaticRecoveryWorld(t, tc, policy, store)
	c.gitRead = storeMap("{from: api, to: store}", true)
	return c, world, store, closed
}

// joinStore makes the store's condition an alert of the open incident.
func joinStore(t *testing.T, c *Controller, world *settlingWorld, store *storeDown) string {
	t.Helper()
	store.down.Store(true)
	findings, err := store.Detect(t.Context(), nil)
	if err != nil || !c.joinAlertSet(findings[0], world.clock.Now()) {
		t.Fatalf("the condition did not join the alert set: %v", err)
	}
	return findingKey(findings[0])
}

// A program repaired one part; a condition its write cannot clear starts the
// agent as soon as the write settled and had its stability window.
func TestPartialProgramFixHandsOffTheConditionItCannotClear(t *testing.T) {
	c, world, store, _ := beyondWorld(t, settlingCase{healAfter: 3 * time.Second}, settings.AfterPlaybookAgent)
	key := joinStore(t, c, world, store)
	autoStep(t, c, world.clock, 4*time.Second)
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off before the settled write had its stability window")
	}
	autoStep(t, c, world.clock, time.Second)
	report := autoReport(c)
	if !report.GetHandoffSelected() || !slices.Contains(report.GetHandoffKeys(), key) || report.GetHandoffState() == nil ||
		report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED {
		t.Fatalf("report = %v", report)
	}
	if c.learnedPassAllowedLocked() || c.awaitingAutomaticRecoveryLocked() != nil {
		t.Fatal("the stage may still write after the agent owns the repair")
	}
	// One dispatch: the same alert never selects it again.
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
		t.Fatalf("dispatch state = %s", c.dispatchState)
	}
	c.mu.Lock()
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.mu.Unlock()
	autoStep(t, c, world.clock, 10*time.Second)
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_RUNNING || world.restarts.Load() != 1 {
		t.Fatalf("dispatch state = %s, %d writes", c.dispatchState, world.restarts.Load())
	}
}

// What waits its whole bound: a full fix whose alerts clear slowly, a condition
// that can be a consequence of what was repaired, one that clears by itself,
// one not observed now, and an application whose map does not say.
func TestAlertsTheWritesCanStillClearWaitTheBound(t *testing.T) {
	for name, prepare := range map[string]func(*testing.T, *Controller, *settlingWorld, *storeDown){
		"request alerts clear slowly": func(*testing.T, *Controller, *settlingWorld, *storeDown) {},
		"the condition depends on the written component": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			c.gitRead = storeMap("{from: store, to: queue}, {from: queue, to: api}", true)
			joinStore(t, c, w, s)
		},
		"the condition cleared": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			s.down.Store(false)
		},
		"the condition is not observed": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			s.unavailable.Store(true)
		},
		"a clue": func(_ *testing.T, _ *Controller, _ *settlingWorld, s *storeDown) { s.down.Store(true) },
		"no application map": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			c.gitRead = nil
			joinStore(t, c, w, s)
		},
		"an unplaced write": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			c.gitRead = storeMap("", false)
			joinStore(t, c, w, s)
		},
		"a targeted condition": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			autoStep(t, c, w.clock, time.Second) // the learned pass the new condition asks for
			attempt := autoReport(c).GetMitigations()[0]
			found, _ := s.Detect(t.Context(), nil)
			attempt.RemainingFindingFingerprints = append(attempt.RemainingFindingFingerprints, FindingFingerprint(found[0]))
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
			prepare(t, c, world, store)
			bound := autoReport(c).GetRecoveryDeadline().AsTime()
			for world.clock.Now().Before(bound.Add(-10 * time.Second)) {
				autoStep(t, c, world.clock, 10*time.Second)
			}
			if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
				t.Fatalf("the agent started at %s, bound %s", world.clock.Now(), bound)
			}
		})
	}
}

// A program that fixed everything closes alone, however slowly its alerts clear.
func TestFullProgramFixWithSlowAlertsClosesWithoutTheAgent(t *testing.T) {
	c, world, _, closed := beyondWorld(t, settlingCase{healAfter: 80 * time.Second}, settings.AfterPlaybookAgent)
	for i := 0; i < 100 && c.IncidentOpen(); i++ {
		autoStep(t, c, world.clock, time.Second)
	}
	report := deterministicStageReport(closed.Request.PriorStages)
	if c.IncidentOpen() || report.GetHandoffSelected() || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatalf("report = %v", report)
	}
}

// A new condition in the middle of the wait starts the agent then, also when
// the controller restarted in between; no write is repeated.
func TestUnexplainedConditionMidWaitHandsOffAcrossARestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
		autoStep(t, c, world.clock, 30*time.Second)
		if restart {
			checkpoint, err := c.ExportState()
			if err != nil {
				t.Fatal(err)
			}
			again, err := NewController(c.config, []sdk.Detector{c.responderLoop.Detectors[0], store}, world, c.dispatcher, world.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			again.now, again.codePath, again.gitRead = world.clock.Now, c.codePath, c.gitRead
			loop := c.responderLoop
			if err := again.RestoreState(checkpoint); err != nil {
				t.Fatal(err)
			}
			again.responderLoop = loop
			c = again
		}
		key := joinStore(t, c, world, store)
		// The programs are asked about the new condition first.
		autoStep(t, c, world.clock, time.Second)
		autoStep(t, c, world.clock, time.Second)
		if report := autoReport(c); !report.GetHandoffSelected() || !slices.Contains(report.GetHandoffKeys(), key) || world.restarts.Load() != 1 {
			t.Fatalf("restart=%v: report = %v, %d writes", restart, report, world.restarts.Load())
		}
	}
}

// While a written object still rolls out the stage owns it: no handoff.
func TestAProgressingRolloutHoldsTheHandoff(t *testing.T) {
	c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	joinStore(t, c, world, store)
	setUpdatedReplicas(t, world, 0)
	for range 3 {
		autoStep(t, c, world.clock, 10*time.Second)
	}
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off during the stage's own rollout")
	}
	setUpdatedReplicas(t, world, 1)
	autoStep(t, c, world.clock, time.Second)
	if !autoReport(c).GetHandoffSelected() {
		t.Fatal("the settled rollout did not hand off")
	}
}

// A learned pass that is running or queued may still write: it ends first.
func TestALearnedPassHoldsTheHandoff(t *testing.T) {
	c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	joinStore(t, c, world, store)
	autoStep(t, c, world.clock, time.Second) // the learned pass the new condition asks for
	c.midStage.running = true
	autoStep(t, c, world.clock, 10*time.Second)
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off during a learned pass")
	}
	c.midStage.running = false
	c.midStage.pending = false
	autoStep(t, c, world.clock, time.Second)
	if !autoReport(c).GetHandoffSelected() {
		t.Fatal("the ended pass did not hand off")
	}
}

// after_playbook wait and escalate, and a response without an agent, keep
// their bound.
func TestPoliciesWithoutAnAgentKeepTheirBound(t *testing.T) {
	for _, policy := range []string{settings.AfterPlaybookWait, settings.AfterPlaybookEscalate, "no agent stage"} {
		configured := policy
		if policy == "no agent stage" {
			configured = settings.AfterPlaybookAgent
		}
		c, world, store, _ := beyondWorld(t, settlingCase{}, configured)
		if policy != configured {
			c.codePath.Selection.ResponseStages = []string{"deterministic"}
		}
		joinStore(t, c, world, store)
		bound := autoReport(c).GetRecoveryDeadline().AsTime()
		autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
		if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
			t.Fatalf("%s: ended before its bound", policy)
		}
		autoStep(t, c, world.clock, time.Second)
		if autoReport(c).GetHandoffSelected() || !c.escalatedAt.Equal(bound) {
			t.Fatalf("%s: handoff=%v escalated=%s, bound %s", policy, autoReport(c).GetHandoffSelected(), c.escalatedAt, bound)
		}
	}
}
