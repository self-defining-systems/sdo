package runtime

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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

func stepSeconds(t *testing.T, c *Controller, world *settlingWorld, seconds int) {
	t.Helper()
	for range seconds {
		autoStep(t, c, world.clock, time.Second)
	}
}

// seedStore records the store's condition as firing for a minute, and in this
// observation.
func seedStore(t *testing.T, c *Controller, world *settlingWorld, store *storeDown) {
	t.Helper()
	findings, err := store.Detect(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	now := world.clock.Now()
	for i := range findings {
		findings[i].Fingerprint = FindingFingerprint(findings[i])
	}
	if state := c.tracker.states[findingKey(findings[0])]; state != nil {
		state.FirstSeenAt = now.Add(-time.Minute)
	} else {
		c.tracker.Observe(now.Add(-time.Minute), memory.WorkloadControllersDetectorID, findings)
	}
	scope := newRecoveryScope(alertKeys(c.currentIncidentRequest))
	c.rememberRecoverySample(memory.WorkloadControllersDetectorID, now, scope)
	c.recordEvaluation(memory.WorkloadControllersDetectorID, now, findings, scope)
}

// A condition older than the write still leaves the settled write its own window.
func TestAnOldConditionWaitsTheWritesStabilityWindow(t *testing.T) {
	c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	joinStore(t, c, world, store)
	stepSeconds(t, c, world, 1)
	seedStore(t, c, world, store)
	stepSeconds(t, c, world, 3)
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off before the settled write had its stability window")
	}
	stepSeconds(t, c, world, 1)
	if !autoReport(c).GetHandoffSelected() {
		t.Fatal("the window passed without a handoff")
	}
}

// A namesake of a mapped workload in another namespace is no component.
func TestABeyondConditionIsInTheApplicationNamespace(t *testing.T) {
	m := program.AppMap{Components: []program.AppComponent{{Name: "store", Workload: "StatefulSet/store"}}}
	view := sdktest.Snapshot{NamespaceName: "app"}
	if _, ok := m.ComponentOf(view, sdk.ObjectRef{Kind: "StatefulSet", Namespace: "other", Name: "store"}); ok {
		t.Fatal("a workload of another namespace is a component")
	}
	if _, ok := m.ComponentOf(view, sdk.ObjectRef{Kind: "StatefulSet", Namespace: "app", Name: "store"}); !ok {
		t.Fatal("the application's own workload is no component")
	}
}

// A program repaired one part; a condition its write cannot clear starts the
// agent as soon as the write settled and had its stability window.
func TestPartialProgramFixHandsOffTheConditionItCannotClear(t *testing.T) {
	c, world, store, _ := beyondWorld(t, settlingCase{healAfter: 3 * time.Second}, settings.AfterPlaybookAgent)
	key := joinStore(t, c, world, store)
	// First seen at 1 s, so it has fired for the 5 s window at 6 s.
	stepSeconds(t, c, world, 5)
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off before the condition fired for a stability window")
	}
	stepSeconds(t, c, world, 1)
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
	stepSeconds(t, c, world, 10)
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
			stepSeconds(t, c, w, 1)
			s.down.Store(false)
		},
		"the condition is not observed": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			stepSeconds(t, c, w, 1)
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
		"another written object is unplaced": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			stepSeconds(t, c, w, 1)
			attempt := autoReport(c).GetMitigations()[0]
			attempt.TouchedObjects = append(attempt.TouchedObjects, &contracts.ObjectRef{ApiVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"})
		},
		"a script wrote": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			stepSeconds(t, c, w, 1)
			plan := autoReport(c).GetMitigations()[0].GetApplyReceipt().GetPlan()
			plan.Actions = append(plan.Actions, &contracts.Action{Kind: &contracts.Action_RunScript{RunScript: &contracts.RunScript{}}})
		},
		"a transient every few seconds": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			for range 15 {
				s.down.Store(true)
				stepSeconds(t, c, w, 2)
				s.down.Store(false)
				stepSeconds(t, c, w, 1)
			}
		},
		"a mutation is pending": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			stepSeconds(t, c, w, 1)
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
		"a StatefulSet restore continues": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			stepSeconds(t, c, w, 1)
			c.responderLoop.operationMu.Lock()
			t.Cleanup(c.responderLoop.operationMu.Unlock)
		},
		"the stability extension runs": func(t *testing.T, c *Controller, w *settlingWorld, s *storeDown) {
			joinStore(t, c, w, s)
			stepSeconds(t, c, w, 1)
			autoReport(c).StabilityDeadline = autoReport(c).GetRecoveryDeadline()
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
			prepare(t, c, world, store)
			bound := autoReport(c).GetRecoveryDeadline().AsTime()
			for world.clock.Now().Before(bound.Add(-time.Second)) {
				stepSeconds(t, c, world, 1)
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
	for i := 0; i < 120 && c.IncidentOpen(); i++ {
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
		stepSeconds(t, c, world, 30)
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
		stepSeconds(t, c, world, 3)
		if autoReport(c).GetHandoffSelected() {
			t.Fatal("a condition that just joined started the agent")
		}
		stepSeconds(t, c, world, 3)
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
	stepSeconds(t, c, world, 30)
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off during the stage's own rollout")
	}
	setUpdatedReplicas(t, world, 1)
	autoStep(t, c, world.clock, time.Second)
	if !autoReport(c).GetHandoffSelected() {
		t.Fatal("the settled rollout did not hand off")
	}
}

// A rollout that a write reaches through another object (here the whole
// namespace) holds the handoff although the write itself settled.
func TestARelatedRolloutHoldsTheHandoffBeyondTheStage(t *testing.T) {
	for _, rolling := range []bool{false, true} {
		c, world, store, _ := beyondWorld(t, settlingCase{healAfter: time.Second}, settings.AfterPlaybookAgent)
		joinStore(t, c, world, store)
		stepSeconds(t, c, world, 1)
		observed := int64(2)
		if rolling {
			observed = 1
		}
		if _, err := world.client.AppsV1().Deployments("app").Create(t.Context(), &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app", Generation: 2}, Status: appsv1.DeploymentStatus{ObservedGeneration: observed}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, RepairActionReceipt{ActionID: "synthetic", Success: true, CompletedAt: world.clock.Now(), Resources: []sdk.ObjectRef{{Kind: "*", Namespace: "app"}}})
		stepSeconds(t, c, world, 20)
		if got := autoReport(c).GetHandoffSelected(); got == rolling {
			t.Fatalf("rolling=%v: handoff=%v", rolling, got)
		}
	}
}

// A learned pass that is running or queued may still write: it ends first.
func TestALearnedPassHoldsTheHandoff(t *testing.T) {
	c, world, store, _ := beyondWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	joinStore(t, c, world, store)
	autoStep(t, c, world.clock, time.Second) // the learned pass the new condition asks for
	c.midStage.running = true
	observed := len(c.history)
	stepSeconds(t, c, world, 10)
	if autoReport(c).GetHandoffSelected() || len(c.history) != observed {
		t.Fatal("handed off, or judged recovery, during a learned pass")
	}
	// A pass that starts while an observation runs holds the decision too.
	seedStore(t, c, world, store)
	c.mu.Lock()
	c.automaticRecoveryLocked(world.clock.Now(), c.awaitingAutomaticRecoveryLocked(), false, recoveryCheck{beyondStage: c.currentIncidentRequest.JoinedKeys}, nil)
	c.mu.Unlock()
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handed off during a learned pass that started mid-observation")
	}
	c.midStage.running = false
	c.midStage.pending = false
	stepSeconds(t, c, world, 6)
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
		for world.clock.Now().Before(bound.Add(-time.Second)) {
			stepSeconds(t, c, world, 1)
		}
		if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
			t.Fatalf("%s: ended before its bound", policy)
		}
		autoStep(t, c, world.clock, time.Second)
		if autoReport(c).GetHandoffSelected() || !c.escalatedAt.Equal(bound) {
			t.Fatalf("%s: handoff=%v escalated=%s, bound %s", policy, autoReport(c).GetHandoffSelected(), c.escalatedAt, bound)
		}
	}
}
