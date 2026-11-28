package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/apihistory"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
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

// stretchDeadline gives the report an inflated estimate, as a startup history
// that counted outages does.
func stretchDeadline(c *Controller, d time.Duration) {
	report := autoReport(c)
	report.RecoveryDeadline = contractTimestamp(lastStageReceipt(report).Add(d))
	c.currentIncidentRequest.ResponseDeadline = lastStageReceipt(report).Add(time.Hour)
}

// setRollout makes Deployment api rolled out with its pod Ready, or not.
func setRollout(t *testing.T, world *settlingWorld, ready bool, conditions ...appsv1.DeploymentCondition) {
	t.Helper()
	api, err := world.client.AppsV1().Deployments("app").Get(t.Context(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	status := corev1.ConditionFalse
	api.Status.AvailableReplicas, api.Status.Conditions = 0, conditions
	if ready {
		api.Status.AvailableReplicas, status = 1, corev1.ConditionTrue
	}
	if _, err := world.client.AppsV1().Deployments("app").UpdateStatus(t.Context(), api, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	controller := true
	pod, err := world.client.CoreV1().Pods("app").Get(t.Context(), "api-pod", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: "Deployment", Name: "api", Controller: &controller}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status, LastTransitionTime: metav1.NewTime(world.clock.Now())}}
	if _, err := world.client.CoreV1().Pods("app").UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.client.CoreV1().Pods("app").Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func alwaysFails(time.Duration) bool { return true }

// horizon is when a test that never recovers hands off after the settle.
func horizon(test *buyTest) time.Duration {
	return test.window(test.start)["shop"].Workload.ScenarioHorizon("buy") + 5*time.Second
}

// The writes settled and the user test fails over a full window of samples
// after that: the agent starts, once.
func TestAUserTestThatFailsAFullWindowAfterTheSettleHandsOff(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	at, ok := handoffAfter(t, c, world, test.start, 90*time.Second)
	report := autoReport(c)
	if want := horizon(test); !ok || at < want || at > want+3*time.Second {
		t.Fatalf("handoff = %v at %s, want %s", ok, at, want)
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

// What is not failing evidence, while the alert itself stays active.
func TestNoFreshFailureIsNoHandoff(t *testing.T) {
	before := func(limit time.Duration) func(time.Duration) bool {
		return func(at time.Duration) bool { return at < limit }
	}
	for name, test := range map[string]*buyTest{
		"users recovered, the alert clears slowly": {failing: before(0)},
		"the application held the failure a while": {failing: before(30 * time.Second)},
		"failures then no samples":                 {failing: alwaysFails, silent: func(at time.Duration) bool { return at > 9*time.Second }},
		"too few samples":                          {failing: alwaysFails, silent: func(at time.Duration) bool { return at >= 9*time.Second && at < 90*time.Second }},
		"recovered just now":                       {failing: before(36 * time.Second)},
		"an isolated failure now and then":         {failing: func(at time.Duration) bool { return at%(20*time.Second) == 0 }},
		"every 120 seconds":                        {failing: alwaysFails, every: 2 * time.Minute},
		"an unproven test":                         {failing: alwaysFails, unproven: true},
	} {
		t.Run(name, func(t *testing.T) {
			c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
			bound := autoReport(c).GetRecoveryDeadline().AsTime().Sub(test.start)
			if at, ok := handoffAfter(t, c, world, test.start, bound-2*time.Second); ok {
				t.Fatalf("handed off at %s, bound %s", at, bound)
			}
			if at, ok := handoffAfter(t, c, world, test.start, bound+3*time.Second); !ok {
				t.Fatalf("the bound did not hand off by %s", at)
			}
		})
	}
}

func TestAFailingSampleNeedsTheAlertFiringNow(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.detectors[buyTestID].(*stuckUserTest).unavailable = true
	if at, ok := handoffAfter(t, c, world, test.start, 90*time.Second); ok {
		t.Fatalf("handed off at %s", at)
	}
}

// Probe case: a restarted pod boots for 180 s and users recover with it.
func TestABootingPodIsNotASettledWrite(t *testing.T) {
	test := &buyTest{failing: func(at time.Duration) bool { return at < 180*time.Second }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	setRollout(t, world, false)
	if at, ok := handoffAfter(t, c, world, test.start, 180*time.Second); ok {
		t.Fatalf("handed off at %s", at)
	}
	setRollout(t, world, true)
	// Settled at 180 s: the whole wait follows it.
	if at, ok := handoffAfter(t, c, world, test.start, 180*time.Second+90*time.Second); ok {
		t.Fatalf("handed off at %s", at)
	}
}

// Probe case: a healthy rollout that ends at 6 minutes, with the estimate long passed.
func TestAHealthyRolloutIsNotCutShort(t *testing.T) {
	test := &buyTest{failing: func(at time.Duration) bool { return at < 6*time.Minute }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stretchDeadline(c, 95*time.Second)
	setRollout(t, world, false)
	if at, ok := handoffAfter(t, c, world, test.start, 6*time.Minute); ok {
		t.Fatalf("handed off at %s", at)
	}
	setRollout(t, world, true)
	at, ok := handoffAfter(t, c, world, test.start, 10*time.Minute)
	if want := 6*time.Minute + 95*time.Second; !ok || at < want-time.Second || at > want+2*time.Second {
		t.Fatalf("handoff = %v at %s, want %s", ok, at, want)
	}
}

// Probe case: the writes settled long ago; one unready observation, or a lost
// one, changes nothing.
func TestALateUnsettledObservationDoesNotHandOff(t *testing.T) {
	test := &buyTest{failing: func(time.Duration) bool { return false }}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stretchDeadline(c, 28*time.Minute)
	stepSeconds(t, c, world, 400)
	settled := c.stageSettledAt
	setRollout(t, world, false)
	c.loseAutomaticObservation(world.clock.Now())
	stepSeconds(t, c, world, 1)
	setRollout(t, world, true)
	stepSeconds(t, c, world, 1)
	if autoReport(c).GetHandoffSelected() || settled.IsZero() || !c.stageSettledAt.Equal(settled) {
		t.Fatalf("handoff = %v, settled %s then %s", autoReport(c).GetHandoffSelected(), settled, c.stageSettledAt)
	}
	// No new evidence: the cap runs from the settle.
	at, ok := handoffAfter(t, c, world, test.start, 20*time.Minute)
	if want := settled.Sub(test.start) + stageSettledBound; !ok || at < want || at > want+2*time.Second {
		t.Fatalf("handoff = %v at %s, want %s", ok, at, want)
	}
}

// A rollout that flaps before it settled, or a lost observation during it,
// hands off neither at the estimate nor by itself.
func TestALostObservationDuringARolloutDoesNotHandOff(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stretchDeadline(c, 95*time.Second)
	setRollout(t, world, false)
	stepSeconds(t, c, world, 120)
	c.loseAutomaticObservation(world.clock.Now())
	setRollout(t, world, true)
	stepSeconds(t, c, world, 3)
	setRollout(t, world, false)
	stepSeconds(t, c, world, 60)
	if autoReport(c).GetHandoffSelected() || !c.stageSettledAt.IsZero() {
		t.Fatalf("handoff = %v, settled %s", autoReport(c).GetHandoffSelected(), c.stageSettledAt)
	}
}

// Kubernetes gave up on the stage's rollout: the agent takes over.
func TestARolloutKubernetesGaveUpOnHandsOff(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stretchDeadline(c, 28*time.Minute)
	setRollout(t, world, false)
	if at, ok := handoffAfter(t, c, world, test.start, 5*time.Minute); ok {
		t.Fatalf("handed off at %s", at)
	}
	setRollout(t, world, false, appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"})
	if at, ok := handoffAfter(t, c, world, test.start, 5*time.Minute+2*time.Second); !ok {
		t.Fatalf("no handoff by %s", at)
	}
	if exit := autoReport(c).GetRecoveryWait().GetExitReason(); exit != contracts.RecoveryWait_EXIT_REASON_DEADLINE {
		t.Fatalf("exit = %s", exit)
	}
}

// wait and escalate keep the bound they computed, whatever the evidence.
func TestAFailingUserTestLeavesTheOtherPoliciesAlone(t *testing.T) {
	for _, policy := range []string{settings.AfterPlaybookWait, settings.AfterPlaybookEscalate} {
		test := &buyTest{failing: alwaysFails}
		c, world, _ := freshWorld(t, test, policy)
		stretchDeadline(c, 15*time.Minute)
		setRollout(t, world, false, appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"})
		stepSeconds(t, c, world, 15*60-1)
		if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
			t.Fatalf("%s: ended early at %s", policy, c.escalatedAt)
		}
		stepSeconds(t, c, world, 2)
		if c.escalatedAt.IsZero() {
			t.Fatalf("%s: did not end at its bound", policy)
		}
	}
}

// What a write rolls out: a written workload, and a deleted pod's replacement,
// but not the workload of a pod it only wrote into.
func TestAWritesRollouts(t *testing.T) {
	written := verifyT0
	controller := true
	pod := func(created time.Time, ready corev1.ConditionStatus) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "store-0", Namespace: "app", CreationTimestamp: metav1.NewTime(created), OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "store", Controller: &controller}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready, LastTransitionTime: metav1.NewTime(written.Add(20 * time.Second))}}}}
	}
	set := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Status: appsv1.StatefulSetStatus{ReadyReplicas: 1, UpdatedReplicas: 1}}
	down := []sdk.Finding{{PrimaryResource: sdk.ObjectRef{Kind: "StatefulSet", Name: "store"}}}
	for name, tc := range map[string]struct {
		kind, object string
		pods         []corev1.Pod
		conditions   []sdk.Finding
		after        time.Duration
		ready, gave  bool
	}{
		"replacement booting":         {kind: "Pod", object: "store-0", pods: []corev1.Pod{pod(written.Add(time.Second), corev1.ConditionFalse)}},
		"replacement ready":           {kind: "Pod", object: "store-0", pods: []corev1.Pod{pod(written.Add(time.Second), corev1.ConditionTrue)}, ready: true},
		"replacement not made":        {kind: "Pod", object: "store-0", ready: true},
		"only written into":           {kind: "Pod", object: "store-0", pods: []corev1.Pod{pod(written.Add(-time.Hour), corev1.ConditionFalse)}, ready: true},
		"workload's pod booting":      {kind: "StatefulSet", object: "store", pods: []corev1.Pod{pod(written.Add(time.Second), corev1.ConditionFalse)}},
		"an old condition":            {kind: "StatefulSet", object: "store", pods: []corev1.Pod{pod(written.Add(time.Second), corev1.ConditionFalse)}, conditions: down, after: 10 * time.Second},
		"past Kubernetes' self-heal":  {kind: "StatefulSet", object: "store", pods: []corev1.Pod{pod(written.Add(time.Second), corev1.ConditionFalse)}, conditions: down, after: 10 * time.Minute, gave: true},
		"the old pod still there":     {kind: "StatefulSet", object: "store", pods: []corev1.Pod{pod(written.Add(-time.Hour), corev1.ConditionFalse)}, conditions: down, after: 10 * time.Second},
		"a pod made just now":         {kind: "StatefulSet", object: "store", pods: []corev1.Pod{pod(written.Add(10*time.Minute-time.Second), corev1.ConditionFalse)}, conditions: down, after: 10 * time.Minute},
		"a condition of another kind": {kind: "StatefulSet", object: "store", pods: []corev1.Pod{pod(written.Add(time.Second), corev1.ConditionFalse)}, conditions: []sdk.Finding{{PrimaryResource: sdk.ObjectRef{Kind: "StatefulSet", Name: "other"}}}, after: 10 * time.Minute},
		"no workload":                 {kind: "ConfigMap", object: "store", ready: true},
	} {
		t.Run(name, func(t *testing.T) {
			current := set
			if name == "replacement not made" {
				current.Status.ReadyReplicas = 0
				tc.ready = false
			}
			observed := written.Add(tc.after)
			view := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &observed, PodList: tc.pods, StatefulSetList: []appsv1.StatefulSet{current}}
			report := &contracts.StageReport{Mitigations: []*contracts.StageMitigation{{
				ApplyReceipt:   &contracts.PlanApplication{Applied: true, StartedAt: contractTimestamp(written), CompletedAt: contractTimestamp(written)},
				TouchedObjects: []*contracts.ObjectRef{{Kind: tc.kind, Namespace: "app", Name: tc.object}},
			}}}
			ready, _, gave := observeStageRollouts(view, report, tc.conditions)
			if ready != tc.ready || gave != tc.gave {
				t.Fatalf("ready = %v, gave up = %v", ready, gave)
			}
		})
	}
}

// An early handoff whose agent changed nothing leaves the credit to the
// program; a reported change before recovery, or a handoff at the deadline,
// still takes it.
func TestAnEarlyHandoffWithoutAnAgentWriteKeepsTheCredit(t *testing.T) {
	recovered := verifyT0.Add(time.Minute)
	app := &contracts.PlanApplication{ApplicationId: "a", Applied: true, StartedAt: contractTimestamp(verifyT0), CompletedAt: contractTimestamp(verifyT0)}
	report := func(exit contracts.RecoveryWait_ExitReason, keys ...string) *contracts.StageReport {
		return &contracts.StageReport{HandoffSelected: true, HandoffKeys: keys, RecoveryWait: &contracts.RecoveryWait{ExitReason: exit}, Mitigations: []*contracts.StageMitigation{{ApplyReceipt: app}}}
	}
	change := []*contracts.AgentChange{{At: contractTimestamp(verifyT0.Add(30 * time.Second))}}
	for name, tc := range map[string]struct {
		report  *contracts.StageReport
		changes []*contracts.AgentChange
		want    string
	}{
		"no agent write":  {report(contracts.RecoveryWait_EXIT_REASON_UNCOVERED, "k"), nil, "a"},
		"an agent write":  {report(contracts.RecoveryWait_EXIT_REASON_UNCOVERED, "k"), change, ""},
		"a page":          {report(contracts.RecoveryWait_EXIT_REASON_UNCOVERED), nil, ""},
		"at the deadline": {report(contracts.RecoveryWait_EXIT_REASON_DEADLINE, "k"), nil, ""},
	} {
		got := ""
		for id := range automaticCreditApplications(tc.report, []*contracts.PlanApplication{app}, tc.changes, recovered) {
			got = id
		}
		if got != tc.want {
			t.Errorf("%s: credit = %q, want %q", name, got, tc.want)
		}
	}
}

// A restart reads the settle from the cluster: the wait is not replenished.
func TestARestartKeepsTheSettle(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stepSeconds(t, c, world, 20)
	c.stageSettledAt = time.Time{}
	at, ok := handoffAfter(t, c, world, test.start, 90*time.Second)
	if want := horizon(test); !ok || at > want+3*time.Second {
		t.Fatalf("handoff = %v at %s, want %s", ok, at, want)
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

// The next incident starts without this one's settle.
func TestTheSettleEndsWithTheIncident(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stepSeconds(t, c, world, 10)
	if c.stageSettledAt.IsZero() {
		t.Fatal("the writes did not settle")
	}
	c.stageUnsettled, c.stageGaveUp = true, true
	c.clearIncidentLocked()
	if !c.stageSettledAt.IsZero() || c.stageUnsettled || c.stageGaveUp {
		t.Fatal("the settle outlived its incident")
	}
}

// A later write starts the window over even when nothing showed unsettled.
func TestALaterWriteStartsTheWindowOver(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	stretchDeadline(c, 5*time.Minute)
	stepSeconds(t, c, world, 30)
	c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, RepairActionReceipt{ActionID: "synthetic", Success: true, CompletedAt: world.clock.Now()})
	at, ok := handoffAfter(t, c, world, test.start, 2*time.Minute)
	if want := 30*time.Second + horizon(test); !ok || at < want {
		t.Fatalf("handoff = %v at %s, want after %s", ok, at, want)
	}
}
