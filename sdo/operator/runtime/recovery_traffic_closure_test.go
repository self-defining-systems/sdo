package runtime

import (
	"context"
	"errors"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/contracts/wire"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func recoveryTrafficSnapshot(t *testing.T, now time.Time, latency time.Duration) sdktest.Snapshot {
	t.Helper()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","scenarios":[{"id":"read","slo":{"latencyPercentile":50,"maxLatency":"250ms"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	samples := make([]traffic.Sample, 5)
	for i := range samples {
		samples[i] = traffic.Sample{At: now.Add(time.Duration(i-4) * time.Second), Outcome: traffic.OutcomeOK, Status: 200, Latency: latency}
	}
	return sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {Workload: workload, ObservedAt: now, Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "read", Target: traffic.Target{Service: "api", Port: 80}}, Qualified: true, Samples: samples}}}}}
}

// Real program execution produces the repair result; controller observation,
// proof publication and broker recheck consume the same traffic detector.
func repairedTrafficController(t *testing.T) (*Controller, *changingProvider, time.Time) {
	t.Helper()
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	state.Request.ResponseDeadline = now.Add(5 * time.Minute)
	result, err := responsePipelineResult(state, now.Add(-time.Second), now)
	if err != nil {
		t.Fatal(err)
	}
	provider := &changingProvider{snapshot: recoveryTrafficSnapshot(t, now, 100*time.Millisecond)}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	cfg.VerificationTimeout = time.Minute
	c, err := NewController(cfg, []sdk.Detector{trafficDetector("traffic-health", "health")}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest = cloneIncidentRequest(&state.Request)
	c.currentIncidentRequest.PriorStages = cloneStageReports(state.PriorStages)
	c.incidentOpen = true
	c.incidentDispatchedAt = now.Add(-time.Second)
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.handleDispatchCompletion(dispatchCompletion{incidentID: state.Request.IncidentID, result: result, priorStages: state.PriorStages}, now)
	c.now = func() time.Time { return now }
	return c, provider, now
}

func TestRecoveryProofRequiresFreshTrafficAtPublication(t *testing.T) {
	c, p, start := repairedTrafficController(t)
	step := func(seconds int, latency time.Duration) {
		t.Helper()
		now := start.Add(time.Duration(seconds) * time.Second)
		p.snapshot = recoveryTrafficSnapshot(t, now, latency)
		if err := c.Step(context.Background(), now, nil); err != nil {
			t.Fatal(err)
		}
	}
	step(0, 100*time.Millisecond)
	step(10, 100*time.Millisecond)
	// No scheduled sample is due at 15s. A cached green must not publish a
	// proof over a fresh red window, even on the model-free completion path.
	step(15, 330*time.Millisecond)
	if _, ok := c.PendingIncidentClosure(); ok {
		t.Fatal("published recovery from an older green traffic epoch")
	}
	if !c.IncidentOpen() {
		t.Fatal("traffic settling lost incident ownership")
	}
	for _, seconds := range []int{16, 20, 30} {
		step(seconds, 100*time.Millisecond)
		if _, ok := c.PendingIncidentClosure(); ok {
			t.Fatalf("published before renewed stability at %ds", seconds)
		}
	}
	step(31, 100*time.Millisecond)
	closure, ok := c.PendingIncidentClosure()
	if !ok || len(closure.FinalDetectorStates) != 1 {
		t.Fatalf("fresh traffic did not close after stability: %+v", closure)
	}
	if got := c.revalidateClosure(context.Background(), p.snapshot, closure); got.Verdict != closureRecheckConfirmed {
		t.Fatalf("proof/recheck disagree: %+v", got)
	}
}

func TestClosureActiveTrafficWaitsThenRecoversOrHandsOff(t *testing.T) {
	for _, clears := range []bool{true, false} {
		t.Run(map[bool]string{true: "settles", false: "residual"}[clears], func(t *testing.T) {
			c, p, start := repairedTrafficController(t)
			broker := &recordingClosureBroker{}
			if err := c.SetIncidentBroker(broker); err != nil {
				t.Fatal(err)
			}
			now := start
			c.now = func() time.Time { return now }
			for _, seconds := range []int{0, 10, 15} {
				now = start.Add(time.Duration(seconds) * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				if err := c.Step(context.Background(), now, nil); err != nil {
					t.Fatal(err)
				}
			}
			if c.IncidentOpen() {
				t.Fatal("fixture did not produce the real recovery proof")
			}
			now = start.Add(16 * time.Second)
			p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
			executeRecheck(t, c)
			assertStillPending(t, c, broker)
			if clears {
				now = start.Add(17 * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, c)
				assertStillPending(t, c, broker)
				now = start.Add(32 * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, c)
				awaitBrokerResult(t, c.closureResults)
				c.processBrokerCompletions()
				if broker.processCalls() != 1 || c.closureFailure != nil {
					t.Fatal("settled traffic did not commit")
				}
			} else {
				now = start.Add(time.Minute)
				dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: t.TempDir(), ServiceAccount: "responder", LoopFactory: func(r IncidentRequest) *ResponderLoop {
					return NewResponderLoop(r, nil, p, []sdk.Detector{trafficDetector("traffic-health", "health")}, PlanExecutor{}, 5)
				}}
				c.dispatcher = dispatcher
				p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
				executeRecheck(t, c)
				if c.closureFailure != nil || !c.IncidentOpen() {
					t.Fatalf("red traffic became permanent failure instead of residual: %+v", c.closureFailure)
				}
				if err := c.Step(context.Background(), now, nil); err != nil {
					t.Fatal(err)
				}
				effect, ok := c.PendingDispatchEffect()
				if !ok {
					t.Fatal("unrecovered automatic repair did not hand residual to responder")
				}
				task := new(contracts.AgentTask)
				if err := wire.UnmarshalDurable([]byte(effect.Request.PreparedAgentTask), task); err != nil {
					t.Fatal(err)
				}
				if err := dispatcher.Registry.ValidateTask(task); err != nil {
					t.Fatal(err)
				}
				typed := new(contracts.IncidentRequest)
				if err := task.GetRequest().UnmarshalTo(typed); err != nil {
					t.Fatal(err)
				}
				if len(typed.GetFindings()) != 1 || typed.GetFindings()[0].GetDetectorId() != "traffic-health" || len(typed.GetPriorStages()) == 0 || !typed.GetResponseDeadline().AsTime().Equal(c.currentIncidentRequest.ResponseDeadline) {
					t.Fatalf("real worker task lost residuals, receipts or budget: %v", typed)
				}
			}
		})
	}
}

// Every health ID and non-clear status shares the same fresh-observation rule;
// no traffic-name exception, reflection delay, or scheduled old green is proof.
func TestRecoveryFreshHealthSweepBeforeEarlyClosure(t *testing.T) {
	for _, id := range []string{"traffic-health", "health-objective", "other-health"} {
		for _, status := range []string{"clear", "active", "pending", "error"} {
			t.Run(id+"/"+status, func(t *testing.T) {
				c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
				peer := &namedRecoveryDetector{windowDetector: &windowDetector{confirmed: true}, id: id}
				c.detectors[id] = peer
				c.healthDetectorIDs = append(c.healthDetectorIDs, id)
				c.scheduler = NewScheduler(c.detectorList(), time.Unix(2, 0))
				for i := 2; i < 17; i++ {
					step(i)
				}
				switch status {
				case "active":
					peer.set(true, true)
				case "pending":
					peer.set(false, false)
				case "error":
					peer.err = errors.New("synthetic observer unavailable")
				}
				step(17)
				if c.IncidentOpen() != (status != "clear") {
					t.Fatalf("early proof ignored fresh %s on %s", status, id)
				}
				if status != "clear" {
					if _, ok := c.PendingIncidentClosure(); ok {
						t.Fatal("early/model-free reflection path received a false recovery proof")
					}
				}
			})
		}
	}
}

type namedRecoveryDetector struct {
	*windowDetector
	id string
}

func (d *namedRecoveryDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Hour, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}
func (d *namedRecoveryDetector) Detect(ctx context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.windowDetector.Detect(ctx, s)
	for i := range findings {
		findings[i].DetectorID = d.id
	}
	return findings, err
}

func TestClosureTrafficRestartRetainsProofAndBudget(t *testing.T) {
	for _, clears := range []bool{true, false} {
		t.Run(map[bool]string{true: "settles", false: "residual"}[clears], func(t *testing.T) {
			c, p, start := repairedTrafficController(t)
			broker := &recordingClosureBroker{}
			if err := c.SetIncidentBroker(broker); err != nil {
				t.Fatal(err)
			}
			now := start
			c.now = func() time.Time { return now }
			for _, second := range []int{0, 10, 15} {
				now = start.Add(time.Duration(second) * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				if err := c.Step(context.Background(), now, nil); err != nil {
					t.Fatal(err)
				}
			}
			now = start.Add(16 * time.Second)
			p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
			executeRecheck(t, c)
			assertStillPending(t, c, broker)
			saved := exportState(t, c)
			now = start.Add(17 * time.Second)
			restored, err := NewController(c.config, c.detectorList(), p, c.dispatcher, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := restored.RestoreState(saved); err != nil {
				t.Fatal(err)
			}
			if err := restored.SetIncidentBroker(broker); err != nil {
				t.Fatal(err)
			}
			restored.now = func() time.Time { return now }
			if clears {
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, restored)
				assertStillPending(t, restored, broker)
				now = start.Add(31 * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, restored)
				assertStillPending(t, restored, broker)
				now = start.Add(32 * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, restored)
				awaitBrokerResult(t, restored.closureResults)
				if broker.processCalls() != 1 {
					t.Fatal("restart could not renew the shared recovery proof")
				}
			} else {
				now = start.Add(time.Minute)
				p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
				executeRecheck(t, restored)
				if !restored.IncidentOpen() || restored.closureFailure != nil || !restored.verificationDeadline.Equal(start.Add(time.Minute)) {
					t.Fatal("restart reset the original verification budget or permanently failed")
				}
			}
		})
	}
}

func TestRecoveryClearNearDeadlineDoesNotExtendVerificationBudget(t *testing.T) {
	c, p, start := repairedTrafficController(t)
	broker := &recordingClosureBroker{}
	if err := c.SetIncidentBroker(broker); err != nil {
		t.Fatal(err)
	}
	now := start
	c.now = func() time.Time { return now }
	for _, second := range []int{0, 10, 15} {
		now = start.Add(time.Duration(second) * time.Second)
		p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
		if err := c.Step(context.Background(), now, nil); err != nil {
			t.Fatal(err)
		}
	}
	now = start.Add(16 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
	executeRecheck(t, c)
	assertStillPending(t, c, broker)
	now = start.Add(59 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	executeRecheck(t, c)
	assertStillPending(t, c, broker)
	now = start.Add(time.Minute)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	executeRecheck(t, c)
	if !c.IncidentOpen() {
		t.Fatal("expired proof did not return to bounded verification")
	}
	if err := c.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	closure, ok := c.PendingIncidentClosure()
	if !ok || len(closure.FinalDetectorStates) != 0 || closure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || c.closureFailure != nil {
		t.Fatalf("near-deadline clear extended the budget or asserted recovery: %+v", closure)
	}
}

// A detector may retain its result slice. Controller normalization must not
// mutate the producer's evidence or race a concurrent read-only consumer.
func TestRecoveryObservationRetainsDetectorOwnedEvidence(t *testing.T) {
	finding := stateFinding("health")
	finding.DetectorID = ""
	finding.Fingerprint = ""
	detector := &retainedRecoveryDetector{namedRecoveryDetector: &namedRecoveryDetector{windowDetector: &windowDetector{confirmed: true}, id: "health"}, findings: []sdk.Finding{finding}}
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Step(context.Background(), time.Unix(0, 0), nil); err != nil {
		t.Fatal(err)
	}
	if detector.findings[0].DetectorID != "" || detector.findings[0].Fingerprint != "" {
		t.Fatal("normalization mutated the detector's retained evidence")
	}
}

type retainedRecoveryDetector struct {
	*namedRecoveryDetector
	findings []sdk.Finding
}

func (d *retainedRecoveryDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.findings, nil
}

func TestClosureTrafficSharesClearingPersistenceWithoutSerialWindow(t *testing.T) {
	c, p, start := repairedTrafficController(t)
	c.tracker.SetPolicy("traffic-health", 2, 20, 9*time.Second)
	broker := &recordingClosureBroker{}
	if err := c.SetIncidentBroker(broker); err != nil {
		t.Fatal(err)
	}
	now := start
	c.now = func() time.Time { return now }
	for _, second := range []int{0, 10, 15} {
		now = start.Add(time.Duration(second) * time.Second)
		p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
		if err := c.Step(context.Background(), now, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, second := range []int{16, 26} {
		now = start.Add(time.Duration(second) * time.Second)
		p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
		executeRecheck(t, c)
		assertStillPending(t, c, broker)
	}
	if !c.tracker.HasActiveDetector("traffic-health") {
		t.Fatal("fixture did not establish active traffic persistence")
	}
	for second := 27; second < 46; second++ {
		now = start.Add(time.Duration(second) * time.Second)
		p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
		executeRecheck(t, c)
		assertStillPending(t, c, broker)
	}
	now = start.Add(46 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	executeRecheck(t, c)
	awaitBrokerResult(t, c.closureResults)
	if broker.processCalls() != 1 {
		t.Fatal("broker started a second stability window after clearing persistence")
	}
}
