//go:build integration

package runtime

import (
	"context"
	"errors"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/kubernetes/fake"
	"path/filepath"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
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
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	loop.MutationGate = gate
	c.responderLoop = loop
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
			gate := c.responderLoop.MutationGate
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
					loop := NewResponderLoop(r, nil, p, []sdk.Detector{trafficDetector("traffic-health", "health")}, PlanExecutor{}, 5)
					loop.MutationGate = gate
					loop.IncidentStatus = func() *apihistory.IncidentVerified { return c.incidentVerificationWarning(r.IncidentID) }
					loop.CurrentEvidence = func() *IncidentRequest { return cloneIncidentRequest(c.currentIncidentRequest) }
					return loop
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
				lease, err := gate.Begin()
				if err != nil {
					t.Fatalf("residual responder is still fenced after recovery was withdrawn: %v", err)
				}
				if err := lease.Finish(false); err != nil {
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

// No reflection worker or final responder report delays this native pipeline.
// Recovery, an intervening red window and renewed proof all run through the
// real controller before the native broker may publish its model-free skip.
func TestRecoveryTrafficEarlyClosureSkipsReflectionOnlyAfterHealthProof(t *testing.T) {
	ctx := context.Background()
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	p := &changingProvider{}
	c.provider = p
	trafficHealth := trafficDetector("traffic-health", "health")
	c.detectors["traffic-health"] = trafficHealth
	c.detectorSpecs["traffic-health"] = trafficHealth.Spec()
	c.healthDetectorIDs = append(c.healthDetectorIDs, "traffic-health")
	policy := trafficHealth.Spec().Persistence
	c.tracker.SetPolicy("traffic-health", policy.Firing, policy.Clearing, policy.MinDuration)
	c.scheduler = NewScheduler(c.detectorList(), time.Unix(2, 0))
	now := time.Unix(2, 0).UTC()
	c.now = func() time.Time { return now }
	set := func(second int, latency time.Duration) {
		now = time.Unix(int64(second), 0).UTC()
		snapshot := recoveryTrafficSnapshot(t, now, latency)
		snapshot.NamespaceName = c.config.Namespace
		p.snapshot = snapshot
	}
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), simMemoryStructureValidator)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none", Now: func() time.Time { return now }}
	workspace, err := native.PrepareIncident(ctx, c.currentIncidentRequest.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	id := c.currentIncidentRequest.IncidentID
	c.currentIncidentRequest.RepairPolicy = "recorded-actions"
	c.currentIncidentRequest.RepositoryWorktree = workspace.Worktree
	c.currentIncidentRequest.RepositoryBaseCommit = workspace.BaseCommit
	c.currentIncidentRequest.SourceCommit = workspace.BaseCommit
	c.currentIncidentRequest.DeployedCommit = workspace.BaseCommit
	c.broker = native
	for second := 2; second < 36; second++ {
		latency := 100 * time.Millisecond
		if second <= 20 {
			latency = 330 * time.Millisecond
		}
		set(second, latency)
		step(second)
		if !c.IncidentOpen() {
			t.Fatalf("early/model-free path closed while traffic was settling at %d", second)
		}
	}
	set(36, 100*time.Millisecond)
	step(36)
	if c.IncidentOpen() {
		t.Fatal("clear traffic never produced recovery")
	}
	set(37, 330*time.Millisecond)
	step(37)
	executeRecheck(t, c)
	if c.closureFailure != nil || c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING {
		t.Fatal("fresh red traffic became a permanent native closure failure")
	}
	if got := c.cachedResponderVerification(ctx, id, 0); got.Healthy || got.IncidentVerified != nil {
		t.Fatal("verify retained the invalidated recovery proof")
	}
	for _, second := range []int{38, 52} {
		set(second, 100*time.Millisecond)
		executeRecheck(t, c)
		if c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING {
			t.Fatal("native outcome skipped renewed stability")
		}
	}
	set(53, 100*time.Millisecond)
	executeRecheck(t, c)
	awaitBrokerResult(t, c.closureResults)
	completion := <-c.closureResults
	if completion.err != nil {
		t.Fatal(completion.err)
	}
	outcome, err := memory.NewJSONStore(root).Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.GetReflectionSkipReason() != "no_session" {
		t.Fatalf("expected model-free reflection skip: %v", outcome.Record)
	}
	ledger, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.ReflectionDecision.GetNoChangeReason() != "no_session" || !ledger.Closure.GetVerifiedAt().AsTime().Equal(now) {
		t.Fatal("native skip preceded fresh stable health proof")
	}
}

func TestRecoveryFrozenClosureAtHealthBudgetPublishesFailureWithoutRewritingCommand(t *testing.T) {
	for _, scenario := range []struct {
		name                                                       string
		sameFinding, sourcePending, lateClear, observerUnavailable bool
	}{{name: "new-finding"}, {name: "same-finding-new-episode", sameFinding: true}, {name: "unpublished-source", sourcePending: true}, {name: "late-clear-after-budget", lateClear: true}, {name: "observer-unavailable", observerUnavailable: true}} {
		t.Run(scenario.name, func(t *testing.T) {
			sameFinding := scenario.sameFinding
			ctx := context.Background()
			c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			interrupted := true
			repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), func(ctx context.Context, root string, changed []string) error {
				if scenario.sourcePending && interrupted {
					return errors.New("synthetic interruption before source acceptance")
				}
				return simMemoryStructureValidator(ctx, root, changed)
			})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(2, 0).UTC()
			c.now = func() time.Time { return now }
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "scripted", ResponderModel: "none", Now: func() time.Time { return now }, Deriver: OutcomeDeriverFunc(func(ctx context.Context, closure IncidentClosure, proposal, backend, model string) (DerivedOutcome, error) {
				if interrupted {
					return DerivedOutcome{}, errors.New("synthetic exit before outcome")
				}
				return (ProtoOutcomeDeriver{}).Derive(ctx, closure, proposal, backend, model)
			})}
			workspace, err := native.PrepareIncident(ctx, c.currentIncidentRequest.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario.sourcePending {
				tracingWrite(t, workspace.Worktree, "app.txt", "unpublished repair\n")
			}
			id := c.currentIncidentRequest.IncidentID
			c.currentIncidentRequest.RepairPolicy = "recorded-actions"
			c.currentIncidentRequest.RepositoryWorktree = workspace.Worktree
			c.currentIncidentRequest.RepositoryBaseCommit = workspace.BaseCommit
			c.currentIncidentRequest.SourceCommit = workspace.BaseCommit
			c.currentIncidentRequest.DeployedCommit = workspace.BaseCommit
			c.broker = native
			for second := 2; second <= 18; second++ {
				now = time.Unix(int64(second), 0).UTC()
				step(second)
			}
			executeRecheck(t, c)
			awaitBrokerResult(t, c.closureResults)
			c.processBrokerCompletions()
			ledger, err := native.readLedger(id)
			if err != nil {
				t.Fatal(err)
			}
			frozen := proto.Clone(ledger.Closure)
			if !c.pendingClosure.Frozen || c.closureFailure == nil || c.closureFailure.Permanent {
				t.Fatal("fixture did not freeze and interrupt before outcome")
			}
			interrupted = false
			var detector sdk.Detector = trafficDetector("health", "health")
			if sameFinding {
				detector = &namedRecoveryDetector{windowDetector: &windowDetector{confirmed: true, active: true}, id: "health"}
			}
			c.detectors["health"] = detector
			c.detectorSpecs["health"] = detector.Spec()
			c.tracker.SetPolicy("health", 2, 2, 9*time.Second)
			p := &changingProvider{}
			c.provider = p
			now = time.Unix(20, 0).UTC()
			p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
			if scenario.observerUnavailable {
				window := p.snapshot.(sdktest.Snapshot).Traffic["health"]
				window.LoadError = "synthetic prober unavailable"
				p.snapshot.(sdktest.Snapshot).Traffic["health"] = window
			}
			executeRecheck(t, c)
			if c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING || c.closureFailure.Permanent || c.closureFailure.Attempts != 1 {
				t.Fatal("active health spent the broker retry budget")
			}
			if scenario.lateClear {
				// Detector observations continue while broker delivery is delayed.
				// A new streak after the bound cannot retroactively earn recovery.
				c.scheduler = NewScheduler(c.detectorList(), now)
				for _, second := range []int{70, 85} {
					now = time.Unix(int64(second), 0).UTC()
					p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
					if err := c.Step(ctx, now, nil); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				now = c.closureVerificationDeadline(*c.pendingClosure)
				p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
			}
			if scenario.observerUnavailable {
				window := p.snapshot.(sdktest.Snapshot).Traffic["health"]
				window.LoadError = "synthetic prober unavailable"
				p.snapshot.(sdktest.Snapshot).Traffic["health"] = window
			}
			executeRecheck(t, c)
			if c.closureState != contracts.ClosureState_CLOSURE_STATE_PROCESSING {
				t.Fatal("frozen red closure waited past its original health budget instead of finalizing failed")
			}
			awaitBrokerResult(t, c.closureResults)
			c.processBrokerCompletions()
			if c.closureReceipt == nil || c.closureFailure != nil {
				t.Fatalf("expired health did not get a normal failure receipt: %+v", c.closureFailure)
			}
			ledger, err = native.readLedger(id)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(ledger.Closure, frozen) {
				t.Fatal("bounded failure rewrote the immutable broker command")
			}
			outcome, err := memory.NewJSONStore(root).Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if scenario.sourcePending && (ledger.ProposalCommit != "" || nativeGit(t, root, "show", "HEAD:app.txt") != "base") {
				t.Fatal("expired health published an unaccepted source repair")
			}
			expected := "failed"
			if scenario.lateClear || scenario.observerUnavailable {
				expected = "unconfirmed"
			}
			if outcome.Record.GetClassification() != expected {
				t.Fatalf("red traffic received success: %v", outcome.Record)
			}
			if scenario.lateClear || scenario.observerUnavailable {
				return
			}
			if len(c.batcher.Snapshot().Findings) == 0 {
				t.Fatal("failure did not retain active residual work for the next responder")
			}
			before := proto.Clone(outcome.Record)
			replay, err := native.processUnverifiedClosure(ctx, incidentClosureState(ledger.Closure), closureHealthObservation{At: now, Reason: "later unavailable read"})
			if err != nil || replay.OutcomeCommit != c.closureReceipt.OutcomeCommit {
				t.Fatalf("accepted outcome replay failed: %v", err)
			}
			after, err := memory.NewJSONStore(root).Get(ctx, id)
			if err != nil || !proto.Equal(before, after.Record) {
				t.Fatal("later health rewrote an accepted outcome")
			}
			ack, ok := c.PendingClosureAcknowledgmentEffect()
			if !ok {
				t.Fatal("missing failed-outcome acknowledgment")
			}
			if err := c.ExecuteClosureAcknowledgmentEffect(ctx, ack); err != nil {
				t.Fatal(err)
			}
			awaitBrokerResult(t, c.acknowledgmentResults)
			c.processBrokerCompletions()
			if c.pendingClosure != nil {
				t.Fatal("failure receipt did not acknowledge")
			}
			residual := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
			c.dispatcher = residual
			now = now.Add(time.Second)
			c.scheduler = NewScheduler(c.detectorList(), now)
			p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID == id || len(c.currentIncidentRequest.Findings) == 0 {
				t.Fatal("acknowledgment dropped a post-proof health episode instead of handing it to a responder")
			}
			work, ok := c.PendingWorkspaceEffect()
			if !ok {
				t.Fatal("residual did not reach the native workspace handoff")
			}
			if err := c.ExecuteWorkspaceEffect(ctx, work); err != nil {
				t.Fatal(err)
			}
			awaitBrokerResult(t, c.workspaceResults)
			c.processBrokerCompletions()
			effect, ok := c.PendingDispatchEffect()
			if !ok {
				t.Fatal("native residual workspace did not release responder")
			}
			if _, _, err := c.runResponsePipeline(ctx, effect.Request); err != nil {
				t.Fatal(err)
			}
			if received := awaitRequest(t, residual.requests); len(received.Findings) == 0 {
				t.Fatal("responder lost the residual health episode")
			}

		})
	}

}

func TestRecoverySourceInformationCannotBypassRenewedHealthWindow(t *testing.T) {
	c, p, start := repairedTrafficController(t)
	c.config.VerificationTimeout = 90 * time.Second
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
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, View: func(s sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}
	}})
	now = start.Add(16 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	recheck := c.revalidateClosure(context.Background(), p.snapshot, *c.pendingClosure)
	if !recheck.SourceOnly {
		t.Fatalf("fixture did not produce informational source wait: %+v", recheck)
	}
	executeRecheck(t, c)
	assertStillPending(t, c, broker)
	now = start.Add(45 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
	executeRecheck(t, c)
	assertStillPending(t, c, broker)
	now = start.Add(46 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	executeRecheck(t, c)
	assertStillPending(t, c, broker)
	if response := c.cachedResponderVerification(context.Background(), c.pendingClosure.Request.IncidentID, 0); response.Healthy || response.Summary == "" {
		t.Fatal("renewed proof wait was hidden or read as healthy")
	}
	now = start.Add(60 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	executeRecheck(t, c)
	assertStillPending(t, c, broker)
	now = start.Add(61 * time.Second)
	p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
	executeRecheck(t, c)
	awaitBrokerResult(t, c.closureResults)
	if broker.processCalls() != 1 {
		t.Fatal("source information started another health stability window")
	}
}

func TestRecoveryResidualCannotDispatchPastOriginalResponseBudget(t *testing.T) {
	c, p, start := repairedTrafficController(t)
	c.currentIncidentRequest.ResponseDeadline = start.Add(time.Minute)
	if err := c.SetIncidentBroker(&recordingClosureBroker{}); err != nil {
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
	now = start.Add(time.Minute)
	p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
	executeRecheck(t, c)
	if err := c.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("residual handoff reset an exhausted original response budget")
	}
	closure, ok := c.PendingIncidentClosure()
	if !ok || len(closure.FinalDetectorStates) > 0 || c.closureFailure != nil {
		t.Fatal("exhausted response budget did not produce an ordinary unverified closure")
	}
}
