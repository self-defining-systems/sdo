package runtime

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type scriptedDriftResponder struct {
	client   *fake.Clientset
	requests chan IncidentRequest
}

func (s scriptedDriftResponder) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	s.requests <- r
	for _, f := range r.Findings {
		service, err := s.client.CoreV1().Services("app").Get(ctx, f.PrimaryResource.Name, metav1.GetOptions{})
		if err != nil {
			return IncidentResult{}, err
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
		if _, err := s.client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
			return IncidentResult{}, err
		}
	}
	return completedResult(r.IncidentID), nil
}

type serviceDriftView struct{ *sdktest.ReplayReadView }

func (v serviceDriftView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	for _, s := range v.Services() {
		if s.Name == ref.Name {
			return program.Diff{Changed: s.Spec.Ports[0].TargetPort.IntVal != 8080}, nil
		}
	}
	return program.Diff{}, fmt.Errorf("missing service %s", ref.Name)
}

type scenarioPortProgram struct{}

func (scenarioPortProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (scenarioPortProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, s := range v.Services() {
		if s.Name == b["service"].Name {
			if s.Spec.Ports[0].TargetPort.IntVal == 9999 {
				return program.Verdict{Result: program.Match}, nil
			}
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}

func newDriftScenario(t *testing.T, learned bool, healthInterval ...time.Duration) (*Controller, *fake.Clientset, *KubernetesCache, chan IncidentRequest, time.Time) {
	t.Helper()
	start := time.Unix(100, 0).UTC()
	first, peer := driftService(8080), driftService(8080)
	peer.Name = "peer"
	client := fake.NewSimpleClientset(first, peer)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan IncidentRequest, 2)
	config := testControllerConfig()
	config.Namespace = "app"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	health := controllerDetector("health", time.Second, sdk.Finding{})
	if len(healthInterval) > 0 {
		health.spec.Interval = healthInterval[0]
	}
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	health.spec.OriginatingCommit = "seed"
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	c, err := NewController(config, []sdk.Detector{health}, fastloopObjects{client}, scriptedDriftResponder{client, requests}, start)
	if err != nil {
		t.Fatal(err)
	}
	var registry program.ProgramRegistry
	if learned {
		registry, err = program.NewRegistry([]program.Diagnoser{scenarioPortProgram{}}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	c.ConfigureDrift(DriftControllerConfig{Events: cache, Programs: registry, ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, BatchWindow: time.Nanosecond, SweepInterval: time.Second, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return serviceDriftView{&sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}}
	}})
	if err := c.SetIncidentBroker(&recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/tmp/drift-proposal", BaseCommit: "source"}, receipt: ClosureReceipt{OutcomeCommit: "outcome", AckToken: "ack"}}); err != nil {
		t.Fatal(err)
	}
	return c, client, cache, requests, start
}
func injectScenarioPort(t *testing.T, client *fake.Clientset, cache *KubernetesCache, name string) {
	t.Helper()
	ctx := context.Background()
	before, err := client.CoreV1().Services("app").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	broken := before.DeepCopy()
	broken.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	if _, err := client.CoreV1().Services("app").Update(ctx, broken, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, broken)
}
func stepScenario(t *testing.T, c *Controller, cache *KubernetesCache, at time.Time) {
	t.Helper()
	if err := c.StepEvents(context.Background(), at, cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
}
func acknowledgeScenario(t *testing.T, c *Controller) {
	t.Helper()
	ctx := context.Background()
	effect, ok := c.PendingClosureEffect()
	if !ok {
		t.Fatal("missing closure")
	}
	if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, c.closureResults)
	c.processBrokerCompletions()
	ack, ok := c.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("missing acknowledgment")
	}
	if err := c.ExecuteClosureAcknowledgmentEffect(ctx, ack); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, c.acknowledgmentResults)
	c.processBrokerCompletions()
}

func TestHermeticDriftlive4NewFaultAfterHealthyBaseline(t *testing.T) {
	c, client, cache, requests, start := newDriftScenario(t, true)
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	dispatchScenario(t, c)
	first := awaitRequest(t, requests)
	if len(first.DriftEvents) != 1 || len(first.DriftEvents[0].Fields) != 1 {
		t.Fatal("periodic sweep replaced original field evidence")
	}
	awaitDispatchCompletionQueued(t, c)
	stepScenario(t, c, cache, start.Add(2*time.Second))
	if _, ok := c.PendingIncidentClosure(); !ok {
		t.Fatal("first repair did not close")
	}
	acknowledgeScenario(t, c)
	state := exportState(t, c)
	if len(state.FindingStates) == 0 || len(state.DriftEvents) == 0 {
		t.Fatal("earlier drift was not retained")
	}
	if err := c.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	for i := 3; i < 6; i++ {
		stepScenario(t, c, cache, start.Add(time.Duration(i)*time.Second))
		if c.IncidentOpen() {
			t.Fatal("driftlive4: stale retained drift opened an incident before injection")
		}
	}
	injectScenarioPort(t, client, cache, "peer")
	stepScenario(t, c, cache, start.Add(6*time.Second))
	dispatchScenario(t, c)
	second := awaitRequest(t, requests)
	if second.IncidentID == first.IncidentID || len(second.Findings) != 1 || second.Findings[0].PrimaryResource.Name != "peer" || second.Findings[0].ParameterBindings["service"].Name != "peer" || len(second.DriftEvents) != 1 || second.DriftEvents[0].Ref.Name != "peer" {
		t.Fatalf("new incident is not bound to new fault: %+v", second)
	}
	view := exportState(t, c).IncidentView
	if view == nil || len(view.DriftEvents) != 1 || view.DriftEvents[0].Ref.Name != "peer" {
		t.Fatalf("moving incident view includes historical drift: %+v", view)
	}
	awaitDispatchCompletionQueued(t, c)
}

// Failed and no-change reflection both return the same controller contract:
// an acknowledged outcome with no reflection commit and no learned program.
func TestRepairedDriftRecursWithoutProgramAfterRestart(t *testing.T) {
	c, client, cache, requests, start := newDriftScenario(t, false)
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	stepScenario(t, c, cache, start.Add(2*time.Second))
	dispatchScenario(t, c)
	first := awaitRequest(t, requests)
	awaitDispatchCompletionQueued(t, c)
	stepScenario(t, c, cache, start.Add(3*time.Second))
	acknowledgeScenario(t, c)
	state := exportState(t, c)
	if err := c.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	stepScenario(t, c, cache, start.Add(4*time.Second))
	if c.IncidentOpen() {
		t.Fatal("restored baseline opened stale incident")
	}
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(5*time.Second))
	stepScenario(t, c, cache, start.Add(6*time.Second))
	dispatchScenario(t, c)
	second := awaitRequest(t, requests)
	if second.IncidentID == first.IncidentID || second.Findings[0].DetectorID != "drift.unexplained" {
		t.Fatalf("identical repaired fault was suppressed: %+v", second)
	}
	awaitDispatchCompletionQueued(t, c)
}

func dispatchScenario(t *testing.T, c *Controller) {
	t.Helper()
	if effect, ok := c.PendingWorkspaceEffect(); ok {
		if err := c.ExecuteWorkspaceEffect(context.Background(), effect); err != nil {
			t.Fatal(err)
		}
		awaitBrokerResult(t, c.workspaceResults)
		c.processBrokerCompletions()
	}
	executePendingEffect(t, c)
}

func TestTerminalDriftResponderFailureDoesNotWaitForTTL(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t, false)
	dispatcher := &failedJobDispatcher{}
	c.dispatcher = dispatcher
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	stepScenario(t, c, cache, start.Add(2*time.Second))
	dispatchScenario(t, c)
	awaitDispatchCompletionQueued(t, c)
	stepScenario(t, c, cache, start.Add(3*time.Second))
	state := exportState(t, c)
	closure := state.PendingClosure
	if closure == nil || closure.GetDispatchError() == "" || closure.Result != nil || state.DispatchState != contracts.DispatchState_DISPATCH_STATE_IDLE {
		t.Fatalf("terminal failure was not immediate: %+v", state)
	}
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("terminal drift worker was retried")
	}
	if dispatcher.calls != 1 {
		t.Fatalf("failed responder dispatches=%d", dispatcher.calls)
	}
}

func TestWatchEventProgramMatchesUnownedServiceBeforeSweep(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t, true, time.Hour)
	config := *c.drift
	config.SweepInterval = time.Hour
	config.ProgramPersistence.MinDuration = time.Second
	c.ConfigureDrift(config)
	service, err := client.CoreV1().Services("app").Get(context.Background(), "peer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.ManagedFields = nil
	if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	injectScenarioPort(t, client, cache, "peer")
	stepScenario(t, c, cache, start.Add(time.Second))
	if c.IncidentOpen() || len(exportState(t, c).PendingBatch.Findings) != 0 {
		t.Fatal("program fired before minimum duration")
	}
	if c.NextWake().After(start.Add(2 * time.Second)) {
		t.Fatal("pending binding has no scheduled confirmation")
	}
	stepScenario(t, c, cache, start.Add(2*time.Second-time.Nanosecond))
	if c.IncidentOpen() {
		t.Fatal("program fired just before minimum duration")
	}
	stepScenario(t, c, cache, start.Add(2*time.Second))
	state := exportState(t, c)
	if !state.IncidentOpen || state.IncidentRequest == nil || len(state.IncidentRequest.Findings) != 1 {
		t.Fatalf("unowned watch event did not open a learned incident before sweep: %+v", state)
	}
	finding := state.IncidentRequest.Findings[0]
	if finding.DetectorId != "drift.targetport" || finding.Severity != string(sdk.SeverityLatent) || finding.PrimaryResource.Name != "peer" || finding.ParameterBindings["service"].Name != "peer" {
		t.Fatalf("watch finding lost learned binding: %+v", finding)
	}
	if len(state.DriftEvents) != 1 || state.DriftEvents[0].Source != "watch event program match" || !controllerTime(state.DriftEvents[0].ObservedAt).Equal(start.Add(time.Second)) {
		t.Fatalf("watch source and event time missing: %+v", state.DriftEvents)
	}
}

func TestWatchEventProgramMatchesOwnedServiceBeforeSweep(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t, true, time.Hour)
	config := *c.drift
	config.SweepInterval = time.Hour
	config.ProgramPersistence.MinDuration = time.Second
	c.ConfigureDrift(config)
	injectScenarioPort(t, client, cache, "peer")
	stepScenario(t, c, cache, start.Add(time.Second))
	if c.IncidentOpen() || len(exportState(t, c).PendingBatch.Findings) != 0 {
		t.Fatal("program fired before minimum duration")
	}
	if c.NextWake().After(start.Add(2 * time.Second)) {
		t.Fatal("pending binding has no scheduled confirmation")
	}
	stepScenario(t, c, cache, start.Add(2*time.Second-time.Nanosecond))
	if c.IncidentOpen() {
		t.Fatal("program fired just before minimum duration")
	}
	stepScenario(t, c, cache, start.Add(2*time.Second))
	state := exportState(t, c)
	if !state.IncidentOpen || state.IncidentRequest == nil || len(state.IncidentRequest.Findings) != 1 {
		t.Fatalf("owned watch event did not open a learned incident before sweep: %+v", state)
	}
	finding := state.IncidentRequest.Findings[0]
	if finding.DetectorId != "drift.targetport" || finding.Severity != string(sdk.SeverityLatent) || finding.PrimaryResource.Name != "peer" || finding.ParameterBindings["service"].Name != "peer" {
		t.Fatalf("watch finding lost learned binding: %+v", finding)
	}
	if len(state.DriftEvents) != 1 || len(state.DriftEvents[0].Fields) != 1 || state.DriftEvents[0].Source != "watch event program match" || !controllerTime(state.DriftEvents[0].ObservedAt).Equal(start.Add(time.Second)) {
		t.Fatalf("watch source and event time missing: %+v", state.DriftEvents)
	}
}

func TestWatchEventUnownedChangeWithoutProgramMatchIsQuiet(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t, true)
	config := *c.drift
	config.SweepInterval = time.Hour
	c.ConfigureDrift(config)
	before, err := client.CoreV1().Services("app").Get(context.Background(), "peer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before.ManagedFields = nil
	after := before.DeepCopy()
	after.Spec.Ports[0].TargetPort = intstr.FromInt32(8081)
	if _, err := client.CoreV1().Services("app").Update(context.Background(), after, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, after)
	stepScenario(t, c, cache, start.Add(time.Second))
	state := exportState(t, c)
	if state.IncidentOpen || len(state.DriftEvents) != 0 || len(state.PendingBatch.Findings) != 0 {
		t.Fatalf("unmatched unowned change produced drift: %+v", state)
	}
}

func TestWatchEventTransientMidRolloutProgramNeverFires(t *testing.T) {
	c, _, cache, _, start := newDriftScenario(t, true, time.Hour)
	before, after := driftService(8080), driftService(9090)
	after.Spec.Selector = map[string]string{"app": "api"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*after}, PodList: []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "old-api", Namespace: "app", Labels: map[string]string{"app": "api"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}},
	}}}
	provider := &changingProvider{snapshot: snapshot}
	c.provider = provider
	calls := 0
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: scenarioPortProgram{}.Spec(), run: func(_ context.Context, b program.Bindings) program.Verdict {
		calls++
		current := provider.snapshot.(sdktest.Snapshot)
		service := current.ServiceList[0]
		pod := current.PodList[0]
		if b["service"].Name == service.Name && pod.Labels["app"] == service.Spec.Selector["app"] && pod.Spec.Containers[0].Ports[0].ContainerPort != service.Spec.Ports[0].TargetPort.IntVal {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := *c.drift
	config.Programs, config.SweepInterval = registry, time.Hour
	config.ProgramPersistence.MinDuration = time.Second
	config.View = func(snapshot sdk.DetectionContext) program.ReadView {
		// The deployer accepted targetPort 9090; old selected pods still use 8080.
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: false}}
	}
	c.ConfigureDrift(config)
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, after)
	stepScenario(t, c, cache, start.Add(time.Second))
	state := exportState(t, c)
	if calls != 1 || state.IncidentOpen || len(state.PendingBatch.Findings) != 0 {
		t.Fatalf("rollout should be diagnosed but pending: calls=%d state=%+v", calls, state)
	}
	// The rollout settles before confirmation, without another Service event.
	snapshot.PodList[0].Spec.Containers[0].Ports[0].ContainerPort = 9090
	provider.snapshot = snapshot
	stepScenario(t, c, cache, start.Add(2*time.Second))
	stepScenario(t, c, cache, start.Add(3*time.Second))
	if calls != 2 || c.IncidentOpen() || !c.tracker.Quiet() || len(exportState(t, c).PendingBatch.Findings) != 0 {
		t.Fatalf("transient mismatch fired or retained confirmation: calls=%d state=%+v", calls, exportState(t, c))
	}
}

func TestWatchEventRepairQueuesNothingAndSweepClearsLearnedIncident(t *testing.T) {
	c, client, cache, requests, start := newDriftScenario(t, true)
	config := *c.drift
	config.SweepInterval = time.Hour
	config.ProgramPersistence.MinDuration = time.Second
	c.ConfigureDrift(config)
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	stepScenario(t, c, cache, start.Add(2*time.Second))
	dispatchScenario(t, c)
	request := awaitRequest(t, requests)
	awaitDispatchCompletionQueued(t, c)
	// The scripted responder repaired the Service. Deliver its watch event
	// while the learned incident is still open and before the next sweep.
	fixed, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: scenarioPortProgram{}.Spec(), run: func(context.Context, program.Bindings) program.Verdict {
		calls++
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.drift.Programs = registry
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(driftService(9999), fixed)
	snapshot, err := c.provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.processDrift(context.Background(), start.Add(3*time.Second), snapshot, cache.TakeObjectChanges(), false)
	state := exportState(t, c)
	if calls != 1 || !state.IncidentOpen || state.IncidentRequest.IncidentId != request.IncidentID || len(state.IncidentRequest.Findings) != 1 || len(state.PendingBatch.Findings) != 0 {
		t.Fatalf("repair must diagnose without queueing another finding: calls=%d state=%+v", calls, state)
	}
	if !c.tracker.HasActiveDetector("drift.targetport") {
		t.Fatal("program cleared before symmetric minimum duration")
	}
	stepScenario(t, c, cache, start.Add(4*time.Second))
	if c.tracker.HasActiveDetector("drift.targetport") {
		t.Fatal("confirmed repair did not clear the program")
	}
	if _, ok := c.PendingIncidentClosure(); !ok {
		t.Fatalf("sweep NoMatch did not clear the repaired learned incident: %+v", exportState(t, c))
	}
}

func TestWatchEventPendingBindingsSurviveRestartIndependently(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t, true)
	config := *c.drift
	config.SweepInterval = time.Hour
	config.ProgramPersistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1, MinDuration: 10 * time.Second}
	c.ConfigureDrift(config)
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	injectScenarioPort(t, client, cache, "peer")
	stepScenario(t, c, cache, start.Add(3*time.Second))
	raw, err := encodeControllerState(exportState(t, c))
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeControllerState(raw)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh tracker must recover the pending bindings and their deadlines.
	c.tracker = NewFindingStateTracker(1, 1)
	if err := c.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	stepScenario(t, c, cache, start.Add(11*time.Second))
	state = exportState(t, c)
	if state.IncidentRequest == nil || len(state.IncidentRequest.Findings) != 1 || state.IncidentRequest.Findings[0].PrimaryResource.Name != "api" {
		t.Fatalf("one binding changed another's confirmation streak: %+v", state)
	}
	stepScenario(t, c, cache, start.Add(13*time.Second))
	if findings := exportState(t, c).IncidentRequest.Findings; len(findings) != 2 {
		t.Fatalf("second binding failed to confirm independently: %+v", findings)
	}
}

func TestProgramSweepUsesDefaultTimedConfirmation(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t, true)
	config := *c.drift
	config.ProgramPersistence = sdk.PersistencePolicy{}
	c.ConfigureDrift(config)
	// Omit the watch event: only the sweep can discover this state.
	service, err := client.CoreV1().Services("app").Get(context.Background(), "peer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	stepScenario(t, c, cache, start.Add(time.Second))
	if c.IncidentOpen() {
		t.Fatal("sweep bypassed default persistence")
	}
	stepScenario(t, c, cache, start.Add(11*time.Second))
	state := exportState(t, c)
	if !state.IncidentOpen || state.IncidentRequest == nil || len(state.IncidentRequest.Findings) != 1 {
		t.Fatalf("sweep match did not confirm at default 10s duration: %+v", state)
	}
	if events := state.IncidentRequest.DriftEvents; len(events) != 1 || events[0].Source != "periodic program sweep" {
		t.Fatalf("sweep provenance changed during confirmation: %+v", events)
	}
}
