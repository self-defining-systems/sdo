//go:build integration

package runtime

import (
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func newDriftScenario(t *testing.T) (*Controller, *fake.Clientset, *KubernetesCache, chan IncidentRequest, time.Time) {
	t.Helper()
	start := time.Unix(100, 0).UTC()
	first, peer := driftService(8080), driftService(8080)
	peer.Name = "peer"
	client := fake.NewSimpleClientset(first, peer)
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan IncidentRequest, 2)
	config := testControllerConfig()
	config.Namespace = "app"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	// Drift scenarios step in whole seconds; a short success soak keeps
	// their timelines while still exercising the gate.
	config.ClosureStabilityWindow = 500 * time.Millisecond
	health := controllerDetector("health", time.Second, sdk.Finding{})

	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	health.spec.OriginatingCommit = "seed"
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	c, err := NewController(config, []sdk.Detector{health}, fastloopObjects{client}, scriptedDriftResponder{client, requests}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Events: cache, BatchWindow: time.Nanosecond, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return serviceDriftView{&sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}}
	}})
	if err := c.SetIncidentBroker(&recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/tmp/drift-proposal", BaseCommit: "source"}, receipt: ClosureReceipt{OutcomeCommit: "outcome", AckToken: "ack"}}); err != nil {
		t.Fatal(err)
	}
	return c, client, cache, requests, start
}

func TestHermeticDriftlive4NewFaultAfterHealthyBaseline(t *testing.T) {
	c, client, cache, requests, start := newDriftScenario(t)
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	stepScenario(t, c, cache, start.Add(time.Second+time.Nanosecond))
	dispatchScenario(t, c)
	first := awaitRequest(t, requests)
	if len(first.DriftEvents) != 1 || len(first.DriftEvents[0].Fields) != 1 {
		t.Fatal("source drift lost original field evidence")
	}
	awaitDispatchCompletionQueued(t, c)
	stepScenario(t, c, cache, start.Add(2*time.Second))
	stepScenario(t, c, cache, start.Add(2*time.Second+c.config.ClosureStabilityWindow))
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
	stepScenario(t, c, cache, start.Add(6*time.Second+time.Nanosecond))
	dispatchScenario(t, c)
	second := awaitRequest(t, requests)
	if second.IncidentID == first.IncidentID || len(second.Findings) != 1 || second.Findings[0].PrimaryResource.Name != "peer" || second.Findings[0].ParameterBindings["resource"].Name != "peer" || len(second.DriftEvents) != 1 || second.DriftEvents[0].Ref.Name != "peer" {
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
	c, client, cache, requests, start := newDriftScenario(t)
	injectScenarioPort(t, client, cache, "api")
	stepScenario(t, c, cache, start.Add(time.Second))
	stepScenario(t, c, cache, start.Add(2*time.Second))
	dispatchScenario(t, c)
	first := awaitRequest(t, requests)
	awaitDispatchCompletionQueued(t, c)
	stepScenario(t, c, cache, start.Add(3*time.Second))
	stepScenario(t, c, cache, start.Add(3*time.Second+c.config.ClosureStabilityWindow))
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

func TestTerminalDriftResponderFailureDoesNotWaitForTTL(t *testing.T) {
	c, client, cache, _, start := newDriftScenario(t)
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
	if len(closure.Request.DriftEvents) != 1 {
		t.Fatal("terminal failure lost its drift evidence")
	}
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("terminal drift worker was retried")
	}
	if dispatcher.calls != 1 {
		t.Fatalf("failed responder dispatches=%d", dispatcher.calls)
	}
}

