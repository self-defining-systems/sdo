package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// driftOnlyController opens an incident whose only finding is unexplained
// drift while the health detector stays clear, and blocks its responder.
func driftOnlyController(t *testing.T) (*Controller, *blockingDispatcher, func(int)) {
	t.Helper()
	health := controllerDetector("health", time.Second, sdk.Finding{})
	health.spec.Class, health.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 1}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	health.spec.OriginatingCommit = "health-objective"
	d := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	config := testControllerConfig()
	config.Namespace = "app"
	c, err := NewController(config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, d, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{BatchWindow: time.Nanosecond})
	step := func(i int) {
		t.Helper()
		if err := c.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	step(0)
	c.queueUnexplainedFinding(time.Unix(0, 0), sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}, Classification: sdk.DriftUnexplained})
	step(1)
	executePendingEffect(t, c)
	awaitRequest(t, d.requests)
	return c, d, step
}

// An incident with no failing health detector gets the agent's whole turn:
// health being clear while the responder runs does not close it, and drift
// does not hold it once the agent returns.
func TestDriftOnlyIncidentStaysOpenUntilTheAgentReturns(t *testing.T) {
	c, d, step := driftOnlyController(t)
	closed := make(chan IncidentClosure, 1)
	c.OnIncidentClosed = func(v IncidentClosure) { closed <- v }
	for i := 2; i <= 60; i++ {
		step(i)
		if !c.IncidentOpen() {
			t.Fatalf("drift-only incident closed at %ds while the agent was still running", i)
		}
	}
	close(d.release)
	awaitDispatchCompletionQueued(t, c)
	for i := 61; i <= 120 && c.IncidentOpen(); i++ {
		step(i)
	}
	select {
	case closure := <-closed:
		if closure.VerifiedWhileResponderRunning {
			t.Fatalf("closure skipped the agent's turn: %+v", closure)
		}
	default:
		t.Fatal("drift held the closure after the agent returned and health was clear")
	}
}

// Without any health detector, the incident's own non-drift findings are the
// observation basis; drift never joins them as an unresolved finding.
func TestDriftNeverEntersVerificationFindingsWithoutHealthDetectors(t *testing.T) {
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}}
	}})
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}
	event := sdk.DriftEvent{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort"}}, Classification: sdk.DriftUnexplained}
	drift := sdk.Finding{DetectorID: "drift.unexplained", Status: sdk.FindingActive, Fingerprint: "drift", PrimaryResource: ref, Metadata: map[string]any{"drift_event": event}}
	c.mu.Lock()
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "incident", Namespace: "app", Findings: []sdk.Finding{drift}}
	c.mu.Unlock()
	source := c.verifyClosureSource(context.Background(), time.Unix(1, 0), sdktest.Snapshot{NamespaceName: "app"}, nil)
	c.mu.Lock()
	verification := c.continuationVerificationLocked(true, source)
	c.mu.Unlock()
	if !verification.Healthy || len(verification.Findings) != 0 || len(verification.Drift) != 1 {
		t.Fatalf("drift decided verification: %+v", verification)
	}
}
