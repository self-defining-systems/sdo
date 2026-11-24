package runtime

import (
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// A closure whose health recheck confirms but whose opening user test never
// passes since the last write waits for the test's horizon and then commits
// unconfirmed. The wait is one timer: a recheck that confirms again must not
// restart it, or the incident stays open for as long as the test stays pending.
func TestClosureWaitingOnRecoveredIsBounded(t *testing.T) {
	ctx := t.Context()
	now := verifyT0.Add(2 * time.Minute)
	after := verifyT0.Add(time.Minute)
	cfg := testControllerConfig()
	cfg.Namespace = "app"

	failing := func(time.Time) bool { return false }
	controller, err := NewController(cfg, []sdk.Detector{&closureScopeDetector{id: "traffic-probe-call"}}, riskSnapshots{view: verifyWorld{now: now, probeOK: failing}.view()}, &recordingDispatcher{}, after)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	controller.gitRead = fakeArchitecture(verifyArch)
	controller.broker = &riskBroker{}
	result := completedResult("synthetic-incident")
	result.Timing = TimingMetrics{StartedAt: verifyT0, CompletedAt: after}
	request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "app", IncidentID: "synthetic-incident", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree",
		Findings: []sdk.Finding{browseTest()}, OpeningKeys: findingKeys([]sdk.Finding{browseTest()})}
	controller.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: verifyT0, DispatchedAt: verifyT0, ResponderCompletedAt: after, VerifiedAt: after,
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "traffic-probe-call", EvaluatedAt: after, Status: DetectorEvaluationClear}}}
	controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING

	// Ten minutes of rechecks, ten seconds apart, far beyond any horizon.
	for range 60 {
		effect, ok := controller.PendingClosureEffect()
		if !ok || !effect.Freeze {
			break
		}
		if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
			t.Fatal(err)
		}
		awaitEffectGoroutines(controller)
		now = now.Add(10 * time.Second)
	}
	pending := controller.pendingClosure
	if pending == nil || pending.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		t.Fatalf("closure still waiting on a pending user test after 10 minutes: %+v", pending)
	}
}
