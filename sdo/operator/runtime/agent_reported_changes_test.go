package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/sdk"
)

// With the recording off the agent's own cluster writes are unknown, so a fixed incident reaches success only through
// a change the agent reported before the alerts cleared; that success is what lets the closing turn learn.
func TestRecordingOffSuccessNeedsAnAgentChangeBeforeTheClear(t *testing.T) {
	for name, tc := range map[string]struct {
		changeAt  time.Duration // relative to the step that verifies the closure; zero value means no change reported
		reported  bool
		wantClass string
	}{
		"change reported before the clear": {changeAt: -1500 * time.Millisecond, reported: true, wantClass: outcome.Success},
		"completed with no change":         {wantClass: outcome.ClearedWithoutAction},
		"change after the clear":           {changeAt: time.Hour, reported: true, wantClass: outcome.ClearedWithoutAction},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			d := controllerDetector("health", time.Second, sdk.Finding{})
			d.spec.Class = sdk.DetectorClassHealth
			d.spec.Owner = sdk.DetectorOwnerHealthJudge
			d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
			d.spec.OriginatingCommit = "synthetic"
			d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
			cfg := testControllerConfig()
			cfg.ClosureStabilityWindow = time.Second
			c, err := NewController(cfg, []sdk.Detector{d}, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			c.incidentOpen, c.responderDone = true, true
			c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
			c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", ResponseDeadline: now.Add(time.Minute)}
			c.incidentDetectedAt, c.incidentDispatchedAt, c.responderCompletedAt = now.Add(-3*time.Second), now.Add(-2*time.Second), now.Add(-time.Second)
			result := completedResult("synthetic")
			if tc.reported {
				result.AgentChanges = []*contracts.AgentChange{{Summary: "set a row back", Target: "table rows", At: timestamppb.New(now.Add(tc.changeAt))}}
			}
			c.currentIncidentResult = &result
			var closed *IncidentClosure
			c.OnIncidentClosed = func(closure IncidentClosure) { closed = &closure }
			for i := 0; i < 4; i++ {
				if err := c.Step(ctx, now, nil); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Second)
			}
			if closed == nil {
				t.Fatal("recovered incident did not close")
			}
			derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, *closed, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if got := derived.Record.GetClassification(); got != tc.wantClass {
				t.Fatalf("classification = %s, want %s", got, tc.wantClass)
			}
			if tc.reported && len(derived.Record.GetAgentChanges()) != 1 {
				t.Fatalf("agent-reported changes not kept in the outcome: %v", derived.Record.GetAgentChanges())
			}
			learns := ClosingLearns(derived.Record.GetClassification(), closingChangesOf(closureWithRecordedActions(*closed, derived.Record)), false)
			if learns != (tc.wantClass == outcome.Success) {
				t.Fatalf("ClosingLearns = %v for %s", learns, tc.wantClass)
			}
		})
	}
}
