package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// A responder can finish before a rollout settles. Its last self-check must
// not override the controller's subsequent independent recovery proof.
func TestVerifiedClosureSettlesFailedResponderReport(t *testing.T) {
	for _, scenario := range []string{"recovered", "still-unhealthy", "dispatch-failed"} {
		t.Run(scenario, func(t *testing.T) {
			unhealthy := scenario == "still-unhealthy"
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			finding := sdk.Finding{}
			if unhealthy {
				finding = sdk.Finding{RuleID: "unhealthy", Status: sdk.FindingActive}
			}
			d := controllerDetector("health", time.Second, finding)
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
			result.Status = IncidentFailed
			result.UnresolvedFindings = []sdk.Finding{{DetectorID: "health", RuleID: "unhealthy", Status: sdk.FindingActive}}
			result.RepairActions = []RepairActionReceipt{{ActionID: "synthetic-patch", Kind: "kubectl-patch", Success: true}}
			c.currentIncidentResult = &result
			if scenario == "dispatch-failed" {
				c.dispatchError = "synthetic Job failure"
			}
			var closed *IncidentClosure
			c.OnIncidentClosed = func(closure IncidentClosure) { closed = &closure }
			for i := 0; i < 4; i++ {
				if err := c.Step(ctx, now, nil); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Second)
			}
			if unhealthy {
				if closed != nil {
					t.Fatal("unhealthy report became verified")
				}
				return
			}
			if closed == nil {
				t.Fatal("recovered incident did not close")
			}
			if scenario == "dispatch-failed" {
				if closed.Result.Status != IncidentFailed {
					t.Fatal("dispatch failure acquired completed status")
				}
				return
			}
			if len(closed.Result.VerificationEvidence) != 1 || closed.Result.VerificationEvidence[0].Name != "responder_report_before_recovery" {
				t.Fatal("original report evidence lost")
			}
			if closed.Result.Status != IncidentCompleted || len(closed.Result.UnresolvedFindings) > 0 {
				t.Fatalf("stale responder report survived independent proof: %+v", closed.Result)
			}
			outcome, err := (ProtoOutcomeDeriver{}).Derive(ctx, *closed, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Record.GetClassification() != "success" {
				t.Fatalf("classification = %s", outcome.Record.GetClassification())
			}
			if result.Status != IncidentFailed || len(result.UnresolvedFindings) != 1 {
				t.Fatal("mutated worker's original report")
			}
		})
	}
}
