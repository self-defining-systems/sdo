package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

func TestRECEarlyReportMergePreservesVerifiedCause(t *testing.T) {
	for _, multiple := range []bool{false, true} {
		name := "single-finding"
		if multiple {
			name = "shared-resource-findings"
		}
		t.Run(name, func(t *testing.T) {
			c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
			request := *c.currentIncidentRequest
			if multiple {
				peer := request.Findings[0]
				peer.Summary = "another finding on the same repaired resource"
				peer.Evidence = "independent opening evidence"
				request.Findings = append(append([]sdk.Finding{}, request.Findings...), peer)
				c.currentIncidentRequest = &request
			}
			loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
			loop.menuSession = &contracts.MitigationSession{ProviderSessionId: "rec-session"}
			ref := request.Findings[0].PrimaryResource
			loop.menuRepairs = []RepairActionReceipt{{ActionID: "sdo-act-rec", Kind: "kubectl-patch", Target: "Service/demo/api", Summary: "repair resource", Resources: []sdk.ObjectRef{ref}, StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0), Success: true}}
			c.responderLoop = loop
			for i := 2; i <= 17; i++ {
				step(i)
			}
			early, ok := c.PendingIncidentClosure()
			if !ok {
				t.Fatal("missing early closure")
			}
			assertVerified := func(label string, closure IncidentClosure) {
				t.Helper()
				derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
				if err != nil {
					t.Fatal(err)
				}
				if got := len(validate.VerifiedReflectionCauses(derived.Record)); got != 1 {
					t.Fatalf("%s: verified causes=%d, want 1; causes=%v", label, got, derived.Record.ConfirmedRootCauses)
				}
			}
			assertVerified("missing report", early)
			final := IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted, ResponderSessionID: "rec-session", Timing: TimingMetrics{StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(18, 0)}, ConfirmedRootCauses: []ConfirmedRootCause{{Summary: "reported routing repair", Resources: []sdk.ObjectRef{ref}}}, RepairActions: loop.menuRepairs}
			late := early
			late.VerifiedWhileResponderRunning = false
			late.Result = &final
			assertVerified("late closure", late)
			c.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, result: final}, time.Unix(18, 0))
			merged, ok := c.PendingIncidentClosure()
			if !ok {
				t.Fatal("missing reported closure")
			}
			assertVerified("normal early report", merged)
			cause := merged.Result.ConfirmedRootCauses[0]
			if cause.Summary != final.ConfirmedRootCauses[0].Summary || len(cause.Evidence) != len(request.Findings) || len(cause.ExplainedDetectors) != 1 {
				t.Fatalf("lost report or controller evidence: %+v", cause)
			}
		})
	}
}
