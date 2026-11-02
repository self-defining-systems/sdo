package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

func TestSharedCauseGateCanVerifyColdRepairWhilePeerFindingRemains(t *testing.T) {
	outcome := coldPartialRepairOutcome(t)
	evidence := incidentResultState(&contracts.IncidentResult{ConfirmedRootCauses: outcome.ConfirmedRootCauses, RepairActions: outcome.RepairActions, ProgramToolCalls: outcome.ProgramToolCalls})
	closure := IncidentClosure{Request: IncidentRequest{IncidentID: "contract", SourceCommit: "source", Namespace: "app"}, Result: &evidence, DetectedAt: time.Unix(90, 0), DispatchedAt: time.Unix(95, 0), ResponderCompletedAt: time.Unix(120, 0)}
	closure.Result.IncidentID = "contract"
	closure.Result.Status = IncidentFailed
	for _, finding := range outcome.GetFindings() {
		closure.Request.Findings = append(closure.Request.Findings, findingState(finding))
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	indices := partialRepairCauses(derived.Record)
	if len(indices) != 1 || indices[0] != 0 {
		t.Fatalf("fixture lacks real partial repair proof: %v", indices)
	}
	derived.Record.ReflectionEligibility = &contracts.ReflectionEligibility{Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: outcome.GetConfirmedRootCauses()[0].GetResources()}}}
	if got := validate.VerifiedReflectionCauses(derived.Record); len(got) != 1 || got[0].GetCauseIndex() != 0 {
		t.Fatalf("shared gate lost controller-verified finding clearance: %v", got)
	}
}
