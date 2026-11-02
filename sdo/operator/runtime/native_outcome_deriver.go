package runtime

import (
	"context"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/outcome_derivation"
)

// ProtoOutcomeDeriver bridges the runtime closure to the generated contract.
// The derivation package remains independent of runtime and repository layout.
type ProtoOutcomeDeriver struct {
	SessionRoot string
}

func (d ProtoOutcomeDeriver) Derive(ctx context.Context, closure IncidentClosure, repairCommit, backend, model string) (DerivedOutcome, error) {
	message, err := toProtoClosure(closure)
	if err != nil {
		return DerivedOutcome{}, err
	}
	// Add transport-owned write facts to the private contract copy, preserving
	// the frozen closure and any worker-supplied receipts. These receipts still
	// need fresh independent finding clearance under the shared cause validator.
	if d.SessionRoot != "" && message.GetResult() != nil {
		var resources []*contracts.ObjectRef
		for _, finding := range message.GetRequest().GetFindings() {
			resources = append(resources, finding.GetPrimaryResource())
			resources = append(resources, finding.GetRelatedResources()...)
		}
		for _, cause := range message.GetResult().GetConfirmedRootCauses() {
			resources = append(resources, cause.GetResources()...)
		}
		until := closure.ResponderCompletedAt
		if closure.VerifiedAt.After(until) {
			until = closure.VerifiedAt
		}
		since := closure.DispatchedAt
		if since.IsZero() {
			since = closure.DetectedAt
		}
		if receipts, readErr := memory.IncidentAPIRepairReceipts(d.SessionRoot, closure.Request.IncidentID, resources, since, until); readErr != nil {
			slog.WarnContext(ctx, "incident_api_repair_receipts_unavailable", "incident_id", closure.Request.IncidentID, "error", readErr)
		} else {
			seen := map[string]int{}
			for index, receipt := range message.Result.RepairActions {
				seen[receipt.GetActionId()] = index
			}
			for _, receipt := range receipts {
				if index, exists := seen[receipt.GetActionId()]; exists {
					message.Result.RepairActions[index] = receipt
				} else {
					seen[receipt.GetActionId()] = len(message.Result.RepairActions)
					message.Result.RepairActions = append(message.Result.RepairActions, receipt)
				}
			}
		}
	}
	result, err := outcome_derivation.Derive(ctx, message, repairCommit, backend, model)
	if err != nil {
		return DerivedOutcome{}, err
	}
	// A failed global closure can still have a complete controller verify
	// proving individual findings cleared. Preserve those actual evaluations
	// (including peer fingerprints) for the shared publication validator.
	if summary, _ := latestPartialVerification(result.Record); summary != nil {
		for _, evaluation := range summary.Evaluations {
			result.Record.DetectorHistory = append(result.Record.DetectorHistory, &contracts.DetectorEvaluation{DetectorId: evaluation.DetectorID, EvaluatedAt: timestamppb.New(evaluation.At), Status: string(evaluation.Status), Fingerprints: append([]string(nil), evaluation.Fingerprints...)})
		}
	}
	return DerivedOutcome{Record: result.Record, RequiresReflection: result.RequiresReflection}, nil
}

func toProtoClosure(closure IncidentClosure) (*contracts.IncidentClosure, error) {
	return incidentClosureContract(closure)
}
