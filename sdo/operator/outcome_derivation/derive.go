// Package outcome_derivation turns independently observed closure facts into an
// immutable operational-memory outcome. It has no repository or broker access.
package outcome_derivation

import (
	"context"
	"errors"
	"fmt"
	outcomeclass "sdo.dev/operator/contracts/outcome"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type Result struct {
	Record             *contractsv1alpha1.OutcomeRecord
	RequiresReflection bool
}

// Derive classifies a controller closure and checks each claimed root cause
// against detector observations and timely, recorded responder actions.
func Derive(ctx context.Context, closure *contractsv1alpha1.IncidentClosure, repairCommit, backend, model string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if closure == nil || closure.GetRequest() == nil || closure.GetRequest().GetIncidentId() == "" {
		return Result{}, errors.New("outcome derivation requires an incident request")
	}
	if closure.GetResult() != nil && closure.GetResult().GetIncidentId() != closure.GetRequest().GetIncidentId() {
		return Result{}, errors.New("result incident_id must match request")
	}
	if backend == "" || model == "" {
		return Result{}, errors.New("responder backend and model are required")
	}
	for label, ts := range map[string]*timestamppb.Timestamp{
		"detected_at": closure.GetDetectedAt(), "dispatched_at": closure.GetDispatchedAt(),
		"responder_completed_at": closure.GetResponderCompletedAt(),
	} {
		if ts == nil || !ts.IsValid() {
			return Result{}, fmt.Errorf("%s is required", label)
		}
	}
	healthVerified := len(closure.GetFinalDetectorStates()) > 0
	for _, evaluation := range closure.GetFinalDetectorStates() {
		if evaluation.GetStatus() != "clear" {
			healthVerified = false
			break
		}
	}
	if healthVerified && (closure.GetVerifiedAt() == nil || !closure.GetVerifiedAt().IsValid()) {
		return Result{}, errors.New("verified_at is required when health is verified")
	}
	ordered := []*timestamppb.Timestamp{closure.GetDetectedAt(), closure.GetDispatchedAt(), closure.GetResponderCompletedAt()}
	if healthVerified {
		ordered = append(ordered, closure.GetVerifiedAt())
	}
	for index := 1; index < len(ordered); index++ {
		if ordered[index].AsTime().Before(ordered[index-1].AsTime()) {
			return Result{}, errors.New("outcome timestamps must be chronological")
		}
	}
	verifications := verifyDiagnosis(closure)
	classification := classify(closure, repairCommit, healthVerified, verifications)
	request, result := closure.GetRequest(), closure.GetResult()

	usage := &contractsv1alpha1.UsageMetrics{}
	if result != nil && result.GetUsage() != nil {
		usage = proto.Clone(result.GetUsage()).(*contractsv1alpha1.UsageMetrics)
	}
	surfaced := make([]string, 0, len(request.GetSurfacedPlaybooks()))
	for _, playbook := range request.GetSurfacedPlaybooks() {
		surfaced = append(surfaced, playbook.GetPlaybookId())
	}
	applied := make([]string, 0, len(result.GetAppliedPlaybooks()))
	for _, playbook := range result.GetAppliedPlaybooks() {
		applied = append(applied, playbook.GetPlaybookId())
	}
	rejected := difference(surfaced, applied)
	completedAt := closure.GetResponderCompletedAt()
	var verifiedAt *timestamppb.Timestamp
	if healthVerified {
		completedAt = closure.GetVerifiedAt()
		verifiedAt = closure.GetVerifiedAt()
	}
	var mitigatedAt *timestamppb.Timestamp
	if result != nil {
		mitigatedAt = closure.GetResponderCompletedAt()
		if healthVerified {
			clearedAt := repairClearAt(closure)
			cutoff := timestamppb.New(*clearedAt)
			for _, action := range result.GetRepairActions() {
				completed := action.GetCompletedAt()
				if !action.GetSuccess() || completed == nil || !completed.IsValid() ||
					completed.AsTime().After(cutoff.AsTime()) {
					continue
				}
				if mitigatedAt == closure.GetResponderCompletedAt() || completed.AsTime().After(mitigatedAt.AsTime()) {
					mitigatedAt = completed
				}
			}
			if mitigatedAt == closure.GetResponderCompletedAt() && clearedAt != nil {
				mitigatedAt = cutoff
			}
		}
	}
	var commit *string
	if repairCommit != "" {
		commit = &repairCommit
	}
	record := &contractsv1alpha1.OutcomeRecord{
		SchemaVersion: 1, IncidentId: request.GetIncidentId(), SourceCommit: request.GetSourceCommit(), DeployedCommit: request.GetDeployedCommit(),
		DetectorHistory: cloneMessages(request.GetDetectorHistory()), Findings: cloneMessages(request.GetFindings()),
		DriftEvents:       cloneMessages(request.GetDriftEvents()),
		SurfacedPlaybooks: surfaced, InspectedPlaybooks: append([]string(nil), surfaced...), ConfirmedPlaybooks: append([]string(nil), applied...),
		RejectedPlaybooks: rejected, AppliedPlaybooks: applied,
		ConfirmedRootCauses: cloneMessages(result.GetConfirmedRootCauses()), FinalHealthDetectorState: cloneMessages(closure.GetFinalDetectorStates()),
		Classification: classification, RepairCommit: commit, RepairActions: cloneMessages(result.GetRepairActions()), MemoryCommit: commit,
		ResponderBackend: backend, ResponderModel: model, Usage: usage,
		Timestamps: &contractsv1alpha1.OutcomeTimestamps{
			DetectedAt: cloneTimestamp(closure.GetDetectedAt()), DispatchedAt: cloneTimestamp(closure.GetDispatchedAt()),
			MitigatedAt: cloneTimestamp(mitigatedAt), VerifiedAt: cloneTimestamp(verifiedAt), CompletedAt: cloneTimestamp(completedAt),
		},
		DiagnosisVerification: verifications,
		ProgramToolCalls:      cloneMessages(result.GetProgramToolCalls()), ProgramRuns: cloneMessages(result.GetProgramRuns()),
		PlanApplications: cloneMessages(result.GetPlanApplications()), ProgramFailureSignals: append([]string(nil), result.GetProgramFailureSignals()...),
		ResponseLoopsUsed: result.GetResponseLoopsUsed(), ResponseLoopExhausted: result.GetResponseLoopExhausted(),
		ResolutionPath: result.GetResolutionPath(), FastPathPrograms: append([]string(nil), result.GetFastPathPrograms()...), FastPathSkippedReason: result.GetFastPathSkippedReason(), FastPathRelationCoveredFindings: cloneMessages(result.GetFastPathRelationCoveredFindings()),
	}
	for _, label := range CheckedDriftAssessments(request.GetDriftEvents(), result.GetDriftEvents()) {
		for i, event := range record.DriftEvents {
			if sameDriftObservation(event, label) {
				record.DriftEvents[i] = label
			}
		}
	}
	reflect := false
	if classification == outcomeclass.Success {
		for _, verification := range verifications {
			if verification.Verdict == "confirmed" {
				reflect = true
				break
			}
		}
	}
	return Result{Record: record, RequiresReflection: reflect}, nil
}

func cloneMessages[T proto.Message](items []T) []T {
	result := make([]T, 0, len(items))
	for _, item := range items {
		result = append(result, proto.Clone(item).(T))
	}
	return result
}

func cloneTimestamp(value *timestamppb.Timestamp) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*timestamppb.Timestamp)
}

func difference(left, right []string) []string {
	seen := make(map[string]bool, len(right))
	for _, value := range right {
		seen[value] = true
	}
	result := []string{}
	for _, value := range left {
		if !seen[value] {
			result = append(result, value)
			seen[value] = true
		}
	}
	return result
}

func classify(closure *contractsv1alpha1.IncidentClosure, repairCommit string, healthVerified bool, verifications []*contractsv1alpha1.DiagnosisVerification) string {
	result := closure.GetResult()
	if closure.GetRequest() == nil {
		return outcomeclass.Failed
	}
	// The controller owns the missed-fault flag in prior Python closures. This
	// proto does not carry it; no negative claim is inferred from its absence.
	if closure.GetDispatchError() != "" || result == nil || result.GetStatus() == "failed" {
		return outcomeclass.Failed
	}
	if !healthVerified {
		return outcomeclass.Failed
	}
	if repairCommit == "" && (result.GetStatus() == "completed" || result.GetStatus() == "cancelled") {
		hadSuccessfulAction := false
		for _, action := range result.GetRepairActions() {
			if action.GetSuccess() {
				hadSuccessfulAction = true
				break
			}
		}
		if !hadSuccessfulAction {
			return outcomeclass.ClearedWithoutAction
		}
	}
	if result.GetStatus() == "cancelled" {
		return outcomeclass.Cancelled
	}
	if len(result.GetConfirmedRootCauses()) == 0 {
		return outcomeclass.FalsePositive
	}
	if len(verifications) > 0 {
		attributed := false
		confirmed := false
		for _, verification := range verifications {
			if verification.GetVerdict() == "contradicted" {
				return "partial"
			}
			confirmed = confirmed || verification.GetVerdict() == "confirmed"
			if verification.Repair.Attributed {
				attributed = true
			}
		}
		if !attributed {
			return outcomeclass.ExternalRecovery
		}
		if !confirmed {
			return "partial"
		}
	}
	return outcomeclass.Success
}
