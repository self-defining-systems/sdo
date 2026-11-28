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

// Derive classifies a controller closure by what the controller observed: the
// Job's own status and whether the health detectors cleared. recorded is agent.recording: with it off the agent's
// own cluster writes are unknown, so the changes it reports stand in for them (see classify).
func Derive(ctx context.Context, closure *contractsv1alpha1.IncidentClosure, repairCommit, backend, model string, recorded bool) (Result, error) {
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
	healthVerified := len(closure.GetFinalDetectorStates()) > 0 || closure.GetHealthClearedAt().IsValid()
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
	if healthVerified && !closure.GetVerifiedWhileResponderRunning() {
		ordered = append(ordered, closure.GetVerifiedAt())
	}
	for index := 1; index < len(ordered); index++ {
		if ordered[index].AsTime().Before(ordered[index-1].AsTime()) {
			return Result{}, errors.New("outcome timestamps must be chronological")
		}
	}
	classification := classify(closure, repairCommit, healthVerified, recorded)
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
	// Mitigation is the latest successful repair action's completion, else
	// the responder's completion.
	var mitigatedAt *timestamppb.Timestamp
	if result != nil {
		mitigatedAt = closure.GetResponderCompletedAt()
		var latest *timestamppb.Timestamp
		for _, action := range result.GetRepairActions() {
			completed := action.GetCompletedAt()
			if action.GetSuccess() && completed != nil && completed.IsValid() &&
				!completed.AsTime().After(completedAt.AsTime()) &&
				(latest == nil || completed.AsTime().After(latest.AsTime())) {
				latest = completed
			}
		}
		if latest != nil {
			mitigatedAt = latest
		}
	}
	if healthVerified && closure.GetVerifiedWhileResponderRunning() {
		mitigatedAt = closure.GetVerifiedAt()
	}
	var commit *string
	if repairCommit != "" {
		commit = &repairCommit
	}
	record := &contractsv1alpha1.OutcomeRecord{
		DriftReprompt:           cloneMessage(result.GetDriftReprompt()),
		StatePersistenceFailure: cloneMessage(closure.GetStatePersistenceFailure()),
		SchemaVersion:           1, IncidentId: request.GetIncidentId(), SourceCommit: request.GetSourceCommit(), DeployedCommit: request.GetDeployedCommit(),
		DetectorHistory: append(append(cloneMessages(request.GetDetectorHistory()), cloneMessages(closure.GetLearnedDetectorStates())...), cloneMessages(closure.GetPartialDetectorStates())...), Findings: cloneMessages(request.GetFindings()),
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
		ProgramToolCalls: cloneMessages(result.GetProgramToolCalls()), ProgramRuns: cloneMessages(result.GetProgramRuns()),
		PlanApplications: cloneMessages(result.GetPlanApplications()), ProgramFailureSignals: append([]string(nil), result.GetProgramFailureSignals()...),
		ResponseLoopsUsed: result.GetResponseLoopsUsed(), ResponseLoopExhausted: result.GetResponseLoopExhausted(),
		ResolutionPath: result.GetResolutionPath(), PlaybookCoverage: cloneMessage(request.GetPlaybookCoverage()),
		PriorStages: cloneMessages(request.GetPriorStages()), SourceCheckNotes: append([]string(nil), closure.GetSourceCheckNotes()...),
		RecoveryVerification: cloneMessage(closure.GetRecoveryVerification()),
		AgentFindings:        cloneMessages(result.GetFindings()),
		AgentChanges:         cloneMessages(result.GetAgentChanges()),
	}
	for _, label := range CheckedDriftAssessments(request.GetDriftEvents(), result.GetDriftEvents()) {
		for i, event := range record.DriftEvents {
			if sameDriftObservation(event, label) {
				record.DriftEvents[i] = label
			}
		}
	}
	return Result{Record: record, RequiresReflection: classification == outcomeclass.Success}, nil
}

func cloneMessages[T proto.Message](items []T) []T {
	result := make([]T, 0, len(items))
	for _, item := range items {
		//assertcheck:ok proto.Clone of a T returns a T
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

// classify reduces the closure to four outcomes: the responder Job failed, a
// page closed at its bound or the incident was reset (failed), a verified repair
// whose closure recheck window never completed (unconfirmed), health cleared
// with no repair mutation (cleared_without_action), or anything else that
// cleared (success). SDO never escalates, so no new closure is escalated.
func classify(closure *contractsv1alpha1.IncidentClosure, repairCommit string, healthVerified, recorded bool) string {
	result := closure.GetResult()
	if closure.GetStatePersistenceFailure() != nil || closure.GetRequest() == nil || closure.GetDispatchError() != "" || result == nil {
		return outcomeclass.Failed
	}
	if result.GetStatus() == "failed" {
		return outcomeclass.Failed
	}
	if closure.GetUnconfirmedReason() != contractsv1alpha1.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		return outcomeclass.Unconfirmed
	}
	if !healthVerified {
		return outcomeclass.Failed
	}
	if repairCommit == "" {
		for _, action := range result.GetRepairActions() {
			if action.GetSuccess() {
				return outcomeclass.Success
			}
		}
		if !recorded && agentChangeBeforeClear(closure) {
			return outcomeclass.Success
		}
		return outcomeclass.ClearedWithoutAction
	}
	return outcomeclass.Success
}

// agentChangeBeforeClear reports whether the agent reported a change that took effect before the alert set cleared
// for good. With the recording off the agent's own cluster writes are unknown, so its reported changes stand in for
// them: a clear the agent did not precede with a change (the harness undoing the fault, a transient that passed) is
// not its fix. A change time after the verified clear, or after the responder ended, is not a cause of the clear.
func agentChangeBeforeClear(closure *contractsv1alpha1.IncidentClosure) bool {
	cleared := closure.GetHealthClearedAt()
	if cleared == nil || !cleared.IsValid() {
		cleared = closure.GetVerifiedAt()
	}
	if cleared == nil || !cleared.IsValid() {
		return false
	}
	for _, change := range closure.GetResult().GetAgentChanges() {
		if at := change.GetAt(); at != nil && at.IsValid() && !at.AsTime().After(cleared.AsTime()) && !at.AsTime().Before(closure.GetDetectedAt().AsTime()) {
			return true
		}
	}
	return false
}

func cloneMessage[T proto.Message](value T) T {
	if !value.ProtoReflect().IsValid() {
		return value
	}
	//assertcheck:ok proto.Clone of a T returns a T
	return proto.Clone(value).(T)
}
