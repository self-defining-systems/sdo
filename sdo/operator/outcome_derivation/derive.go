// Package outcome_derivation turns independently observed closure facts into an
// immutable operational-memory outcome. It has no repository or broker access.
package outcome_derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
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
	healthVerified := closure.GetDetectorReviewRequiredAt() == nil && len(closure.GetFinalDetectorStates()) > 0
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

	detectorHistory, err := protoList(request.GetDetectorHistory())
	if err != nil {
		return Result{}, err
	}
	findings, err := protoList(request.GetFindings())
	if err != nil {
		return Result{}, err
	}
	finalStates, err := protoList(closure.GetFinalDetectorStates())
	if err != nil {
		return Result{}, err
	}
	rootCauses, err := protoList(result.GetConfirmedRootCauses())
	if err != nil {
		return Result{}, err
	}
	repairActions, err := protoList(result.GetRepairActions())
	if err != nil {
		return Result{}, err
	}
	usage := json.RawMessage(`{"llm_calls":0,"input_tokens":0,"output_tokens":0}`)
	if result != nil && result.GetUsage() != nil {
		usage, err = protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}.Marshal(result.GetUsage())
		if err != nil {
			return Result{}, err
		}
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
	var verifiedAt any
	if healthVerified {
		completedAt = closure.GetVerifiedAt()
		verifiedAt = timestamp(closure.GetVerifiedAt())
	}
	var mitigatedAt any
	if result != nil {
		mitigatedAt = timestamp(closure.GetResponderCompletedAt())
	}
	var commit any
	if repairCommit != "" {
		commit = repairCommit
	}
	record := map[string]any{
		"schema_version": 1,
		"incident_id":    request.GetIncidentId(), "source_commit": request.GetSourceCommit(), "deployed_commit": request.GetDeployedCommit(),
		"detector_history": detectorHistory, "findings": findings,
		"surfaced_playbooks": surfaced, "inspected_playbooks": surfaced, "confirmed_playbooks": applied,
		"rejected_playbooks": rejected, "applied_playbooks": applied,
		"confirmed_root_causes": rootCauses, "final_health_detector_state": finalStates,
		"classification": classification, "repair_commit": commit, "repair_actions": repairActions, "memory_commit": commit,
		"responder_backend": backend, "responder_model": model, "usage": usage,
		"timestamps": map[string]any{
			"detected_at": timestamp(closure.GetDetectedAt()), "dispatched_at": timestamp(closure.GetDispatchedAt()),
			"mitigated_at": mitigatedAt, "verified_at": verifiedAt, "completed_at": timestamp(completedAt),
		},
		"diagnosis_verification": verifications,
	}
	document, err := json.Marshal(record)
	if err != nil {
		return Result{}, err
	}
	typed := new(contractsv1alpha1.OutcomeRecord)
	if err := protojson.Unmarshal(document, typed); err != nil {
		return Result{}, fmt.Errorf("derived outcome violates contract: %w", err)
	}
	reflect := false
	if classification == "success" {
		for _, verification := range verifications {
			if verification.Verdict == "confirmed" {
				reflect = true
				break
			}
		}
	}
	return Result{Record: typed, RequiresReflection: reflect}, nil
}

func protoList[T proto.Message](items []T) ([]json.RawMessage, error) {
	result := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		data, err := protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}.Marshal(item)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}

func timestamp(value *timestamppb.Timestamp) string {
	return value.AsTime().UTC().Format(time.RFC3339Nano)
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

func classify(closure *contractsv1alpha1.IncidentClosure, repairCommit string, healthVerified bool, verifications []rootCauseVerification) string {
	result := closure.GetResult()
	if closure.GetRequest() == nil {
		return "failed"
	}
	// The controller owns the missed-fault flag in prior Python closures. This
	// proto does not carry it; no negative claim is inferred from its absence.
	if closure.GetDispatchError() != "" || result == nil || result.GetStatus() == "failed" {
		return "failed"
	}
	if !healthVerified {
		if result.GetStatus() == "cancelled" {
			return "cancelled"
		}
		return "partial"
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
			return "cleared_without_action"
		}
	}
	if result.GetStatus() == "cancelled" {
		return "cancelled"
	}
	if len(result.GetConfirmedRootCauses()) == 0 {
		return "false_positive"
	}
	if len(verifications) > 0 {
		attributed := false
		for _, verification := range verifications {
			if verification.Repair.Attributed {
				attributed = true
				break
			}
		}
		if !attributed {
			return "external_recovery"
		}
	}
	return "success"
}
