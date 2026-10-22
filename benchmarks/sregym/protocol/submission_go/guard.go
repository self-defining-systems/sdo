// Package submission owns SREGym submission phase decisions. It has no HTTP,
// Kubernetes, wall clock or model dependency and is outside production SDO.
package submission

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func terminalStage(stage string) bool {
	values := c.ConductorTerminalStage(0).Descriptor().Values()
	for i := 0; i < values.Len(); i++ {
		value := values.Get(i)
		if value.Number() != 0 && strings.ToLower(strings.TrimPrefix(string(value.Name()), "CONDUCTOR_TERMINAL_STAGE_")) == stage {
			return true
		}
	}
	return false
}

func rejection(code c.SubmissionGuardFailure_Code, message string) *c.SubmissionGuardDecision {
	return &c.SubmissionGuardDecision{Failure: &c.SubmissionGuardFailure{Code: code, Message: message}}
}

// Decide authorizes a POST only while the conductor awaits exactly that phase.
// A received acknowledgment is not permission to submit another phase, and a
// completion poll can never authorize a second POST.
func Decide(data []byte) *c.SubmissionGuardDecision {
	request := &c.SubmissionGuardRequest{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, request); err != nil {
		return rejection(c.SubmissionGuardFailure_CODE_INVALID_WIRE, "invalid submission guard ProtoJSON: "+err.Error())
	}
	if !slices.Contains(wire.StringIn(request, "phase"), request.GetPhase()) {
		return rejection(c.SubmissionGuardFailure_CODE_INVALID_PHASE, "submission phase must be diagnosis or mitigation")
	}
	if (request.GetStatus() == nil) == (request.GetAcknowledgement() == nil) {
		return rejection(c.SubmissionGuardFailure_CODE_INVALID_WIRE, "exactly one conductor status or acknowledgement is required")
	}
	if ack := request.GetAcknowledgement(); ack != nil {
		status := ack.GetStatus()
		if status == "done" {
			return &c.SubmissionGuardDecision{Action: c.SubmissionGuardDecision_ACTION_RETURN}
		}
		if !slices.Contains([]string{"200", "ok", "acknowledged"}, status) {
			return rejection(c.SubmissionGuardFailure_CODE_NOT_ACKNOWLEDGED, fmt.Sprintf("SREGym %s submission was not acknowledged", request.GetPhase()))
		}
		if request.GetPhase() == "diagnosis" {
			return &c.SubmissionGuardDecision{Action: c.SubmissionGuardDecision_ACTION_RETURN}
		}
		return &c.SubmissionGuardDecision{Action: c.SubmissionGuardDecision_ACTION_WAIT}
	}
	stage := request.GetStatus().GetStage()
	terminal := terminalStage(stage)
	result := &c.SubmissionGuardDecision{Action: c.SubmissionGuardDecision_ACTION_WAIT}
	if request.GetSubmissionSent() {
		if terminal {
			result.Action = c.SubmissionGuardDecision_ACTION_RETURN
			result.Response = &c.SubmissionBridgeResponse{Done: &c.SubmissionPhaseStatus{Status: stage}}
		}
		return result
	}
	if stage == request.GetPhase() {
		result.Action = c.SubmissionGuardDecision_ACTION_POST
		return result
	}
	if terminal && request.GetPhase() == "mitigation" {
		result.Action = c.SubmissionGuardDecision_ACTION_RETURN
		result.Response = &c.SubmissionBridgeResponse{Mitigation: &c.SubmissionPhaseStatus{Status: "already_submitted"}, Done: &c.SubmissionPhaseStatus{Status: stage}}
		return result
	}
	if request.GetPhase() == "diagnosis" && (stage == "mitigation" || terminal) {
		reason := "the diagnosis was already submitted and the harness is awaiting mitigation. Keep repairing and verifying; run the mitigation submission once when done. Never resubmit a diagnosis: it would be graded as your mitigation."
		if terminal {
			reason = "the problem has already ended; nothing more can be submitted."
		}
		result.Action = c.SubmissionGuardDecision_ACTION_RETURN
		result.Response = &c.SubmissionBridgeResponse{Diagnosis: &c.SubmissionPhaseStatus{Status: "already_submitted", Stage: proto.String(stage), Message: proto.String(fmt.Sprintf("diagnosis not submitted: harness stage is '%s'; %s", stage, reason))}}
	}
	return result
}
