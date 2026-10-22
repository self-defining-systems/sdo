package outcome_derivation

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// ObserveController interprets a read-only controller snapshot for API readers.
// It neither validates a new controller publication nor changes lifecycle state.
// Generated decoding and closed-enum checks replace readers' string parsing.
func ObserveController(data []byte) *c.ControllerObservation {
	result := &c.ControllerObservation{}
	reject := func(message string) *c.ControllerObservation {
		result.Failure = &c.ControllerObservationFailure{Code: c.ControllerObservationFailure_CODE_INVALID_SNAPSHOT, Message: message}
		return result
	}
	request := &c.ControllerObservationRequest{}
	if err := protojson.Unmarshal(data, request); err != nil {
		return reject("invalid controller observation ProtoJSON: " + err.Error())
	}
	state := request.GetState()
	if state == nil {
		return reject("controller observation requires a state snapshot")
	}
	if state.GetClosureState().Descriptor().Values().ByNumber(state.GetClosureState().Number()) == nil || state.GetDispatchState().Descriptor().Values().ByNumber(state.GetDispatchState().Number()) == nil {
		return reject("controller snapshot contains an undefined lifecycle state")
	}
	ids := map[string]bool{}
	for _, id := range []string{state.GetLastAcknowledgedIncidentId(), state.GetPendingClosure().GetRequest().GetIncidentId(), state.GetIncidentRequest().GetIncidentId()} {
		if id != "" {
			ids[id] = true
		}
	}
	for id := range ids {
		result.KnownIncidentIds = append(result.KnownIncidentIds, id)
	}
	sort.Strings(result.KnownIncidentIds)
	result.NewIncidentOpened = state.GetIncidentOpen()
	for id := range ids {
		if !slices.Contains(request.GetKnownIncidentIds(), id) {
			result.NewIncidentOpened = true
		}
	}
	result.Acknowledged = request.GetIncidentId() != "" && state.GetLastAcknowledgedIncidentId() == request.GetIncidentId()
	result.OpenIncidentSummary = controllerSummary(state)
	failure := state.GetClosureFailure()
	if state.GetClosureState() == c.ClosureState_CLOSURE_STATE_FAILED && failure.GetPermanent() && (request.GetIncidentId() == "" || failure.GetIncidentId() == request.GetIncidentId()) {
		id := failure.GetIncidentId()
		if id == "" {
			id = request.GetIncidentId()
		}
		if id == "" {
			id = "unknown"
		}
		result.Failure = &c.ControllerObservationFailure{Code: c.ControllerObservationFailure_CODE_CLOSURE_FAILED, IncidentId: id, Message: fmt.Sprintf("incident '%s' closure permanently failed after %d broker attempts: %s; %s", id, failure.GetAttempts(), failure.GetLastError(), failure.GetAction())}
		return result
	}
	closure := state.GetPendingClosure()
	id := closure.GetRequest().GetIncidentId()
	if id != "" && !slices.Contains(request.GetKnownIncidentIds(), id) && observedTimestamp(closure.GetVerifiedAt()) {
		result.VerifiedIncidentId = id
		result.VerifiedClosure = proto.Clone(closure).(*c.IncidentClosure)
		return result
	}
	if id = state.GetLastAcknowledgedIncidentId(); id != "" && !slices.Contains(request.GetKnownIncidentIds(), id) {
		result.VerifiedIncidentId = id
		return result
	}
	if request.GetStopOnReview() && state.GetDetectorReviewRequired() {
		id = state.GetIncidentRequest().GetIncidentId()
		if id == "" {
			id = "unknown"
		}
		result.Failure = &c.ControllerObservationFailure{Code: c.ControllerObservationFailure_CODE_DETECTOR_REVIEW_REQUIRED, IncidentId: id, Message: state.GetDetectorReviewReason()}
		if result.Failure.Message == "" {
			result.Failure.Message = "no reason recorded"
		}
	}
	return result
}

func observedTimestamp(stamp *timestamppb.Timestamp) bool {
	return stamp != nil && stamp.CheckValid() == nil && !stamp.AsTime().IsZero()
}

func clipObservation(text string, limit int) string {
	runes := []rune(text)
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return text
}

func controllerSummary(state *c.ControllerState) string {
	dispatch := state.GetDispatchState().String()
	if state.GetDispatchState() == c.DispatchState_DISPATCH_STATE_UNSPECIFIED {
		dispatch = "unknown"
	}
	if !state.GetIncidentOpen() {
		summary := fmt.Sprintf("; no incident open (dispatch state '%s')", dispatch)
		if failure := state.GetClosureFailure(); failure.GetPermanent() {
			summary += fmt.Sprintf("; closure of '%s' failed permanently: %s", failure.GetIncidentId(), clipObservation(strings.TrimSpace(failure.GetLastError()), 500))
		}
		return summary
	}
	id := state.GetIncidentRequest().GetIncidentId()
	if id == "" {
		id = "unknown"
	}
	parts := []string{fmt.Sprintf("; incident '%s' is open", id)}
	if result := state.GetIncidentResult(); result != nil {
		parts = append(parts, fmt.Sprintf("responder status '%s'", result.GetStatus()))
		if len(result.GetRepairChanges()) != 0 {
			parts = append(parts, "responder notes: "+clipObservation(strings.Join(result.GetRepairChanges(), "; "), 500))
		}
	} else if state.GetDispatchError() != "" {
		parts = append(parts, "dispatch error: "+state.GetDispatchError())
	} else {
		parts = append(parts, fmt.Sprintf("responder not done (dispatch state '%s')", dispatch))
	}
	if state.GetDetectorReviewRequired() {
		reason := state.GetDetectorReviewReason()
		if reason == "" {
			reason = "no reason recorded"
		}
		parts = append(parts, "detector review required: "+reason)
	}
	return strings.Join(parts, ", ")
}
