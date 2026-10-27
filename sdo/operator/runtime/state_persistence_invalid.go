package runtime

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func clonePersistenceFailure(failure *contracts.StatePersistenceFailure) *contracts.StatePersistenceFailure {
	if failure == nil {
		return nil
	}
	return proto.Clone(failure).(*contracts.StatePersistenceFailure)
}

// recoverInvalidState fails the incident, not the persistence loop. Roll back
// bookkeeping to the last validated checkpoint and publish a minimal failed
// closure. No invalid mutation or health evidence is promoted into authority.
// The failed result retains the provider session so reflection can still run.
func (c *Controller) recoverInvalidState(invalid error) (*contracts.ControllerState, error) {
	c.mu.Lock()
	baseline := c.durable
	closure := cloneIncidentClosure(c.pendingClosure)
	request := cloneIncidentRequest(c.currentIncidentRequest)
	result := cloneIncidentResult(c.currentIncidentResult)
	c.durable = nil // no side effect may run until the replacement is saved
	c.mu.Unlock()
	state := &contracts.ControllerState{Version: RuntimeStateVersion}
	if baseline != nil {
		state = proto.Clone(baseline).(*contracts.ControllerState)
	}
	if closure != nil {
		request = &closure.Request
		result = closure.Result
	}
	var typedRequest *contracts.IncidentRequest
	if request != nil {
		typedRequest, _ = incidentRequestContract(*request)
		if typedRequest != nil && wire.Validate(typedRequest) != nil {
			typedRequest = nil
		}
	}
	if typedRequest == nil {
		typedRequest = state.GetIncidentRequest()
	}
	if typedRequest == nil {
		typedRequest = state.GetPendingClosure().GetRequest()
	}
	if typedRequest == nil {
		return nil, fmt.Errorf("state_persistence_invalid without incident identity: %w", invalid)
	}
	at := c.clock()
	if previous := state.GetPendingClosure(); previous != nil {
		for _, stamp := range []*timestamppb.Timestamp{previous.DetectedAt, previous.DispatchedAt, previous.ResponderCompletedAt, previous.VerifiedAt} {
			if stamp != nil && stamp.IsValid() && stamp.AsTime().After(at) {
				at = stamp.AsTime()
			}
		}
	}
	now := timestamppb.New(at)
	failure := &contracts.StatePersistenceFailure{Reason: wire.StringConst(&contracts.StatePersistenceFailure{}, "reason"), Detail: invalid.Error()}
	failedResult := &contracts.IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentId: typedRequest.GetIncidentId(), Status: "failed", Usage: &contracts.UsageMetrics{}, Timing: &contracts.TimingMetrics{StartedAt: now, CompletedAt: now}}
	if result != nil {
		failedResult.ResponderSessionId = proto.String(result.ResponderSessionID)
		if wire.Validate(&result.Usage) == nil {
			failedResult.Usage = proto.Clone(&result.Usage).(*contracts.UsageMetrics)
		}
	}
	failedClosure := &contracts.IncidentClosure{Request: typedRequest, Result: failedResult, DispatchError: proto.String(failure.Reason + ": " + failure.Detail), DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now, VerifiedAt: now, StatePersistenceFailure: failure}
	if previous := state.GetPendingClosure(); previous != nil && previous.GetRequest().GetIncidentId() == typedRequest.GetIncidentId() {
		failedClosure.DetectedAt = previous.DetectedAt
		failedClosure.DispatchedAt = previous.DispatchedAt
	}
	state.IncidentRequest = typedRequest
	state.IncidentResult = failedResult
	state.PendingClosure = failedClosure
	state.ClosureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	state.ClosureReceipt = nil
	state.ClosureFailure = nil
	state.IncidentOpen = false
	state.ResponderDone = true
	state.DispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	state.DispatchError = failedClosure.GetDispatchError()
	if err := wire.Validate(state); err != nil {
		return nil, fmt.Errorf("state_persistence_invalid replacement: %w", err)
	}
	if err := c.RestoreState(state); err != nil {
		return nil, err
	}
	return state, nil
}
