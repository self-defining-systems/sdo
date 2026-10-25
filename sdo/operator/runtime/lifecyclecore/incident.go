package lifecyclecore

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// IncidentEvent contains facts supplied by the driver. Detection, independent
// health verification, transport classification and persistence stay at edges.
type IncidentEvent struct {
	Kind                IncidentEventKind
	EffectID            string
	At                  time.Time
	Request             *contracts.IncidentRequest
	Result              *contracts.IncidentResult
	Error               string
	Terminal            bool
	Verified            bool
	FinalDetectorStates []*contracts.DetectorEvaluation
	VerificationTimeout time.Duration
}

type IncidentEventKind uint8

const (
	IncidentBegin IncidentEventKind = iota + 1
	IncidentDrive
	IncidentResponseCompleted
	IncidentVerify
	IncidentRestart
	IncidentEffectStarted
	IncidentWorkspacePrepared
)

type IncidentEffectKind uint8

const (
	IncidentPrepareWorkspace IncidentEffectKind = iota + 1
	IncidentDispatch
	IncidentFreezeClosure
)

// IncidentEffect is data. Its identity and payload survive retries/restarts;
// executing it still requires the driver's accepted-state durability barrier.
type IncidentEffect struct {
	Kind    IncidentEffectKind
	ID      string
	Request *contracts.IncidentRequest
	Closure *contracts.IncidentClosure
}

// StepIncident is a pure decision seam for the controller's incident slice.
// It consumes generated state and returns owned copies; it never executes work.
// The C1-owned driver can adopt this seam independently of the closure machine.
func StepIncident(state *contracts.ControllerState, event IncidentEvent) (*contracts.ControllerState, []IncidentEffect) {
	next := &contracts.ControllerState{}
	if state != nil {
		next = proto.Clone(state).(*contracts.ControllerState)
	}
	if event.Kind == IncidentBegin {
		if next.GetIncidentOpen() || BlocksNext(next.GetClosureState(), next.GetPendingClosure() != nil) || event.Request.GetIncidentId() == "" {
			return next, nil
		}
		next.IncidentOpen = true
		next.IncidentRequest = proto.Clone(event.Request).(*contracts.IncidentRequest)
		next.IncidentResult, next.ResponderCompletedAt = nil, nil
		next.ResponderDone, next.DispatchError = false, ""
		next.IncidentDetectedAt = timestamppb.New(event.At)
		next.IncidentDispatchedAt = nil
		next.DispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
		return next, nil
	}
	request := next.GetIncidentRequest()
	if !next.GetIncidentOpen() || request == nil {
		return next, nil
	}
	if event.Kind == IncidentRestart {
		switch next.GetDispatchState() {
		case contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING:
			next.DispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
		case contracts.DispatchState_DISPATCH_STATE_RUNNING:
			next.DispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
		}
	}
	if event.Kind == IncidentEffectStarted {
		switch next.GetDispatchState() {
		case contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING:
			if event.EffectID == incidentIdentity(request, IncidentPrepareWorkspace) {
				next.DispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING
			}
		case contracts.DispatchState_DISPATCH_STATE_PENDING:
			if event.EffectID == incidentIdentity(request, IncidentDispatch) {
				next.DispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
				if next.IncidentDispatchedAt == nil {
					next.IncidentDispatchedAt = timestamppb.New(event.At)
				}
			}
		}
		return next, nil
	}
	if event.Kind == IncidentWorkspacePrepared {
		if (next.GetDispatchState() != contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING && next.GetDispatchState() != contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING) || event.Request.GetIncidentId() != request.GetIncidentId() || event.Request.GetRepositoryWorktree() == "" || event.Request.GetRepositoryBaseCommit() == "" {
			return next, nil
		}
		next.IncidentRequest.RepositoryWorktree = event.Request.GetRepositoryWorktree()
		next.IncidentRequest.RepositoryBaseCommit = event.Request.GetRepositoryBaseCommit()
		next.DispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
		return next, nil
	}
	if event.Kind == IncidentResponseCompleted {
		// Completions correlate by the incident identity even if transport returned
		// no result. Late completions from a prior incident cannot finish this one.
		if event.Request.GetIncidentId() != request.GetIncidentId() || next.GetResponderDone() || (next.GetDispatchState() != contracts.DispatchState_DISPATCH_STATE_PENDING && next.GetDispatchState() != contracts.DispatchState_DISPATCH_STATE_RUNNING) {
			return next, nil
		}
		next.DispatchError = event.Error
		next.IncidentResult = nil
		if event.Error != "" && !event.Terminal {
			next.ResponderDone = false
			next.ResponderCompletedAt = nil
			next.DispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
			return next, nil
		}
		next.ResponderDone = true
		completed := event.At
		if at := next.GetIncidentDispatchedAt(); at != nil && completed.Before(at.AsTime()) {
			completed = at.AsTime()
		}
		next.ResponderCompletedAt = timestamppb.New(completed)
		next.DispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
		if event.Error == "" && event.Result != nil {
			next.IncidentResult = proto.Clone(event.Result).(*contracts.IncidentResult)
		}
		return next, nil
	}
	if event.Kind == IncidentVerify && next.GetResponderDone() && next.GetResponderCompletedAt() != nil {
		if !event.Verified && event.At.Before(next.ResponderCompletedAt.AsTime().Add(event.VerificationTimeout)) {
			return next, nil
		}
		closure := &contracts.IncidentClosure{
			Request:    proto.Clone(request).(*contracts.IncidentRequest),
			DetectedAt: next.IncidentDetectedAt, DispatchedAt: next.IncidentDispatchedAt,
			ResponderCompletedAt: next.ResponderCompletedAt, VerifiedAt: timestamppb.New(event.At),
		}
		if next.IncidentResult != nil {
			closure.Result = proto.Clone(next.IncidentResult).(*contracts.IncidentResult)
		}
		if event.Verified {
			for _, detector := range event.FinalDetectorStates {
				if detector != nil {
					closure.FinalDetectorStates = append(closure.FinalDetectorStates, proto.Clone(detector).(*contracts.DetectorEvaluation))
				}
			}
		}
		next.IncidentOpen = false
		// Freeze through the same closure machine, so terminal failures and the
		// archive outbox retain their existing ownership and admission semantics.
		next, _ = Step(next, Event{Kind: Begin, At: event.At, Closure: closure})
		return next, []IncidentEffect{{Kind: IncidentFreezeClosure, ID: incidentIdentity(request, IncidentFreezeClosure), Closure: proto.Clone(closure).(*contracts.IncidentClosure)}}
	}
	if event.Kind == IncidentDrive {
		kind := IncidentEffectKind(0)
		switch next.GetDispatchState() {
		case contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING:
			kind = IncidentPrepareWorkspace
		case contracts.DispatchState_DISPATCH_STATE_PENDING:
			kind = IncidentDispatch
		}
		if kind != 0 {
			return next, []IncidentEffect{{Kind: kind, ID: incidentIdentity(request, kind), Request: proto.Clone(request).(*contracts.IncidentRequest)}}
		}
	}
	return next, nil
}

func incidentIdentity(request *contracts.IncidentRequest, kind IncidentEffectKind) string {
	return fmt.Sprintf("%s/incident/%d", Identity(request.GetIncidentId(), request.GetRepositoryBaseCommit(), ProcessClosure), kind)
}
