// Package lifecyclecore owns pure lifecycle transitions. The runtime driver
// supplies events and executes returned values; this package cannot perform I/O.
package lifecyclecore

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type EventKind uint8

const (
	Begin EventKind = iota + 1
	Drive
	Rejected
	Committed
	Acknowledged
	AcknowledgmentRejected
	Canceled
	Restart
	ArchivesPersisted
	AgentTimeout
	Superseded
	ResponseExpired
)

type RetryPolicy struct {
	MaxAttempts    int32
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Event struct {
	Kind                EventKind
	At                  time.Time
	EffectID            string
	Closure             *contracts.IncidentClosure
	Receipt             *contracts.ClosureReceipt
	Error               string
	Policy              RetryPolicy
	ArchivedIncidentIDs []string
}

type EffectKind uint8

const (
	ProcessClosure EffectKind = iota + 1
	AcknowledgeClosure
)

type Effect struct {
	Kind    EffectKind
	ID      string
	Closure *contracts.IncidentClosure
	Receipt *contracts.ClosureReceipt
}

// Identity is independent of retry time and process lifetime. Length prefixes
// avoid ambiguity even when an external incident ID contains separators.
func Identity(incident, acceptedCommit string, step EffectKind) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d:%s%d:%s:%d", len(incident), incident, len(acceptedCommit), acceptedCommit, step))))
}

// BlocksNext keeps terminal failure local to its incident. The failed closure
// remains available, and its archive outbox is saved before any new effect.
func BlocksNext(phase contracts.ClosureState, pending bool) bool {
	return phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED || phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING || (pending && phase != contracts.ClosureState_CLOSURE_STATE_FAILED)
}

// Step is the implementation, not a separate model of the driver. It owns
// closure retry/commit/ack decisions and preserves all unrelated state fields.
// Inputs are never mutated, including event payloads retained by a transition.
func Step(state *contracts.ControllerState, event Event) (*contracts.ControllerState, []Effect) {
	next := &contracts.ControllerState{}
	if state != nil {
		next = proto.Clone(state).(*contracts.ControllerState)
	}
	pending := next.GetPendingClosure()
	phase := next.GetClosureState()
	if event.Kind == Begin {
		if event.Closure == nil || event.Closure.GetRequest().GetIncidentId() == "" || BlocksNext(next.GetClosureState(), next.GetPendingClosure() != nil) {
			return next, nil
		}
		next.PendingClosure = proto.Clone(event.Closure).(*contracts.IncidentClosure)
		next.ClosureState = contracts.ClosureState_CLOSURE_STATE_PENDING
		next.ClosureReceipt, next.ClosureFailure = nil, nil
		return next, nil
	}
	if event.Kind == Restart {
		switch phase {
		case contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED:
			if pending != nil {
				next.ClosureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			}
		case contracts.ClosureState_CLOSURE_STATE_PROCESSING:
			next.ClosureState = contracts.ClosureState_CLOSURE_STATE_PENDING
		case contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING:
			next.ClosureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
		}
		return next, nil
	}
	if event.Kind == ArchivesPersisted {
		next.FailedClosures = slices.DeleteFunc(next.FailedClosures, func(record *contracts.FailedIncidentClosure) bool {
			return slices.Contains(event.ArchivedIncidentIDs, record.GetFailure().GetIncidentId())
		})
		return next, nil
	}
	if event.Kind == ResponseExpired && phase != contracts.ClosureState_CLOSURE_STATE_PENDING && phase != contracts.ClosureState_CLOSURE_STATE_PROCESSING {
		return next, nil
	}
	if event.Kind == Superseded || event.Kind == ResponseExpired {
		if pending == nil || phase == contracts.ClosureState_CLOSURE_STATE_FAILED {
			return next, nil
		}
		failure := &contracts.ClosureFailure{IncidentId: pending.GetRequest().GetIncidentId(), Attempts: 1, MaxAttempts: 1, LastError: event.Error, FirstFailedAt: timestamppb.New(event.At), LastFailedAt: timestamppb.New(event.At), Permanent: true, Reason: contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_BROKER_ERROR, Action: "obsolete health evidence archived; investigate with the adopted health policy"}
		if event.Kind == ResponseExpired {
			failure.Reason = contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_AGENT_TIMEOUT
			failure.Action = "expired incident without live work archived; detect a fresh incident"
		}
		next.ClosureFailure = failure
		next.ClosureReceipt = nil
		next.ClosureState = contracts.ClosureState_CLOSURE_STATE_FAILED
		next.FailedClosures = append(next.FailedClosures, &contracts.FailedIncidentClosure{Closure: proto.Clone(pending).(*contracts.IncidentClosure), Failure: proto.Clone(failure).(*contracts.ClosureFailure)})
		return next, nil
	}
	receipt := next.GetClosureReceipt()
	if pending == nil && (receipt == nil || (phase != contracts.ClosureState_CLOSURE_STATE_COMMITTED && phase != contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING)) {
		return next, nil
	}
	id := pending.GetRequest().GetIncidentId()
	if id == "" {
		id = receipt.GetIncidentId()
	}
	processID := Identity(id, pending.GetRequest().GetRepositoryBaseCommit(), ProcessClosure)
	ackID := Identity(id, receipt.GetOutcomeCommit()+"\x00"+receipt.GetReflectionCommit(), AcknowledgeClosure)
	if event.Kind != Drive && event.EffectID != processID && event.EffectID != ackID {
		return next, nil
	}
	switch event.Kind {
	case Drive:
		switch phase {
		case contracts.ClosureState_CLOSURE_STATE_PENDING:
			if event.EffectID != "" && event.EffectID != processID {
				return next, nil
			}
			failure := next.GetClosureFailure()
			if failure.GetPermanent() || (failure.GetNextAttemptAt() != nil && event.At.Before(failure.NextAttemptAt.AsTime())) {
				return next, nil
			}
			next.ClosureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
			return next, []Effect{{Kind: ProcessClosure, ID: processID, Closure: proto.Clone(pending).(*contracts.IncidentClosure)}}
		case contracts.ClosureState_CLOSURE_STATE_COMMITTED:
			if event.EffectID != "" && event.EffectID != ackID {
				return next, nil
			}
			if receipt == nil {
				return next, nil
			}
			next.ClosureState = contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING
			return next, []Effect{{Kind: AcknowledgeClosure, ID: ackID, Receipt: proto.Clone(receipt).(*contracts.ClosureReceipt)}}
		}
	case Committed:
		if event.EffectID != processID || (phase != contracts.ClosureState_CLOSURE_STATE_PROCESSING && phase != contracts.ClosureState_CLOSURE_STATE_PENDING) {
			return next, nil
		}
		if event.Receipt.GetIncidentId() != id || event.Receipt.GetOutcomeCommit() == "" || event.Receipt.GetAckToken() == "" {
			event.Kind, event.Error = Rejected, "broker returned invalid closure receipt"
			return reject(next, event, contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_INVALID_RECEIPT), nil
		}
		next.ClosureReceipt = proto.Clone(event.Receipt).(*contracts.ClosureReceipt)
		next.ClosureFailure = nil
		next.ClosureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
	case Rejected, Canceled, AgentTimeout:
		if event.EffectID != processID || (phase != contracts.ClosureState_CLOSURE_STATE_PROCESSING && phase != contracts.ClosureState_CLOSURE_STATE_PENDING) {
			return next, nil
		}
		next.ClosureState = contracts.ClosureState_CLOSURE_STATE_PENDING
		if event.Kind == AgentTimeout {
			return reject(next, event, contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_AGENT_TIMEOUT), nil
		}
		if event.Kind == Rejected {
			return reject(next, event, contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_BROKER_ERROR), nil
		}
	case AcknowledgmentRejected:
		if event.EffectID == ackID && phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
			next.ClosureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
		}
	case Acknowledged:
		if event.EffectID != ackID || phase != contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
			return next, nil
		}
		next.LastAcknowledgedIncidentId = id
		next.PendingClosure, next.ClosureReceipt, next.ClosureFailure = nil, nil, nil
		next.ClosureState = contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED
	}
	return next, nil
}

func reject(next *contracts.ControllerState, event Event, reason contracts.ClosureFailureReason) *contracts.ControllerState {
	id := next.GetPendingClosure().GetRequest().GetIncidentId()
	failure := next.GetClosureFailure()
	if failure == nil {
		failure = &contracts.ClosureFailure{IncidentId: id, FirstFailedAt: timestamppb.New(event.At)}
	}
	failure.Attempts++
	if failure.MaxAttempts <= 0 {
		failure.MaxAttempts = max(1, event.Policy.MaxAttempts)
	}
	failure.LastError = event.Error
	failure.LastFailedAt = timestamppb.New(event.At)
	failure.Reason = reason
	next.ClosureFailure = failure
	next.ClosureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	if failure.Attempts < failure.MaxAttempts {
		delay := event.Policy.InitialBackoff
		for attempt := int32(1); attempt < failure.Attempts; attempt++ {
			if delay >= event.Policy.MaxBackoff/2 {
				delay = event.Policy.MaxBackoff
				break
			}
			delay *= 2
		}
		failure.NextAttemptAt = timestamppb.New(event.At.Add(min(delay, event.Policy.MaxBackoff)))
		return next
	}
	failure.NextAttemptAt = nil
	failure.Permanent = true
	failure.Reason = contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_RETRY_EXHAUSTED
	failure.Action = "closure evidence is archived per incident; inspect the broker ledger and failed-closure ConfigMap; later incidents continue"
	next.ClosureState = contracts.ClosureState_CLOSURE_STATE_FAILED
	next.FailedClosures = append(next.FailedClosures, &contracts.FailedIncidentClosure{Closure: proto.Clone(next.PendingClosure).(*contracts.IncidentClosure), Failure: proto.Clone(failure).(*contracts.ClosureFailure)})
	return next
}
