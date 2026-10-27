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

type ReflectionEventKind uint8

const (
	ReflectionBegin ReflectionEventKind = iota + 1
	ReflectionDrive
	ReflectionInspected
	ReflectionCompleted
	ReflectionReviewed
	ReflectionPublished
	ReflectionRejected
	ReflectionReset
	ReflectionCleaned
	ReflectionFault
)

type ReflectionEffectKind uint8

const (
	InspectReflection ReflectionEffectKind = iota + 1
	DispatchReflection
	ReviewReflection
	PublishReflection
	ResetReflection
	CleanupReflection
)

type ReflectionEvent struct {
	Kind        ReflectionEventKind
	EffectID    string
	At          time.Time
	MaxRetries  uint32
	Workspace   *contracts.ProposalWorkspace
	Paths       []string
	Commit      string
	Decision    *contracts.ReflectionDecision
	Diagnostics []*contracts.MemoryDiagnostic
}

type ReflectionEffect struct {
	IncidentID       string
	ProposalCommit   string
	OutcomeCommit    string
	Kind             ReflectionEffectKind
	ID               string
	Workspace        *contracts.ProposalWorkspace
	Closure          *contracts.IncidentClosure
	Outcome          *contracts.OutcomeRecord
	Decision         *contracts.ReflectionDecision
	Attempt          uint32
	Diagnostics      []*contracts.MemoryDiagnostic
	Paths            []string
	DispatchAt       *timestamppb.Timestamp
	NoRepairCount uint32
}

func ReflectionFeedback(ledger *contracts.BrokerLedger) (uint32, []*contracts.MemoryDiagnostic) {
	rejections := ledger.GetReflectionRejections()
	if len(rejections) == 0 {
		return 1, nil
	}
	diagnostics := make([]*contracts.MemoryDiagnostic, len(rejections[len(rejections)-1].GetDiagnostics()))
	for i, diagnostic := range rejections[len(rejections)-1].GetDiagnostics() {
		diagnostics[i] = proto.Clone(diagnostic).(*contracts.MemoryDiagnostic)
	}
	return uint32(len(rejections) + 1), diagnostics
}

func reflectionIdentity(state *contracts.BrokerLedger, kind ReflectionEffectKind) string {
	attempt, _ := ReflectionFeedback(state)
	// A correction is a new step, not a transport retry of the rejected task.
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d:%s%d:%s:reflection/%d/%d/%d", len(state.GetIncidentId()), state.GetIncidentId(), len(state.GetOutcomeCommit()), state.GetOutcomeCommit(), state.GetReflectionPhase(), kind, attempt))))
}

// StepReflection owns dispatch, completed-result retention, typed rejections and
// reset/publication ordering. Files, validation Jobs and commits remain one
// atomic trusted broker publication effect at the edge.
func StepReflection(state *contracts.BrokerLedger, event ReflectionEvent) (*contracts.BrokerLedger, []ReflectionEffect) {
	next := &contracts.BrokerLedger{}
	if state != nil {
		next = proto.Clone(state).(*contracts.BrokerLedger)
	}
	if event.Kind == ReflectionBegin {
		eligibility := next.GetOutcomeDocument().GetReflectionEligibility()
		if next.ReflectionNoRepairCount == nil && eligibility != nil && eligibility.NoRepairCount != nil {
			count := eligibility.GetNoRepairCount()
			next.ReflectionNoRepairCount = &count
		}

		if next.ReflectionCommit != "" {
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_DONE
			return next, nil
		}
		if next.ReflectionPhase == contracts.ReflectionPhase_REFLECTION_PHASE_UNSPECIFIED {
			next.ReflectionMaxRetries = event.MaxRetries
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_INSPECT
			if next.ReflectionRejected {
				next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_RESET
			}
		}
		return next, nil
	}
	phase := next.GetReflectionPhase()
	var kind ReflectionEffectKind
	switch phase {
	case contracts.ReflectionPhase_REFLECTION_PHASE_INSPECT, contracts.ReflectionPhase_REFLECTION_PHASE_INSPECT_RESULT:
		kind = InspectReflection
	case contracts.ReflectionPhase_REFLECTION_PHASE_DISPATCH:
		kind = DispatchReflection
	case contracts.ReflectionPhase_REFLECTION_PHASE_REVIEW:
		kind = ReviewReflection
	case contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH:
		kind = PublishReflection
	case contracts.ReflectionPhase_REFLECTION_PHASE_RESET:
		kind = ResetReflection
	case contracts.ReflectionPhase_REFLECTION_PHASE_FAILED_CLEANUP:
		kind = CleanupReflection
	default:
		return next, nil
	}
	identity := reflectionIdentity(next, kind)
	if event.Kind == ReflectionDrive {
		attempt, diagnostics := ReflectionFeedback(next)
		effect := ReflectionEffect{IncidentID: next.GetIncidentId(), ProposalCommit: next.GetProposalCommit(), OutcomeCommit: next.GetOutcomeCommit(), Kind: kind, ID: identity, Attempt: attempt, Diagnostics: diagnostics, Paths: slices.Clone(next.ReflectionPaths), NoRepairCount: next.GetReflectionNoRepairCount()}
		if next.ReflectionWorkspace != nil {
			effect.Workspace = proto.Clone(next.ReflectionWorkspace).(*contracts.ProposalWorkspace)
		}
		if next.Closure != nil {
			effect.Closure = proto.Clone(next.Closure).(*contracts.IncidentClosure)
		}
		if next.OutcomeDocument != nil {
			effect.Outcome = proto.Clone(next.OutcomeDocument).(*contracts.OutcomeRecord)
		}
		if next.ReflectionDecision != nil {
			effect.Decision = proto.Clone(next.ReflectionDecision).(*contracts.ReflectionDecision)
		}
		if next.ReflectionDispatchAt != nil {
			effect.DispatchAt = proto.Clone(next.ReflectionDispatchAt).(*timestamppb.Timestamp)
		}
		return next, []ReflectionEffect{effect}
	}
	if event.EffectID != identity {
		return next, nil
	}
	switch event.Kind {
	case ReflectionInspected:
		if kind != InspectReflection {
			return next, nil
		}
		if event.Commit != "" {
			next.ReflectionCommit = event.Commit
			next.ReflectionPaths = slices.Clone(event.Paths)
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_DONE
			return next, nil
		}
		if event.Workspace == nil || event.Workspace.GetCheckoutPath() == "" || event.Workspace.GetBaseCommit() == "" {
			return next, nil
		}
		next.ReflectionWorkspace = proto.Clone(event.Workspace).(*contracts.ProposalWorkspace)
		next.ReflectionPaths = slices.Clone(event.Paths)
		if next.ReflectionDecision == nil {
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_DISPATCH
			next.ReflectionDispatchAt = timestamppb.New(event.At)
		} else if next.ReflectionDecision.GetLearningDecision() == "failed" && next.ReflectionDecision.GetError() != "" {
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_FAILED_CLEANUP
		} else {
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_REVIEW
		}
	case ReflectionCompleted:
		if kind != DispatchReflection || event.Decision == nil {
			return next, nil
		}
		next.ReflectionDecision = proto.Clone(event.Decision).(*contracts.ReflectionDecision)
		next.ReflectionResults = append(next.ReflectionResults, proto.Clone(event.Decision).(*contracts.ReflectionDecision))
		next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_INSPECT_RESULT
	case ReflectionReviewed:
		if kind != ReviewReflection {
			return next, nil
		}
		// Even an empty diff must reach the broker's complete decision and
		// accepted-outcome checks. The broker returns a commit-free receipt
		// for a valid no-op; it also owns rejection and retry diagnostics.
		next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH
	case ReflectionPublished:
		if kind != PublishReflection {
			return next, nil
		}
		next.ReflectionCommit = event.Commit
		next.ReflectionPaths = slices.Clone(event.Paths)
		next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_DONE
	case ReflectionRejected:
		if (kind != ReviewReflection && kind != PublishReflection) || len(event.Diagnostics) == 0 {
			return next, nil
		}
		attempt, _ := ReflectionFeedback(next)
		rejection := &contracts.ProposalRejection{Attempt: attempt, RejectedAt: timestamppb.New(event.At)}
		for _, diagnostic := range event.Diagnostics {
			rejection.Diagnostics = append(rejection.Diagnostics, proto.Clone(diagnostic).(*contracts.MemoryDiagnostic))
		}
		next.ReflectionRejections = append(next.ReflectionRejections, rejection)
		// Once a memory conflict occurs, all publication/correction attempts
		// share a three-attempt cap, including invalid corrections. Persist
		// the tightened bound so restart cannot grant another attempt.
		for _, diagnostic := range event.Diagnostics {
			if diagnostic.GetRule() == "reflection.objective-changed" || diagnostic.GetRule() == "reflection.policy-changed" {
				next.ReflectionMaxRetries = 0
				break
			}
			if diagnostic.GetMemoryConflict() != nil {
				next.ReflectionMaxRetries = min(next.ReflectionMaxRetries, 2)
				break
			}
		}
		next.ReflectionRejected = uint32(len(next.ReflectionRejections)) > next.ReflectionMaxRetries
		next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_RESET
	case ReflectionReset:
		if kind != ResetReflection {
			return next, nil
		}
		next.ReflectionDecision, next.ReflectionWorkspace, next.ReflectionDispatchAt = nil, nil, nil
		next.ReflectionPaths = nil
		next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_INSPECT
		if next.ReflectionRejected {
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_DONE
		}
	case ReflectionCleaned:
		if kind == CleanupReflection {
			next.ReflectionPaths = nil
			next.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_DONE
		}
	case ReflectionFault:
		// No cleanup, result discard, new task, or new deadline on an API fault.
	}
	return next, nil
}

// ReflectionTaskID preserves the registered worker's established identity.
func ReflectionTaskID(incident, outcomeCommit string, attempt uint32) string {
	// Keep the original hash framing so happy-path task/session IDs are unchanged.
	identity := fmt.Sprintf("%x", sha256.Sum256([]byte(incident+"\x00"+outcomeCommit)))
	taskID := "reflection-" + identity
	if attempt > 1 {
		taskID = fmt.Sprintf("%s-retry%d", taskID, attempt-1)
	}
	return taskID
}

func ReflectionDeadline(at time.Time, timeout time.Duration) *timestamppb.Timestamp {
	return timestamppb.New(at.Add(timeout))
}
