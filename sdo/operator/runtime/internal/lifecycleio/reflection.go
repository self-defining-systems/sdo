package lifecycleio

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// ReflectionDriver owns the I/O of the effects chosen by the pure machine.
// Reflection uses a registered worker callback; it does not expose state writes.
type ReflectionDriver struct {
	Repository  *broker.Broker
	SessionRoot string
	Reflect     func(context.Context, lifecyclecore.ReflectionEffect) (*contracts.ReflectionDecision, error)
	Phase       func(string) error
	Now         func() time.Time
}

func (d ReflectionDriver) Execute(ctx context.Context, effect lifecyclecore.ReflectionEffect) (lifecyclecore.ReflectionEvent, error) {
	event := lifecyclecore.ReflectionEvent{EffectID: effect.ID}
	if d.Repository == nil || d.Now == nil {
		return event, fmt.Errorf("reflection driver requires repository and injected clock")
	}
	if effect.Kind == lifecyclecore.DispatchReflection && d.Reflect == nil {
		return event, fmt.Errorf("reflection driver requires registered worker")
	}
	id := effect.IncidentID
	var err error
	switch effect.Kind {
	case lifecyclecore.InspectReflection:
		var accepted broker.Receipt
		var ok bool
		accepted, ok, err = d.Repository.AcceptedCommit(ctx, id, broker.ReflectionPhase)
		if err == nil && ok {
			event.Kind = lifecyclecore.ReflectionInspected
			event.Commit = accepted.Commit
			event.Paths = accepted.ChangedPaths
			break
		}
		if err != nil {
			break
		}
		var proposal broker.Proposal
		proposal, err = d.Repository.CreateProposalForPhase(ctx, id, broker.ReflectionPhase)
		if err != nil {
			break
		}
		event.Paths, err = d.Repository.ChangedPaths(ctx, id, broker.ReflectionPhase)
		event.Kind = lifecyclecore.ReflectionInspected
		event.Workspace = &contracts.ProposalWorkspace{ProposalId: lifecyclecore.ReflectionTaskID(id, effect.OutcomeCommit, effect.Attempt), CheckoutPath: proposal.Path, BaseCommit: proposal.BaseCommit}
	case lifecyclecore.DispatchReflection:
		if d.Phase != nil {
			err = d.Phase("reflection_dispatch")
			if err != nil {
				break
			}
		}
		event.Decision, err = d.Reflect(ctx, effect)
		if err == nil && d.Phase != nil {
			err = d.Phase("worker_result_picked_up")
		}
		event.Kind = lifecyclecore.ReflectionCompleted
	case lifecyclecore.ReviewReflection:
		// Retain the durable review checkpoint, but leave every content rule
		// to the broker's complete Check over final files and committed inputs.
		event.Kind = lifecyclecore.ReflectionReviewed
	case lifecyclecore.PublishReflection:
		publishCtx := ctx
		if d.SessionRoot != "" {
			var captureDir string
			captureDir, err = memory.IncidentCorpusDir(d.SessionRoot, id)
			if err != nil {
				break
			}
			publishCtx = validate.WithCaptureSource(publishCtx, captureDir, id)
		}
		var accepted broker.Receipt
		accepted, err = d.Repository.PublishReflection(publishCtx, id, effect.Decision)
		event.Kind = lifecyclecore.ReflectionPublished
		event.Commit, event.Paths = accepted.Commit, accepted.ChangedPaths
	case lifecyclecore.ResetReflection, lifecyclecore.CleanupReflection:
		err = d.Repository.ReleaseProposal(id, broker.ReflectionPhase)
		event.Kind = lifecyclecore.ReflectionReset
		if effect.Kind == lifecyclecore.CleanupReflection {
			event.Kind = lifecyclecore.ReflectionCleaned
		}
	default:
		return event, fmt.Errorf("unknown reflection effect %d", effect.Kind)
	}
	event.At = d.Now()
	if err != nil {
		if effect.Kind == lifecyclecore.ReviewReflection || effect.Kind == lifecyclecore.PublishReflection {
			if diagnostics, typed := broker.RejectionDiagnostics(err); typed {
				event.Kind = lifecyclecore.ReflectionRejected
				event.Diagnostics = diagnostics
				return event, nil
			}
		}
		return event, err
	}
	return event, nil
}
