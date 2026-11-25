package lifecycle

import (
	"context"
	"errors"
	"time"

	"sdo.dev/operator/rendered"
)

// A rollout takes minutes, and what a verdict needs most of that time (the
// compiled detectors, the prober's required samples of the application) does
// not depend on the rollout being done: it needs the application serving the
// candidate. A CandidateSession lets a verifier use the rollout's own time.

// ErrNoSession is returned by BeginCandidate when the verifier cannot overlap
// its work with a rollout (the caller verifies after it, as always).
var ErrNoSession = errors.New("the verifier cannot start before the rollout")

// SessionVerifier is a CandidateVerifier that can begin verifying a candidate
// while it rolls out.
type SessionVerifier interface {
	CandidateVerifier
	// BeginCandidate starts what does not depend on the rollout (compiling the
	// candidate's detectors) for the clean HEAD commit of root.
	BeginCandidate(ctx context.Context, root, commit, namespace, objective string) (CandidateSession, error)
}

// CandidateSession is one candidate's verification in progress.
type CandidateSession interface {
	// Applied says the deployer has applied the candidate (its last phase) and
	// is waiting for the rollout: the session starts observing the application
	// under pins, so the samples a verdict needs accumulate during the rollout.
	Applied(ctx context.Context, pins rendered.ReceiptPins) error
	// Verdict judges the rolled-out candidate as CandidateVerifier would, from
	// what the session observed. readyAt is when the rollout completed: a
	// verdict only counts evaluations judged after it.
	Verdict(ctx context.Context, source DeploymentReceipt, readyAt time.Time) (HealthVerdict, error)
	// Close releases what the session holds.
	Close()
}

type targetHookKey struct{}

// WithTargetHook returns a context whose deployment calls hook before it
// applies each phase, and again once that apply succeeded. An error stops the
// deployment: nothing is written that the target does not record.
func WithTargetHook(ctx context.Context, hook func(context.Context, rendered.TargetWrite) error) context.Context {
	return context.WithValue(ctx, targetHookKey{}, hook)
}

// RecordTarget calls the hook WithTargetHook set, if any.
func RecordTarget(ctx context.Context, write rendered.TargetWrite) error {
	hook, _ := ctx.Value(targetHookKey{}).(func(context.Context, rendered.TargetWrite) error)
	if hook == nil {
		return nil
	}
	if err := hook(ctx, write); err != nil {
		return &targetRecordError{err}
	}
	return nil
}

// targetRecordError is a failed record of the target: not a source fault.
type targetRecordError struct{ err error }

func (e *targetRecordError) Error() string               { return "record the deployment target: " + e.err.Error() }
func (e *targetRecordError) Unwrap() error               { return e.err }
func (e *targetRecordError) InfrastructureOutcome() bool { return true }

type appliedHookKey struct{}

// WithAppliedHook returns a context whose deployment calls hook once it has
// applied the last phase of the candidate, before it waits for the rollout,
// with the pins of the images it applied.
func WithAppliedHook(ctx context.Context, hook func(rendered.ReceiptPins)) context.Context {
	return context.WithValue(ctx, appliedHookKey{}, hook)
}

// AppliedHookFrom is the hook WithAppliedHook set, or nil.
func AppliedHookFrom(ctx context.Context) func(rendered.ReceiptPins) {
	hook, _ := ctx.Value(appliedHookKey{}).(func(rendered.ReceiptPins))
	return hook
}
