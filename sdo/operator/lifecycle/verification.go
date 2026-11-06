package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/lifecycle/internal/waitio"
)

// VerificationOutcome is what independent verification concluded. Only a
// source fault is the deployer's to answer; a pending window is waited out and
// an infrastructure failure is retried, and neither consumes a deployer
// attempt.
type VerificationOutcome string

const (
	// VerificationClear means every accepted detector evaluated clear.
	VerificationClear VerificationOutcome = "clear"
	// VerificationSourceFault means the verified rollout trips its detectors.
	VerificationSourceFault VerificationOutcome = "source_fault"
	// VerificationPending means an observation window is incomplete or a
	// detector could not be evaluated yet.
	VerificationPending VerificationOutcome = "pending"
)

// DetectorState is one detector's evaluation with the reason it gave.
type DetectorState struct {
	DetectorID string
	Status     string
	Reason     string
}

func (s DetectorState) String() string {
	text := s.DetectorID + " " + s.Status
	if s.Reason != "" {
		text += " (" + s.Reason + ")"
	}
	return text
}

// HealthVerdict is the typed result of independent health verification.
type HealthVerdict struct {
	Outcome   VerificationOutcome
	Summary   string
	Detectors []DetectorState
}

// Feedback words the verdict, with each detector's state, for a reader that
// must act on it.
func (v HealthVerdict) Feedback() string {
	parts := make([]string, 0, len(v.Detectors))
	for _, state := range v.Detectors {
		parts = append(parts, state.String())
	}
	if len(parts) == 0 {
		return v.Summary
	}
	return v.Summary + ": " + strings.Join(parts, "; ")
}

// PendingVerificationError ends a lifecycle whose verification stayed pending
// for the whole bounded window.
type PendingVerificationError struct {
	Evaluations int
	Window      time.Duration
	Verdict     HealthVerdict
}

func (e *PendingVerificationError) Error() string {
	return fmt.Sprintf("health verification still pending after %d evaluations over %s (not a source fault, no deployer attempt was spent): %s", e.Evaluations, e.Window.Round(time.Second), e.Verdict.Feedback())
}

// InfrastructureError ends a lifecycle whose trusted step kept failing for an
// infrastructure reason (BuildKit startup, registry rate limit, a rollout that
// was still progressing) through the whole bounded retry.
type InfrastructureError struct {
	Step     string
	Attempts int
	Err      error
}

func (e *InfrastructureError) Error() string {
	return fmt.Sprintf("%s failed for an infrastructure reason on all %d tries (not a source fault, no deployer attempt was spent): %v", e.Step, e.Attempts, e.Err)
}

func (e *InfrastructureError) Unwrap() error { return e.Err }

// infrastructureOutcome is implemented by the trusted steps' classified
// errors (build, registry resolution, rollout) that are not source faults.
type infrastructureOutcome interface{ InfrastructureOutcome() bool }

// IsInfrastructureOutcome reports whether err, or an error it wraps, marks a
// failure of a trusted step (not a source fault) with InfrastructureOutcome.
func IsInfrastructureOutcome(err error) bool { return isInfrastructure(err) }

func isInfrastructure(err error) bool {
	var marker infrastructureOutcome
	return errors.As(err, &marker) && marker.InfrastructureOutcome()
}

const (
	defaultPendingEvaluations    = 8
	defaultPendingInterval       = 15 * time.Second
	defaultInfrastructureTries   = 3
	defaultInfrastructureBackoff = 20 * time.Second
)

func orDefault[T int | time.Duration](value, fallback T) T {
	if value <= 0 {
		return fallback
	}
	return value
}

func (c Coordinator) wait(ctx context.Context, d time.Duration) error {
	if c.Wait != nil {
		return c.Wait(ctx, d)
	}
	return waitio.Wait(ctx, d)
}

// retryInfrastructure runs a trusted step, repeating it while it fails for an
// infrastructure reason. Any other error, and the last infrastructure error
// once the bound is spent, is returned to the caller.
func (c Coordinator) retryInfrastructure(ctx context.Context, cfg Config, step string, run func() error) error {
	tries := orDefault(cfg.InfrastructureTries, defaultInfrastructureTries)
	backoff := orDefault(cfg.InfrastructureBackoff, defaultInfrastructureBackoff)
	for try := 1; ; try++ {
		err := run()
		if err == nil || !isInfrastructure(err) || ctx.Err() != nil {
			return err
		}
		if try >= tries {
			return &InfrastructureError{Step: step, Attempts: try, Err: err}
		}
		if err := c.wait(ctx, backoff); err != nil {
			return err
		}
	}
}

// verifyHealth evaluates until the verdict is not pending, within the bounded
// window. A pending verdict at the bound is a terminal error naming each
// pending detector.
func (c Coordinator) verifyHealth(ctx context.Context, cfg Config, acceptedCommit string, deployment DeploymentReceipt) (HealthVerdict, error) {
	return c.awaitVerdict(ctx, cfg, func() (HealthVerdict, error) {
		return c.Verifier.Verify(ctx, acceptedCommit, cfg.Namespace, cfg.HealthObjective, deployment)
	})
}

// awaitVerdict repeats one independent evaluation while it is pending, within
// cfg's pending bounds, retrying infrastructure failures of each evaluation.
func (c Coordinator) awaitVerdict(ctx context.Context, cfg Config, evaluate func() (HealthVerdict, error)) (HealthVerdict, error) {
	evaluations := orDefault(cfg.PendingEvaluations, defaultPendingEvaluations)
	interval := orDefault(cfg.PendingInterval, defaultPendingInterval)
	for evaluation := 1; ; evaluation++ {
		var verdict HealthVerdict
		err := c.retryInfrastructure(ctx, cfg, "independent health verification", func() (err error) {
			verdict, err = evaluate()
			return err
		})
		if err != nil {
			return HealthVerdict{}, err
		}
		if verdict.Outcome != VerificationPending {
			return verdict, nil
		}
		if evaluation >= evaluations {
			return verdict, &PendingVerificationError{Evaluations: evaluation, Window: time.Duration(evaluation-1) * interval, Verdict: verdict}
		}
		if err := c.wait(ctx, interval); err != nil {
			return HealthVerdict{}, err
		}
	}
}

// ProvidedPlanError ends a lifecycle whose human-provided deployment plan
// failed admission, deployment or verification. No agent is dispatched to
// repair a plan the human supplied, so the failure is for the human.
type ProvidedPlanError struct {
	// Stage is "deploy" (admission, deployment, evidence) or "verify".
	Stage  string
	Reason string
}

func (e *ProvidedPlanError) Error() string {
	return fmt.Sprintf("the provided deployment plan failed at %s (no agent was asked to correct it): %s", e.Stage, e.Reason)
}
