package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CandidateVerifier judges a commit that is the clean HEAD of root (the
// accepted repository or a proposal checkout) with that commit's detectors
// against the live deployment. deployment.IndependentHealthVerifier
// implements it.
type CandidateVerifier interface {
	VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source DeploymentReceipt) (HealthVerdict, error)
}

// MaintenanceWindow pauses and resumes the controller's evaluation (the
// controller maintenance ConfigMap) around an advance's cluster changes. A
// pause is only declared by Pause; it is in force once Acknowledged reports
// that the controller drained its in-flight evaluation and stopped opening
// incidents. Nothing is applied before that.
//
// A pause is self-expiring: Pause carries the deadline after which the
// controller treats it as resumed, so a Resume that fails or never runs
// cannot suspend detection indefinitely.
type MaintenanceWindow interface {
	Pause(ctx context.Context, reason string, expiresAt time.Time) error
	// Acknowledged reports whether the controller confirmed the pause the
	// latest Pause declared.
	Acknowledged(ctx context.Context) (bool, error)
	Resume(ctx context.Context) error
}

// ErrNoAcceptedPlan means no deployment of this installation is recorded, or
// the accepted tree has no .sdo/deployment/plan.yaml (for example a seeded

// install), so nothing can redeploy an upstream revision. The host stops following the source.
var ErrNoAcceptedPlan = errors.New("accepted deployment evidence has no deployment plan; upstream advance is unavailable for this installation")

// HoldReason classifies a held head or repair deployment.
type HoldReason string

const (
	HoldBuild   HoldReason = "build"   // images resolve before any apply
	HoldRollout HoldReason = "rollout" // cluster left as is
	HoldHealth  HoldReason = "health"  // cluster left as is
)

// evaluation pass the controller can be draining.
const (
	DefaultPauseAckTimeout  = 5 * time.Minute
	DefaultPauseAckInterval = time.Second
	// DefaultPauseApplyBound covers the deployment, its correction rounds and
	// the verification between the acknowledgement and the settle window.
	DefaultPauseApplyBound = 30 * time.Minute
	DefaultResumeTries     = 5
	DefaultResumeBackoff   = 2 * time.Second
)

// DefaultSettleWindow is the settle window of an advance whose AdvanceConfig
// does not set one. A firing that persists past it is a regression of the
// candidate.
const DefaultSettleWindow = 5 * time.Minute

// pauseControl lifts and waits on a MaintenanceWindow for a deploy that runs
// under a controller pause.
type pauseControl struct {
	maintenance MaintenanceWindow
	wait        func(context.Context, time.Duration) error
}

func (a pauseControl) resume(ctx context.Context, cfg pauseBounds) error {
	tries := orDefault(cfg.ResumeTries, DefaultResumeTries)
	backoff := orDefault(cfg.ResumeBackoff, DefaultResumeBackoff)
	var err error
	for try := 0; try < tries; try++ {
		if try > 0 {
			if waitErr := (Coordinator{Wait: a.wait}).wait(ctx, backoff); waitErr != nil {
				return errors.Join(err, waitErr)
			}
		}
		if err = a.maintenance.Resume(ctx); err == nil {
			return nil
		}
	}
	return err
}

// awaitPauseAcknowledged polls the controller's acknowledgement of the pause
// within the bound, on the injected wait. False means the bound passed.
func (a pauseControl) awaitPauseAcknowledged(ctx context.Context, cfg pauseBounds) (bool, error) {
	interval := orDefault(cfg.PauseAckInterval, DefaultPauseAckInterval)
	polls := int(orDefault(cfg.PauseAckTimeout, DefaultPauseAckTimeout) / interval)
	steps := Coordinator{Wait: a.wait}
	for poll := 0; ; poll++ {
		acknowledged, err := a.maintenance.Acknowledged(ctx)
		if err != nil {
			return false, fmt.Errorf("controller maintenance acknowledgement: %w", err)
		}
		if acknowledged {
			return true, nil
		}
		if poll >= polls {
			return false, nil
		}
		if err := steps.wait(ctx, interval); err != nil {
			return false, err
		}
	}
}

// awaitSettled judges the rolled-out candidate once it has settled. The rollout
// is complete when this runs, but its pods are cold: a detector firing inside
// the settle window is warm-up, not a verdict, and is evaluated again (each
// evaluation also waits out its own pending samples). Clear ends it at once. A
// firing that outlasts the window is returned for attribution.

// pauseBounds overrides the pause defaults; the zero value selects them.
type pauseBounds struct {
	PauseAckTimeout, PauseAckInterval, ResumeBackoff, PauseApplyBound, SettleWindow time.Duration
	ResumeTries                                                                     int
}
