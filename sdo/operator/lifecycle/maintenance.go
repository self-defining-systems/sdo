package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability"
)

// CandidateVerifier judges a commit that is the clean HEAD of root (the
// accepted repository or a proposal checkout) with that commit's detectors
// against the live deployment. deployment.IndependentHealthVerifier
// implements it.
type CandidateVerifier interface {
	VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source DeploymentReceipt) (HealthVerdict, error)
}

// DeploymentCorrection is one request to the deployment agent in an advance:
// the advance checkout (the accepted tree merged with upstream, where the agent
// edits deployment files and patches), the accepted plan, and the hold that the
// operability report named, as the correction feedback.
type DeploymentCorrection struct {
	AdvanceID string
	Proposal  broker.Proposal
	Plan      *contracts.DeploymentPlan
	Feedback  string
	// Attempt counts the corrections of this advance, from 1.
	Attempt int
}

// DeploymentCorrector dispatches the deployment agent for an advance held by a
// cause the deployer owns (operability.RouteIn ModeAdvance: patch, render,
// build). It returns the plan the agent proposes for the advance checkout.
type DeploymentCorrector interface {
	CorrectDeployment(ctx context.Context, correction DeploymentCorrection) (*contracts.DeploymentPlan, error)
}

// DefaultMaxCorrections bounds the deployment agent's corrections of one
// advance revision (a Go-side budget, like the onboarding attempts).
const DefaultMaxCorrections = 2

// OperabilityExposer makes a report the application's current operability
// report (the control namespace's sdo-operability ConfigMap).
type OperabilityExposer interface {
	PublishOperability(ctx context.Context, report operability.Report) error
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

// ControllerActivity reports whether the controller is in the middle of work
// an advance must not overlap (an open incident, closure in flight).
type ControllerActivity interface {
	Busy(ctx context.Context) (reason string, busy bool, err error)
}

// ErrNoAcceptedPlan means no deployment of this installation is recorded, or
// the accepted tree has no .sdo/deployment/plan.yaml (for example a seeded

// install), so nothing can redeploy an upstream revision. The host stops following the source.
var ErrNoAcceptedPlan = errors.New("accepted deployment evidence has no deployment plan; upstream advance is unavailable for this installation")

// HoldReason classifies a held revision (operational-contract plan §5 edge
// cases in parentheses).
type HoldReason string

const (
	HoldRefRewritten  HoldReason = "ref_rewritten"  // E10: base is not an ancestor
	HoldMemoryTouched HoldReason = "memory_touched" // E10: upstream changes .sdo/
	HoldUnavailable   HoldReason = "materialize"    // the revision cannot be fetched (not transient)
	HoldMergeConflict HoldReason = "merge_conflict" // E1
	HoldRender        HoldReason = "render"         // E2, before any cluster effect
	// The deployment patch series does not apply to the candidate (E1), found
	// before any cluster effect. The kinds are patchset.PatchError kinds.
	HoldPatchConflict   HoldReason = "patch_conflict"   // context no longer matches, or the file is gone
	HoldPatchUpstreamed HoldReason = "patch_upstreamed" // the change is already upstream; retire the patch
	HoldPatchInvalid    HoldReason = "patch_invalid"    // malformed or forbidden; cannot happen to accepted patches
	// HoldShapeChange: the shape changed and no detection designer is wired to
	// refresh the detectors for it, so nothing is deployed (E4-E6).
	HoldShapeChange HoldReason = "shape_change"
	// HoldDesigner: the advance was deployed, verified and ACCEPTED, but the
	// detection designer's refresh for the changed shape failed. The accepted
	// detectors are those of the previous shape until a refresh succeeds;
	// AdvanceResult.Receipt names the accepted commit and AdvanceResult.Owed
	// carries the obligation the host retries (the hold itself is superseded
	// by the follower's next poll, the obligation is not).
	HoldDesigner HoldReason = "designer_refresh"
	HoldBuild    HoldReason = "build"   // E3, images resolve before any apply
	HoldRollout  HoldReason = "rollout" // E7, cluster left as is
	HoldHealth   HoldReason = "health"  // E8, cluster left as is
	// HoldDeployer: the deployment agent was dispatched for a deployer-owned
	// hold and did not converge: it was given the same correction twice, or its
	// corrections spent the budget. Terminal for this revision; a person looks.
	HoldDeployer HoldReason = "deployer_not_converging"
	// HoldScope: the offline operability checks found the candidate outside
	// the install's scope or registry allowlist, before any cluster effect.
	HoldScope HoldReason = "scope"
	// HoldValidation: the broker's publication validation (the one PublishAdvance
	// applies) rejects the candidate. Found before any cluster effect: an advance
	// never rolls out a candidate the broker would refuse to accept.
	HoldValidation HoldReason = "validation"
	// HoldImmutable: the API server refuses an immutable change the deployer's
	// recreate policy does not replace (immutable_change, deployer: a deployment
	// correction; data_bearing_change, human). Found by the apply; nothing of
	// the refused phase was changed.
	HoldImmutable HoldReason = "immutable"
	// HoldOperability: an operability check failed for a cause none of the
	// reasons above names (an SDO defect or an unknown cause), so a person
	// must look.
	HoldOperability HoldReason = "operability"
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

// MissingCollaboratorError reports required collaborators a component was
// built without. Nil is never a mode: it would disable the behavior silently.
type MissingCollaboratorError struct {
	Component string
	Missing   []string
}

func (e *MissingCollaboratorError) Error() string {
	return e.Component + " needs " + strings.Join(e.Missing, ", ")
}

// RequireCollaborators returns a *MissingCollaboratorError naming the entries
// whose value is nil or false, or nil when every one is present.
func RequireCollaborators(component string, present map[string]bool, order ...string) error {
	var missing []string
	for _, name := range order {
		if !present[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return &MissingCollaboratorError{Component: component, Missing: missing}
}

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
