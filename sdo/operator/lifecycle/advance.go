package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/upstream"
)

// UpstreamMaterializer makes an upstream revision a commit in a Git object
// store. upstream.Catalog implements it.
type UpstreamMaterializer interface {
	Materialize(ctx context.Context, id string, config *anypb.Any, rev upstream.Revision, gitDir string) (string, error)
}

// AdvanceBroker is the part of the broker an advance needs. The broker owns
// the accepted repository, the three-way merge and the publication, so it
// recomputes the merge on publish with the same procedure it used to create
// the proposal.
type AdvanceBroker interface {
	// UpstreamChange reports, in the accepted object store, whether base is an
	// ancestor of upstream and which paths upstream changed relative to base.
	UpstreamChange(ctx context.Context, base, upstream string) (ancestor bool, paths []string, err error)
	// AcceptedSourceEvidence is the deployment evidence of the accepted HEAD
	// (found is false when nothing is recorded).
	AcceptedSourceEvidence(ctx context.Context) (evidence broker.SourceEvidence, found bool, err error)
	// ProposeAdvance creates the isolated advance checkout at accepted HEAD
	// (proposal.BaseCommit), merges upstream into it three-way with base as
	// the merge base and, without conflicts, commits the result as the
	// checkout's HEAD: candidate. Conflicting paths are returned, not an error;
	// the checkout then has no candidate.
	ProposeAdvance(ctx context.Context, id, base, upstream string) (proposal broker.Proposal, candidate string, conflicts []string, err error)
	// PublishAdvance accepts the candidate of proposal id with evidence of its
	// live deployment and the upstream/base trailers.
	PublishAdvance(ctx context.Context, id, upstream, base string, evidence broker.SourceEvidence) (broker.Receipt, error)
	// ReleaseAdvance discards the advance checkout id.
	ReleaseAdvance(id string) error
}

// CandidateVerifier judges a commit that is the clean HEAD of root (the
// accepted repository or a proposal checkout) with that commit's detectors
// against the live deployment. deployment.IndependentHealthVerifier
// implements it.
type CandidateVerifier interface {
	VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source DeploymentReceipt) (HealthVerdict, error)
}

// ShapeDigest returns the desired-state shape digest of revision in the
// repository at root, rendered by source (rendered.DesiredSet.ShapeDigest).
type ShapeDigest func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error)

// MaintenanceWindow pauses and resumes the controller's evaluation (the
// controller maintenance ConfigMap) around an advance's cluster changes.
type MaintenanceWindow interface {
	Pause(ctx context.Context, reason string) error
	Resume(ctx context.Context) error
}

// ControllerActivity reports whether the controller is in the middle of work
// an advance must not overlap (an open incident, closure in flight).
type ControllerActivity interface {
	Busy(ctx context.Context) (reason string, busy bool, err error)
}

// ErrNoAcceptedPlan means the accepted deployment evidence carries no
// admitted DeploymentPlan (for example a seeded install), so nothing can
// redeploy an upstream revision. The host stops following the source.
var ErrNoAcceptedPlan = errors.New("accepted deployment evidence has no deployment plan; upstream advance is unavailable for this installation")

// AdvanceOutcome is what an advance concluded.
type AdvanceOutcome string

const (
	// AdvanceAccepted: the broker accepted the deployed and verified revision.
	AdvanceAccepted AdvanceOutcome = "accepted"
	// AdvanceHeld: the revision is refused for a typed reason; a newer
	// revision supersedes the hold. A rollout or health hold leaves the
	// cluster as it is, so ordinary detection opens the incident.
	AdvanceHeld AdvanceOutcome = "held"
	// AdvanceDeferred: the controller was busy; try again next poll.
	AdvanceDeferred AdvanceOutcome = "deferred"
)

// HoldReason classifies a held revision (operational-contract plan §5 edge
// cases in parentheses).
type HoldReason string

const (
	HoldRefRewritten  HoldReason = "ref_rewritten"  // E10: base is not an ancestor
	HoldMemoryTouched HoldReason = "memory_touched" // E10: upstream changes .sdo/
	HoldUnavailable   HoldReason = "materialize"    // the revision cannot be fetched (not transient)
	HoldMergeConflict HoldReason = "merge_conflict" // E1
	HoldRender        HoldReason = "render"         // E2, before any cluster effect
	// HoldShapeChange is the MVP stand-in for the designer route (E4-E6).
	HoldShapeChange HoldReason = "shape_change"
	HoldBuild       HoldReason = "build"   // E3, images resolve before any apply
	HoldRollout     HoldReason = "rollout" // E7, cluster left as is
	HoldHealth      HoldReason = "health"  // E8, cluster left as is
)

// AdvanceResult is the typed result of one advance.
type AdvanceResult struct {
	Outcome AdvanceOutcome
	// Upstream is the materialized upstream commit (empty before step 1).
	Upstream string
	Hold     HoldReason
	// Detail words the hold or deferral for the holds record and logs.
	Detail string
	// Paths are the conflicting or .sdo/ paths of a hold, when it has any.
	Paths   []string
	Receipt broker.Receipt
	Verdict HealthVerdict
}

// FollowerOutcome maps the result to the follower's bookkeeping.
func (r AdvanceResult) FollowerOutcome() upstream.Outcome {
	switch r.Outcome {
	case AdvanceAccepted:
		return upstream.Outcome{Kind: upstream.Accepted}
	case AdvanceHeld:
		return upstream.Outcome{Kind: upstream.Refused, Reason: r.reason()}
	default:
		return upstream.Outcome{Kind: upstream.Retry}
	}
}

func (r AdvanceResult) reason() string {
	text := string(r.Hold)
	if r.Detail != "" {
		text += ": " + r.Detail
	}
	if len(r.Paths) > 0 {
		text += " (" + strings.Join(r.Paths, ", ") + ")"
	}
	return text
}

// AdvanceConfig is the installation an advance deploys into.
type AdvanceConfig struct {
	Namespace, HealthObjective string
	// GitDir is the accepted repository's Git directory, where the upstream
	// revision is materialized.
	GitDir string
	// Bounds of the waits; zero selects the onboarding defaults.
	PendingEvaluations    int
	PendingInterval       time.Duration
	InfrastructureTries   int
	InfrastructureBackoff time.Duration
}

// Advancer moves the accepted source to a newer upstream revision without an
// agent: merge, impact check, maintenance-paused deploy, live verification of
// the candidate, then broker acceptance. Nothing is accepted that was not
// deployed and verified; a failure after the cluster changed is left for
// ordinary detection (no rollback).
type Advancer struct {
	Upstreams   UpstreamMaterializer
	Broker      AdvanceBroker
	Readiness   SourceReadinessVerifier
	Verifier    CandidateVerifier
	Shape       ShapeDigest
	Maintenance MaintenanceWindow
	Activity    ControllerActivity
	// Wait pauses between bounded re-evaluations and infrastructure retries;
	// nil waits on a timer.
	Wait func(context.Context, time.Duration) error
}

func (a Advancer) validate(cfg AdvanceConfig, request upstream.Request) error {
	if a.Upstreams == nil || a.Broker == nil || a.Readiness == nil || a.Verifier == nil || a.Shape == nil || a.Maintenance == nil || a.Activity == nil {
		return errors.New("advance needs upstreams, broker, readiness, verifier, shape, maintenance and activity")
	}
	if cfg.Namespace == "" || strings.TrimSpace(cfg.HealthObjective) == "" || cfg.GitDir == "" {
		return errors.New("advance needs a namespace, health objective and accepted Git directory")
	}
	if request.Source.Kind == "" || request.Source.Config == nil || request.Revision.ID == "" || request.Prev == "" {
		return errors.New("advance request needs a source kind and config, a revision and the previous upstream commit")
	}
	return nil
}

// AdvanceID names the advance of one upstream commit (broker proposal ID).
func AdvanceID(commit string) string {
	if len(commit) > 12 {
		commit = commit[:12]
	}
	return "advance-" + commit
}

// Advance runs one advance of request.Source to request.Revision. A returned
// error is transient or unexpected (the follower retries next poll); every
// refusal of the revision itself is an AdvanceHeld result.
func (a Advancer) Advance(ctx context.Context, cfg AdvanceConfig, request upstream.Request) (AdvanceResult, error) {
	if err := a.validate(cfg, request); err != nil {
		return AdvanceResult{}, err
	}
	commit, err := a.Upstreams.Materialize(ctx, request.Source.Kind, request.Source.Config, request.Revision, cfg.GitDir)
	if err != nil {
		if errors.Is(err, upstream.ErrUnavailable) || ctx.Err() != nil {
			return AdvanceResult{}, fmt.Errorf("materialize upstream %s: %w", request.Revision.ID, err)
		}
		return held(AdvanceResult{}, HoldUnavailable, err.Error()), nil
	}
	result := AdvanceResult{Upstream: commit}
	ancestor, paths, err := a.Broker.UpstreamChange(ctx, request.Prev, commit)
	if err != nil {
		return result, fmt.Errorf("upstream change %s..%s: %w", request.Prev, commit, err)
	}
	if !ancestor {
		return held(result, HoldRefRewritten, fmt.Sprintf("previous upstream %s is not an ancestor of %s", request.Prev, commit)), nil
	}
	if memory := memoryPaths(paths); len(memory) > 0 {
		result.Paths = memory
		return held(result, HoldMemoryTouched, "upstream changes operational memory"), nil
	}
	evidence, found, err := a.Broker.AcceptedSourceEvidence(ctx)
	if err != nil {
		return result, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if !found || evidence.Plan == nil || evidence.Plan.GetSource() == nil {
		return result, ErrNoAcceptedPlan
	}
	plan := evidence.Plan
	id := AdvanceID(commit)
	proposal, candidate, conflicts, err := a.Broker.ProposeAdvance(ctx, id, request.Prev, commit)
	if err != nil {
		return result, fmt.Errorf("propose advance: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = a.Broker.ReleaseAdvance(id)
		}
	}()
	if len(conflicts) > 0 {
		result.Paths = append([]string(nil), conflicts...)
		return held(result, HoldMergeConflict, "upstream conflicts with accepted source"), nil
	}
	if candidate == "" {
		return result, errors.New("advance proposal has neither a candidate nor conflicts")
	}
	if hold, detail, err := a.impact(ctx, proposal, candidate, plan.GetSource()); err != nil || hold != "" {
		return held(result, hold, detail), err
	}
	return a.deployAndAccept(ctx, cfg, result, proposal, candidate, plan, request.Prev, &published)
}

// impact renders the accepted HEAD and the candidate with the same source.
// A candidate that does not render, or whose shape differs, is held before
// anything touches the cluster.
func (a Advancer) impact(ctx context.Context, proposal broker.Proposal, candidate string, source *contracts.DeploymentSource) (HoldReason, string, error) {
	base, err := a.Shape(ctx, proposal.Path, proposal.BaseCommit, source)
	if err != nil {
		return "", "", fmt.Errorf("render accepted source %s: %w", proposal.BaseCommit, err)
	}
	next, err := a.Shape(ctx, proposal.Path, candidate, source)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}
		return HoldRender, err.Error(), nil
	}
	if next != base {
		return HoldShapeChange, "desired-state shape changed; the designer route is not available yet", nil
	}
	return "", "", nil
}

func (a Advancer) deployAndAccept(ctx context.Context, cfg AdvanceConfig, result AdvanceResult, proposal broker.Proposal, candidate string, plan *contracts.DeploymentPlan, prev string, published *bool) (_ AdvanceResult, err error) {
	if err := a.Maintenance.Pause(ctx, "upstream advance "+result.Upstream); err != nil {
		return result, fmt.Errorf("pause controller maintenance: %w", err)
	}
	defer func() {
		// Resume even when ctx is done: a paused controller stops detection.
		if resumeErr := a.Maintenance.Resume(context.WithoutCancel(ctx)); resumeErr != nil {
			err = errors.Join(err, fmt.Errorf("resume controller maintenance: %w", resumeErr))
		}
	}()
	reason, busy, err := a.Activity.Busy(ctx)
	if err != nil {
		return result, fmt.Errorf("controller activity: %w", err)
	}
	if busy {
		result.Outcome, result.Detail = AdvanceDeferred, reason
		return result, nil
	}
	steps := Coordinator{Wait: a.Wait}
	bounds := Config{Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, PendingEvaluations: cfg.PendingEvaluations, PendingInterval: cfg.PendingInterval, InfrastructureTries: cfg.InfrastructureTries, InfrastructureBackoff: cfg.InfrastructureBackoff}
	var receipt DeploymentReceipt
	err = steps.retryInfrastructure(ctx, bounds, "upstream advance deployment", func() (err error) {
		receipt, err = a.Readiness.ApplyAndVerify(ctx, proposal.Path, plan)
		return err
	})
	if err != nil {
		var infrastructure *InfrastructureError
		if errors.As(err, &infrastructure) || ctx.Err() != nil {
			return result, err
		}
		var image *imagesrc.Error
		if errors.As(err, &image) {
			return held(result, HoldBuild, err.Error()), nil
		}
		return held(result, HoldRollout, err.Error()), nil
	}
	if !completeDeploymentReceipt(receipt) || !rendered.SameDeploymentSource(receipt.Source, plan.GetSource()) {
		return held(result, HoldRollout, "deployment returned incomplete evidence or another source"), nil
	}
	verdict, err := steps.awaitVerdict(ctx, bounds, func() (HealthVerdict, error) {
		return a.Verifier.VerifyCandidate(ctx, proposal.Path, candidate, cfg.Namespace, cfg.HealthObjective, receipt)
	})
	result.Verdict = verdict
	if err != nil {
		var pending *PendingVerificationError
		if errors.As(err, &pending) {
			return held(result, HoldHealth, pending.Error()), nil
		}
		return result, err
	}
	if verdict.Outcome != VerificationClear {
		return held(result, HoldHealth, verdict.Feedback()), nil
	}
	accepted, err := a.Broker.PublishAdvance(ctx, AdvanceID(result.Upstream), result.Upstream, prev, deploymentToBroker(receipt, plan))
	if err != nil {
		return result, fmt.Errorf("publish advance: %w", err)
	}
	*published = true
	result.Outcome, result.Receipt = AdvanceAccepted, accepted
	return result, nil
}

func held(result AdvanceResult, reason HoldReason, detail string) AdvanceResult {
	if reason == "" {
		return result
	}
	result.Outcome, result.Hold, result.Detail = AdvanceHeld, reason, detail
	return result
}

// memoryPaths returns the paths under operational memory (.sdo/).
func memoryPaths(paths []string) []string {
	var out []string
	for _, path := range paths {
		if path == layout.Dir || strings.HasPrefix(path, layout.Dir+"/") {
			out = append(out, path)
		}
	}
	return out
}
