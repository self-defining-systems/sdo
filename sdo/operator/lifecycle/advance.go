package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
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
	// with the plan its tree declares (found is false when nothing is recorded).
	AcceptedSourceEvidence(ctx context.Context) (accepted broker.AcceptedSource, found bool, err error)
	// ProposeAdvance creates the isolated advance checkout at accepted HEAD
	// (proposal.BaseCommit), merges upstream into it three-way with base as
	// the merge base and, without conflicts, commits the result as the
	// checkout's HEAD: candidate. Conflicting paths are returned, not an error;
	// the checkout then has no candidate.
	ProposeAdvance(ctx context.Context, id, base, upstream string) (proposal broker.Proposal, candidate string, conflicts []string, err error)
	// PublishAdvance accepts the candidate of proposal id with evidence of its
	// live deployment and the upstream/base trailers.
	// The stage-advance operability report must bind to the evidence and the
	// accepted head and be accepted by operability.Decide; the broker
	// recomputes both and records the report with the commit.
	PublishAdvance(ctx context.Context, id, upstream, base string, evidence broker.SourceEvidence, report operability.Report) (broker.Receipt, error)
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

// ShapeChange describes an advance that changed the desired-state shape of
// the application (a component added or removed, a Service port edited): the
// detection designer's job is to bring its detectors and traffic probes up to
// date (operational-contract plan §5, E4, E5, E8).
type ShapeChange struct {
	// Upstream is the advanced upstream commit; Accepted is the accepted
	// commit the advance published.
	Upstream, Accepted string
}

// DesignerRefresher re-runs the detection designer (the health judge) for the
// shape of the newly accepted source. The designer's candidate is staged and
// accepted through the operability gate like any other; an error means the
// designer could not produce accepted detection for the new shape. It returns
// nil when the accepted detection is already current.
type DesignerRefresher interface {
	RefreshDetection(ctx context.Context, change ShapeChange) error
}

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

// ErrNoAcceptedPlan means no deployment of this installation is recorded, or
// the accepted tree has no .sdo/deployment/plan.yaml (for example a seeded
// install), so nothing can redeploy an upstream revision. The host stops following the source.
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
	// HoldScope: the offline operability checks found the candidate outside
	// the install's scope or registry allowlist, before any cluster effect.
	HoldScope HoldReason = "scope"
	// HoldOperability: an operability check failed for a cause none of the
	// reasons above names (an SDO defect or an unknown cause), so a person
	// must look.
	HoldOperability HoldReason = "operability"
)

// holdRouting is the operability cause and items of the holds that are not
// derived from a failing check: the revision itself is refused before there
// is a report to read them from. The owner is always operability.RouteIn
// (ModeAdvance, cause).
var holdRouting = map[HoldReason]struct {
	cause operability.Cause
	items []operability.ItemID
}{
	HoldRefRewritten:    {operability.CauseEnvironment, nil},
	HoldMemoryTouched:   {operability.CauseScope, []operability.ItemID{operability.O6}},
	HoldUnavailable:     {operability.CauseEnvironment, nil},
	HoldMergeConflict:   {operability.CausePatch, []operability.ItemID{operability.O1}},
	HoldRender:          {operability.CauseRender, []operability.ItemID{operability.O1}},
	HoldPatchConflict:   {operability.CausePatch, []operability.ItemID{operability.O1}},
	HoldPatchUpstreamed: {operability.CausePatch, []operability.ItemID{operability.O1}},
	HoldPatchInvalid:    {operability.CausePatch, []operability.ItemID{operability.O1}},
	HoldShapeChange:     {operability.CauseCoverage, []operability.ItemID{operability.O3}},
	HoldDesigner:        {operability.CauseCoverage, []operability.ItemID{operability.O3}},
	HoldBuild:           {operability.CauseBuild, []operability.ItemID{operability.O2}},
	HoldRollout:         {operability.CauseRollout, []operability.ItemID{operability.O1}},
	HoldHealth:          {operability.CauseSourceFault, []operability.ItemID{operability.O4}},
	HoldScope:           {operability.CauseScope, []operability.ItemID{operability.O6}},
	HoldOperability:     {operability.CauseInternal, nil},
}

// AdvanceResult is the typed result of one advance.
type AdvanceResult struct {
	Outcome AdvanceOutcome
	// Upstream is the materialized upstream commit (empty before step 1).
	Upstream string
	Hold     HoldReason
	// Cause, Owner and Items say why the revision is held in the operability
	// vocabulary (the persisted reason starts with them): the cause, who must
	// act on it in an advance (operability.RouteIn ModeAdvance) and the items
	// of the report that failed. Empty unless the outcome is a hold.
	Cause operability.Cause
	Owner operability.Owner
	Items []operability.ItemID
	// Detail words the hold or deferral for the holds record and logs.
	Detail string
	// Paths are the conflicting or .sdo/ paths of a hold, when it has any.
	Paths   []string
	Receipt broker.Receipt
	Verdict HealthVerdict
	// Owed is set when the advance was accepted but the detection designer's
	// refresh did not complete (failed or was interrupted): the accepted
	// detectors describe the previous shape. The host keeps it as an open
	// obligation bound to Receipt.Commit and retries until a refresh succeeds.
	Owed *ShapeChange
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

// reason is the persisted hold reason: a parseable prefix
// `cause=<c> owner=<o> items=<O1,O3|->`, then the hold, detail and paths as
// free text. The follower treats the whole string as opaque.
func (r AdvanceResult) reason() string {
	items := "-"
	if len(r.Items) > 0 {
		names := make([]string, len(r.Items))
		for i, item := range r.Items {
			names[i] = string(item)
		}
		items = strings.Join(names, ",")
	}
	text := fmt.Sprintf("cause=%s owner=%s items=%s %s", r.Cause, r.Owner, items, r.Hold)
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
	Upstreams UpstreamMaterializer
	Broker    AdvanceBroker
	Readiness SourceReadiness
	Verifier  CandidateVerifier
	Shape     ShapeDigest
	// Designer refreshes detection after an advance changed the shape. Nil
	// holds a shape change before anything is deployed.
	Designer    DesignerRefresher
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
	accepted, found, err := a.Broker.AcceptedSourceEvidence(ctx)
	if errors.Is(err, planfile.ErrAbsent) {
		return result, fmt.Errorf("%w: %w", ErrNoAcceptedPlan, err)
	}
	if err != nil {
		return result, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if !found || accepted.Plan.GetSource() == nil {
		return result, ErrNoAcceptedPlan
	}
	plan := accepted.Plan
	id := AdvanceID(commit)
	proposal, candidate, conflicts, err := a.Broker.ProposeAdvance(ctx, id, request.Prev, commit)
	if err != nil {
		// The broker applies the patch series to the candidate and removes
		// the checkout itself when it does not apply.
		if hold, detail, paths, ok := patchHold(err); ok {
			result.Paths = paths
			return held(result, hold, detail), nil
		}
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
	hold, detail, paths, shapeChanged, err := a.impact(ctx, proposal, candidate, plan.GetSource())
	if err != nil || hold != "" {
		result.Paths = paths
		return held(result, hold, detail), err
	}
	if shapeChanged && a.Designer == nil {
		return held(result, HoldShapeChange, "desired-state shape changed and no detection designer is configured to refresh the detectors"), nil
	}
	offlineChecks, decision, err := a.offlineChecks(ctx, cfg, proposal, plan)
	if err != nil {
		return result, err
	}
	if decision.Kind != operability.Accepted {
		return heldBy(result, holdForCause(decision.Checks[0].Cause), decision), nil
	}
	result, err = a.deployAndAccept(ctx, cfg, result, proposal, candidate, plan, request.Prev, offlineChecks, &published)
	if err != nil || result.Outcome != AdvanceAccepted || !shapeChanged {
		return result, err
	}
	// Maintenance is resumed: the designer runs while the controller detects.
	// The accepted commit owes a refresh until one succeeds; the owed state is
	// returned even with an interrupted context so the host can persist it.
	change := ShapeChange{Upstream: result.Upstream, Accepted: result.Receipt.Commit}
	result.Owed = &change
	if err := a.Designer.RefreshDetection(ctx, change); err != nil {
		if ctx.Err() != nil {
			return held(result, HoldDesigner, "accepted "+result.Receipt.Commit+", but the detection designer's refresh was interrupted: "+ctx.Err().Error()), ctx.Err()
		}
		return held(result, HoldDesigner, "accepted "+result.Receipt.Commit+", but the detection designer's refresh failed: "+err.Error()), nil
	}
	result.Owed = nil
	return result, nil
}

// impact renders the accepted HEAD and the candidate with the same source.
// A candidate that does not render is held before anything touches the
// cluster. A candidate whose shape differs is reported, not held: the advance
// proceeds and the detection designer refreshes for the new shape.
func (a Advancer) impact(ctx context.Context, proposal broker.Proposal, candidate string, source *contracts.DeploymentSource) (hold HoldReason, detail string, paths []string, shapeChanged bool, err error) {
	base, err := a.Shape(ctx, proposal.Path, proposal.BaseCommit, source)
	if err != nil {
		return "", "", nil, false, fmt.Errorf("render accepted source %s: %w", proposal.BaseCommit, err)
	}
	next, err := a.Shape(ctx, proposal.Path, candidate, source)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", nil, false, ctx.Err()
		}
		// Shape reads the effective tree, so a patch that fails there is the
		// same typed hold the broker raises when it composes the candidate.
		if hold, detail, paths, ok := patchHold(err); ok {
			return hold, detail, paths, false, nil
		}
		return HoldRender, err.Error(), nil, false, nil
	}
	return "", "", nil, next != base, nil
}

const maxHoldDetail = 600

// patchHold classifies a failure of the deployment patch series against a
// candidate. The detail names the patch, its owner, the failing step and, when
// the broker found them, the upstream commits that touched the same paths;
// it is what the follower persists and an operator reads.
func patchHold(err error) (HoldReason, string, []string, bool) {
	var patchErr *patchset.PatchError
	if !errors.As(err, &patchErr) {
		return "", "", nil, false
	}
	var hold *broker.PatchHoldError
	errors.As(err, &hold)
	reason := HoldPatchConflict
	switch patchErr.Kind {
	case patchset.KindUpstreamed:
		reason = HoldPatchUpstreamed
	case patchset.KindMalformed, patchset.KindForbidden:
		reason = HoldPatchInvalid
	}
	var b strings.Builder
	if patchErr.Patch != "" {
		b.WriteString("patch " + patchErr.Patch)
	} else {
		b.WriteString("patch series")
	}
	if hold != nil && hold.Owner != "" {
		b.WriteString(" (owner " + hold.Owner + ")")
	}
	b.WriteString(": ")
	b.WriteString(string(patchErr.Kind))
	if patchErr.Detail != "" {
		b.WriteString(", " + strings.Join(strings.Fields(patchErr.Detail), " "))
	}
	detail := b.String()
	if len(detail) > maxHoldDetail {
		detail = detail[:maxHoldDetail] + "..."
	}
	if reason == HoldPatchUpstreamed {
		detail += "; the change is already in upstream, retire this patch"
	}
	if hold != nil && len(hold.UpstreamCommits) > 0 {
		detail += "; upstream commits touching these paths: " + strings.Join(hold.UpstreamCommits, ", ")
	}
	return reason, detail, append([]string(nil), patchErr.Paths...), true
}

// offlineChecks is the offline subset of the advance report (O1 patches,
// render, determinism and workloads, O2 image declarations, O6 scope and
// allowlist) for the merged candidate, computed with the Deployer's own
// effective tree, render environment and shared functions, so a failure here
// is the one apply would have hit, before the maintenance pause and any
// cluster effect. A Readiness verifier that cannot check offline contributes
// no checks. An infrastructure cause is retried within the bounded window and
// ends as an error (the follower retries the revision).
func (a Advancer) offlineChecks(ctx context.Context, cfg AdvanceConfig, proposal broker.Proposal, plan *contracts.DeploymentPlan) ([]operability.Check, operability.Decision, error) {
	checker, ok := a.Readiness.(OfflineChecker)
	if !ok {
		return nil, operability.Decision{Kind: operability.Accepted}, nil
	}
	steps := Coordinator{Wait: a.Wait}
	var checks []operability.Check
	var decision operability.Decision
	err := steps.retryInfrastructure(ctx, advanceBounds(cfg), "offline operability checks", func() error {
		offline, err := checker.CheckOffline(ctx, proposal.Path, plan)
		if err != nil {
			return err
		}
		subject := operability.Subject{
			AcceptedCommit: proposal.BaseCommit, BaseTree: offline.BaseTree, EffectiveTree: offline.EffectiveTree,
			PatchesDigest: offline.PatchesDigest, RenderDigest: offline.RenderDigest, EnvDigest: offline.EnvDigest,
		}
		checks = offline.Checks
		// Offline, an advance is judged on its deployment items only; the
		// live subset adds O4 and the rest of O1, O2 and O6.
		report, err := operability.New(operability.StageAdvance, subject.WithUnavailable()).With(checks...)
		if err != nil {
			return err
		}
		decision = report.DecideIn(operability.ModeAdvance, operability.O1, operability.O2, operability.O6)
		if decision.Kind == operability.Retry {
			return &retryOffline{decision}
		}
		return nil
	})
	return checks, decision, err
}

func advanceBounds(cfg AdvanceConfig) Config {
	return Config{Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, PendingEvaluations: cfg.PendingEvaluations, PendingInterval: cfg.PendingInterval, InfrastructureTries: cfg.InfrastructureTries, InfrastructureBackoff: cfg.InfrastructureBackoff}
}

// decideAdvance puts the checks in one stage-advance report and decides it for
// an advance.
func decideAdvance(subject operability.Subject, checks ...[]operability.Check) (operability.Report, operability.Decision, error) {
	var all []operability.Check
	for _, group := range checks {
		all = append(all, group...)
	}
	report, err := operability.New(operability.StageAdvance, subject).With(all...)
	if err != nil {
		return operability.Report{}, operability.Decision{}, fmt.Errorf("advance operability report: %w", err)
	}
	return report, report.DecideIn(operability.ModeAdvance, operability.StageItems(operability.StageAdvance)...), nil
}

func skippedDetectors(reason string) []operability.Check {
	return []operability.Check{{ID: "O4.detectors", Phase: operability.PhaseLive, Status: operability.StatusSkipped, Detail: reason}}
}

func (a Advancer) deployAndAccept(ctx context.Context, cfg AdvanceConfig, result AdvanceResult, proposal broker.Proposal, candidate string, plan *contracts.DeploymentPlan, prev string, offline []operability.Check, published *bool) (_ AdvanceResult, err error) {
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
	bounds := advanceBounds(cfg)
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
		// The cluster was touched (or an image failed to build before it was):
		// the live checks classify the failure by type, and the cause routes it
		// for an advance, never to a deployer correction: the candidate is
		// already applied.
		classified, cerr := a.Readiness.CheckLive(ctx, proposal.Path, plan, DeploymentReceipt{}, err)
		if cerr != nil {
			return result, fmt.Errorf("classify deployment failure (%v): %w", err, cerr)
		}
		_, decision, rerr := decideAdvance(operability.Subject{AcceptedCommit: proposal.BaseCommit}.WithUnavailable(), offline, classified, skippedDetectors("not run: the deployment failed"))
		if rerr != nil {
			return result, rerr
		}
		if decision.Kind == operability.Retry {
			return result, err
		}
		return heldBy(result, holdForCause(decision.Checks[0].Cause), decision), nil
	}
	if !completeDeploymentReceipt(receipt) || !rendered.SameDeploymentSource(receipt.Source, plan.GetSource()) {
		return held(result, HoldRollout, "deployment returned incomplete evidence or another source"), nil
	}
	live, err := a.Readiness.CheckLive(ctx, proposal.Path, plan, receipt, nil)
	if err != nil {
		return result, fmt.Errorf("live deployment checks: %w", err)
	}
	subject := operability.Subject{
		AcceptedCommit: proposal.BaseCommit, BaseTree: receipt.BaseTree, EffectiveTree: receipt.EffectiveTree,
		PatchesDigest: receipt.PatchesDigest, RenderDigest: receipt.RenderDigest, EnvDigest: receipt.EnvDigest,
	}
	detectors := skippedDetectors("not run: a deployment check failed")
	if !failing(live) {
		verdict, err := steps.awaitVerdict(ctx, bounds, func() (HealthVerdict, error) {
			return a.Verifier.VerifyCandidate(ctx, proposal.Path, candidate, cfg.Namespace, cfg.HealthObjective, receipt)
		})
		result.Verdict = verdict
		if err != nil {
			var pending *PendingVerificationError
			if errors.As(err, &pending) {
				// The detectors could not answer in time: nothing says the
				// application is unhealthy, and the designer owns the probes.
				pendingHold := held(result, HoldHealth, pending.Error())
				pendingHold.Cause = operability.CauseProbeUnavailable
				pendingHold.Owner = operability.RouteIn(operability.ModeAdvance, pendingHold.Cause)
				return pendingHold, nil
			}
			return result, err
		}
		detectors = verdictChecks(verdict)
	}
	report, decision, err := decideAdvance(subject, offline, live, detectors)
	if err != nil {
		return result, err
	}
	switch decision.Kind {
	case operability.Accepted:
	case operability.Retry:
		return result, &retryChecks{decision}
	default:
		// E7 and E8, after the cluster changed: the cause routes by Go's
		// provenance rule (a firing floor detector is the application's,
		// responder; only the designer's own detectors firing is the
		// designer's); the cluster is left as is for ordinary detection.
		return heldBy(result, holdForCause(decision.Checks[0].Cause), decision), nil
	}
	receiptOfAdvance, err := a.Broker.PublishAdvance(ctx, AdvanceID(result.Upstream), result.Upstream, prev, deploymentToBroker(receipt), report)
	if err != nil {
		return result, fmt.Errorf("publish advance: %w", err)
	}
	*published = true
	result.Outcome, result.Receipt = AdvanceAccepted, receiptOfAdvance
	return result, nil
}

func held(result AdvanceResult, reason HoldReason, detail string) AdvanceResult {
	if reason == "" {
		return result
	}
	result.Outcome, result.Hold, result.Detail = AdvanceHeld, reason, detail
	routing := holdRouting[reason]
	result.Cause, result.Items = routing.cause, routing.items
	result.Owner = operability.RouteIn(operability.ModeAdvance, routing.cause)
	return result
}

// heldBy holds the revision for a decision the report reached: the cause and
// items are those of the failing checks the owner must act on, and the detail
// words them on one line.
func heldBy(result AdvanceResult, reason HoldReason, decision operability.Decision) AdvanceResult {
	detail := strings.Join(strings.Fields(decision.Feedback()), " ")
	if len(detail) > maxHoldDetail {
		detail = detail[:maxHoldDetail] + "..."
	}
	result = held(result, reason, detail)
	if len(decision.Checks) == 0 {
		return result
	}
	result.Cause, result.Owner, result.Items = decision.Checks[0].Cause, decision.Owner, nil
	for _, check := range decision.Checks {
		item, _, _ := strings.Cut(check.ID, ".")
		if id := operability.ItemID(item); !slices.Contains(result.Items, id) {
			result.Items = append(result.Items, id)
		}
	}
	return result
}

// holdForCause names the hold of a failing check by its cause. Causes that
// no advance step has a name for end as an operability hold.
func holdForCause(cause operability.Cause) HoldReason {
	switch cause {
	case operability.CausePatch:
		return HoldPatchConflict
	case operability.CauseRender, operability.CauseRenderNondeterministic:
		return HoldRender
	case operability.CauseBuild:
		return HoldBuild
	case operability.CauseRollout:
		return HoldRollout
	case operability.CauseScope, operability.CauseAllowlist, operability.CauseBudget:
		return HoldScope
	case operability.CauseDetectorFault, operability.CauseSourceFault, operability.CauseProbeUnavailable:
		return HoldHealth
	}
	return HoldOperability
}

// memoryPaths returns the paths under operational memory (.sdo/).
func memoryPaths(paths []string) []string {
	var out []string
	for _, path := range paths {
		if layout.IsMemoryPath(path) {
			out = append(out, path)
		}
	}
	return out
}
