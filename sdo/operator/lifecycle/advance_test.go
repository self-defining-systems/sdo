package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/upstream"
)

// Synthetic identities. The advance never interprets them beyond equality.
var (
	advPrev      = strings.Repeat("1", 40)
	advRevision  = strings.Repeat("2", 40)
	advHead      = strings.Repeat("3", 40)
	advCandidate = strings.Repeat("4", 40)
	advTree      = strings.Repeat("5", 40)
	advEffective = strings.Repeat("7", 40)
	advDigest    = strings.Repeat("6", 64)
)

const advCheckout = "/synthetic/proposals/advance/checkout"

// advWorld records every effect of an advance in one ordered log, so tests
// assert what happened and, as important, what never happened.
type advWorld struct {
	events []string

	materializeErr error
	notAncestor    bool
	changed        []string
	noEvidence     bool
	noPlan         bool
	planAbsent     bool
	plan           *contracts.DeploymentPlan
	conflicts      []string
	proposeErr     error
	shapes         map[string]string
	shapeErr       map[string]error
	busy           string
	applyErrs      []error
	receipt        func(lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt
	verdicts       []lifecycle.HealthVerdict
	publishErr     error
	designer       bool
	designerErr    error
	// live replaces the live deployment checks CheckLive returns (nil: all
	// pass); liveCalls counts them.
	live      []operability.Check
	liveCalls int
	// offline replaces the offline checks CheckOffline returns (nil: all
	// pass); offlineErrs is consumed one per call (infrastructure failures).
	offline     []operability.Check
	offlineErrs []error

	applied   lifecycle.DeploymentReceipt
	proposed  map[string]bool
	published []broker.SourceEvidence
	reports   []operability.Report
}

func (w *advWorld) log(format string, args ...any) {
	w.events = append(w.events, fmt.Sprintf(format, args...))
}

func (w *advWorld) Materialize(_ context.Context, id string, config *anypb.Any, rev upstream.Revision, gitDir string) (string, error) {
	w.log("materialize %s %s", id, rev.ID)
	if id != "git" || config == nil || gitDir != "/synthetic/accepted/.git" {
		return "", errors.New("unexpected materialize arguments")
	}
	if w.materializeErr != nil {
		return "", w.materializeErr
	}
	return rev.ID, nil
}

// The fake broker enforces the U3 contract the advance relies on: a publish
// names an open proposal with a candidate, the same upstream and base it was
// proposed with, and evidence of the candidate's tree that carries the
// accepted plan forward. It is a negative control for the advance: it rejects
// what the real broker would.
func (w *advWorld) UpstreamChange(_ context.Context, base, upstreamCommit string) (bool, []string, error) {
	w.log("change %s..%s", base[:4], upstreamCommit[:4])
	return !w.notAncestor, slices.Clone(w.changed), nil
}

func (w *advWorld) AcceptedSourceEvidence(context.Context) (broker.AcceptedSource, bool, error) {
	if w.noEvidence {
		return broker.AcceptedSource{}, false, nil
	}
	if w.planAbsent {
		return broker.AcceptedSource{}, true, fmt.Errorf("the tree has no plan: %w", planfile.ErrAbsent)
	}
	accepted := broker.AcceptedSource{Evidence: broker.SourceEvidence{DeploymentSource: w.plan.GetSource(), AcceptedCommit: advHead}, Plan: w.plan}
	if w.noPlan {
		accepted.Plan = nil
	}
	return accepted, true, nil
}

func (w *advWorld) ProposeAdvance(_ context.Context, id, base, upstreamCommit string) (broker.Proposal, string, []string, error) {
	w.log("propose %s", id)
	if w.proposeErr != nil {
		// The real broker removes the checkout before it reports a patch hold.
		return broker.Proposal{}, "", nil, w.proposeErr
	}
	if w.proposed == nil {
		w.proposed = map[string]bool{}
	}
	if w.proposed[id] {
		return broker.Proposal{}, "", nil, errors.New("proposal already open")
	}
	w.proposed[id] = true
	proposal := broker.Proposal{IncidentID: id, Path: advCheckout, BaseCommit: advHead}
	if len(w.conflicts) > 0 {
		return proposal, "", slices.Clone(w.conflicts), nil
	}
	if base != advPrev || upstreamCommit != advRevision {
		return broker.Proposal{}, "", nil, errors.New("unexpected merge inputs")
	}
	return proposal, advCandidate, nil, nil
}

func (w *advWorld) PublishAdvance(_ context.Context, id, upstreamCommit, base string, evidence broker.SourceEvidence, report operability.Report) (broker.Receipt, error) {
	w.log("publish %s", id)
	want := operability.Subject{AcceptedCommit: advHead, BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree, PatchesDigest: evidence.PatchesDigest, RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest}
	switch {
	case report.Stage != operability.StageAdvance || report.Subject.BindsTo(want) != nil:
		return broker.Receipt{}, errors.New("report is not a stage-advance report of the candidate")
	case report.Validate() != nil || report.Decide(operability.StageItems(operability.StageAdvance)...).Kind != operability.Accepted:
		return broker.Receipt{}, errors.New("report is not accepted by Decide")
	case !w.proposed[id]:
		return broker.Receipt{}, errors.New("no open advance proposal")
	case upstreamCommit != advRevision || base != advPrev:
		return broker.Receipt{}, errors.New("trailers differ from the proposal's merge")
	case evidence.BaseTree != advTree || evidence.EffectiveTree != advEffective || evidence.ObservedRevision != advEffective || !evidence.Ready:
		return broker.Receipt{}, errors.New("evidence is not of the candidate tree")
	case !proto.Equal(evidence.DeploymentSource, w.plan.GetSource()):
		return broker.Receipt{}, errors.New("evidence does not name the accepted plan's source")
	case evidence.RenderDigest == "" || evidence.EnvDigest == "" || len(evidence.Images) == 0 || len(evidence.ResourceIDs) == 0:
		return broker.Receipt{}, errors.New("incomplete evidence")
	case w.publishErr != nil:
		return broker.Receipt{}, w.publishErr
	}
	delete(w.proposed, id)
	w.published = append(w.published, evidence)
	w.reports = append(w.reports, report)
	return broker.Receipt{IncidentID: id, Commit: strings.Repeat("7", 40)}, nil
}

func (w *advWorld) ReleaseAdvance(id string) error {
	w.log("release %s", id)
	delete(w.proposed, id)
	return nil
}

func (w *advWorld) shape(_ context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
	w.log("shape %s", revision[:4])
	if root != advCheckout || !proto.Equal(source, w.plan.GetSource()) {
		return "", errors.New("shape read outside the proposal or with another source")
	}
	if err := w.shapeErr[revision]; err != nil {
		return "", err
	}
	return w.shapes[revision], nil
}

func (w *advWorld) Pause(_ context.Context, reason string) error {
	w.log("pause")
	return nil
}

func (w *advWorld) Resume(context.Context) error {
	w.log("resume")
	return nil
}

func (w *advWorld) Busy(context.Context) (string, bool, error) {
	return w.busy, w.busy != "", nil
}

func (w *advWorld) ApplyAndVerify(_ context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	w.log("apply")
	if checkout != advCheckout || !proto.Equal(plan, w.plan) {
		return lifecycle.DeploymentReceipt{}, errors.New("applied another checkout or plan")
	}
	if len(w.applyErrs) > 0 {
		err := w.applyErrs[0]
		w.applyErrs = w.applyErrs[1:]
		if err != nil {
			return lifecycle.DeploymentReceipt{}, err
		}
	}
	receipt := lifecycle.DeploymentReceipt{Source: proto.Clone(plan.GetSource()).(*contracts.DeploymentSource), BaseTree: advTree, EffectiveTree: advEffective, PatchesDigest: advDigest, ObservedRevision: advEffective, RenderDigest: advDigest, EnvDigest: advDigest, Images: []string{"toy@sha256:" + advDigest}, ResourceIDs: []string{"Deployment/toy/toy"}, Ready: true}
	if w.receipt != nil {
		receipt = w.receipt(receipt)
	}
	w.applied = receipt
	return receipt, nil
}

func (w *advWorld) CheckLive(_ context.Context, checkout string, plan *contracts.DeploymentPlan, receipt lifecycle.DeploymentReceipt, outcome error) ([]operability.Check, error) {
	w.log("live")
	w.liveCalls++
	if checkout != advCheckout || !proto.Equal(plan, w.plan) {
		return nil, errors.New("live checks of another checkout or plan")
	}
	if outcome != nil {
		// What deployment.ClassifyDeploymentError does by type.
		cause := operability.CauseRollout
		var image *imagesrc.Error
		if errors.As(outcome, &image) {
			cause = operability.CauseBuild
		}
		fail := operability.Check{ID: "O1.deploy", Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: cause, Detail: outcome.Error()}
		rest := []operability.Check{fail}
		for _, id := range []string{"O1.ready", "O2.stamps", "O6.budget"} {
			rest = append(rest, operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusSkipped, Detail: "not run: O1.deploy failed"})
		}
		return rest, nil
	}
	if w.live != nil {
		return slices.Clone(w.live), nil
	}
	var checks []operability.Check
	for _, id := range []string{"O1.deploy", "O1.ready", "O2.stamps", "O6.budget"} {
		checks = append(checks, operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass})
	}
	return checks, nil
}

func (w *advWorld) CheckOffline(_ context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.OfflineChecks, error) {
	w.log("offline")
	if checkout != advCheckout || !proto.Equal(plan, w.plan) {
		return lifecycle.OfflineChecks{}, errors.New("offline checks of another checkout or plan")
	}
	if len(w.offlineErrs) > 0 {
		err := w.offlineErrs[0]
		w.offlineErrs = w.offlineErrs[1:]
		return lifecycle.OfflineChecks{}, err
	}
	checks := slices.Clone(w.offline)
	if w.offline == nil {
		for _, id := range []string{"O1.patches", "O1.render", "O2.images", "O6.scope"} {
			checks = append(checks, operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusPass})
		}
	}
	return lifecycle.OfflineChecks{Checks: checks, BaseTree: advTree, EffectiveTree: advEffective, PatchesDigest: advDigest, RenderDigest: advDigest, EnvDigest: advDigest}, nil
}

func (w *advWorld) VerifyAccepted(context.Context, string, string, lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	return lifecycle.DeploymentReceipt{}, errors.New("an advance never re-verifies an accepted commit")
}

func (w *advWorld) VerifyCandidate(_ context.Context, root, commit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	w.log("verify %s", commit[:4])
	if root != advCheckout || commit != advCandidate || namespace != "toy" || objective == "" || source.BaseTree != w.applied.BaseTree {
		return lifecycle.HealthVerdict{}, errors.New("verified something other than the deployed candidate")
	}
	if len(w.verdicts) == 0 {
		return lifecycle.HealthVerdict{Outcome: lifecycle.VerificationClear}, nil
	}
	verdict := w.verdicts[0]
	if len(w.verdicts) > 1 {
		w.verdicts = w.verdicts[1:]
	}
	return verdict, nil
}

func newAdvWorld(t *testing.T) *advWorld {
	t.Helper()
	config, err := anypb.New(&structpb.Struct{})
	if err != nil {
		t.Fatal(err)
	}
	return &advWorld{
		plan:   &contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "synthetic", Config: config}, ExpectedWorkloadIds: []string{"Deployment/toy/toy"}},
		shapes: map[string]string{advHead: "shape-a", advCandidate: "shape-a"},
	}
}

func (w *advWorld) advancer() lifecycle.Advancer {
	a := lifecycle.Advancer{Upstreams: w, Broker: w, Readiness: w, Verifier: w, Shape: w.shape, Maintenance: w, Activity: w, Wait: func(context.Context, time.Duration) error { return nil }}
	if w.designer {
		a.Designer = w
	}
	return a
}

func (w *advWorld) RefreshDetection(_ context.Context, change lifecycle.ShapeChange) error {
	w.log("designer %s", change.Upstream[:4])
	if change.Accepted == "" {
		return errors.New("designer refresh before the advance was accepted")
	}
	return w.designerErr
}

func advRequest(t *testing.T) upstream.Request {
	t.Helper()
	config, err := anypb.New(&structpb.Struct{})
	if err != nil {
		t.Fatal(err)
	}
	return upstream.Request{Source: upstream.Source{Path: ".", Kind: "git", Config: config}, Revision: upstream.Revision{ID: advRevision, Ref: "main"}, Prev: advPrev}
}

var advConfig = lifecycle.AdvanceConfig{Namespace: "toy", HealthObjective: "serve requests", GitDir: "/synthetic/accepted/.git", PendingEvaluations: 3, InfrastructureTries: 2}

func (w *advWorld) run(t *testing.T) (lifecycle.AdvanceResult, error) {
	t.Helper()
	return w.advancer().Advance(context.Background(), advConfig, advRequest(t))
}

func TestAdvanceAcceptsADeployedAndVerifiedRevision(t *testing.T) {
	w := newAdvWorld(t)
	result, err := w.run(t)
	if err != nil || result.Outcome != lifecycle.AdvanceAccepted || result.Upstream != advRevision {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	id := lifecycle.AdvanceID(advRevision)
	want := []string{"materialize git " + advRevision, "change 1111..2222", "propose " + id, "shape 3333", "shape 4444", "offline", "pause", "apply", "live", "verify 4444", "publish " + id, "resume"}
	if !slices.Equal(w.events, want) {
		t.Fatalf("events\n got %q\nwant %q", w.events, want)
	}
	if len(w.published) != 1 || result.Receipt.Commit == "" {
		t.Fatalf("published %d, receipt %+v", len(w.published), result.Receipt)
	}
	if out := result.FollowerOutcome(); out.Kind != upstream.Accepted {
		t.Fatalf("follower outcome = %+v", out)
	}
}

// Each refusal of the revision is a typed hold. The cluster is touched only
// once impact passed, maintenance is resumed whenever it was paused, nothing
// is published, the proposal is released, and Go never rolls back (one apply).
func TestAdvanceHoldsEveryRefusedRevision(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*advWorld)
		hold    lifecycle.HoldReason
		paths   []string
		touched bool
		// routing is the persisted prefix: cause, owner and the report items.
		routing string
	}{
		{name: "ref rewritten", mutate: func(w *advWorld) { w.notAncestor = true }, hold: lifecycle.HoldRefRewritten, routing: "cause=environment owner=human items=-"},
		{name: "upstream edits operational memory", mutate: func(w *advWorld) { w.changed = []string{"main.go", ".sdo/goal/goal.md"} }, hold: lifecycle.HoldMemoryTouched, routing: "cause=scope owner=human items=O6", paths: []string{".sdo/goal/goal.md"}},
		{name: "merge conflict", mutate: func(w *advWorld) { w.conflicts = []string{"Dockerfile"} }, hold: lifecycle.HoldMergeConflict, routing: "cause=patch owner=deployer items=O1", paths: []string{"Dockerfile"}},
		{name: "candidate does not render", mutate: func(w *advWorld) { w.shapeErr = map[string]error{advCandidate: errors.New("template: bad")} }, hold: lifecycle.HoldRender, routing: "cause=render owner=deployer items=O1"},
		{name: "shape change", mutate: func(w *advWorld) { w.shapes[advCandidate] = "shape-b" }, hold: lifecycle.HoldShapeChange, routing: "cause=coverage owner=designer items=O3"},
		{name: "image build fails", mutate: func(w *advWorld) {
			w.applyErrs = []error{fmt.Errorf("resolve images: %w", &imagesrc.Error{Image: "toy", SourceID: "buildkit", Err: errors.New("compile error")})}
		}, hold: lifecycle.HoldBuild, touched: true, routing: "cause=build owner=deployer items=O1"},
		{name: "rollout fails", mutate: func(w *advWorld) { w.applyErrs = []error{errors.New("workload toy not ready")} }, hold: lifecycle.HoldRollout, touched: true, routing: "cause=rollout owner=responder items=O1"},
		{name: "receipt of another source", mutate: func(w *advWorld) {
			w.receipt = func(r lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt {
				r.Source = &contracts.DeploymentSource{RendererId: "other", Config: r.Source.GetConfig()}
				return r
			}
		}, hold: lifecycle.HoldRollout, touched: true, routing: "cause=rollout owner=responder items=O1"},
		{name: "incomplete receipt", mutate: func(w *advWorld) {
			w.receipt = func(r lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt { r.Ready = false; return r }
		}, hold: lifecycle.HoldRollout, touched: true, routing: "cause=rollout owner=responder items=O1"},
		{name: "health regression", mutate: func(w *advWorld) {
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationSourceFault, Summary: "firing"}}
		}, hold: lifecycle.HoldHealth, touched: true, routing: "cause=source_fault owner=responder items=O4"},
		{name: "health never settles", mutate: func(w *advWorld) {
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationPending}}
		}, hold: lifecycle.HoldHealth, touched: true, routing: "cause=probe_unavailable owner=designer items=O4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdvWorld(t)
			tc.mutate(w)
			result, err := w.run(t)
			if err != nil || result.Outcome != lifecycle.AdvanceHeld || result.Hold != tc.hold {
				t.Fatalf("result = %+v, err = %v; want hold %s", result, err, tc.hold)
			}
			if tc.paths != nil && !slices.Equal(result.Paths, tc.paths) {
				t.Fatalf("paths = %v, want %v", result.Paths, tc.paths)
			}
			if out := result.FollowerOutcome(); out.Kind != upstream.Refused || !strings.HasPrefix(out.Reason, tc.routing+" "+string(tc.hold)) {
				t.Fatalf("follower outcome = %+v, want the prefix %q", out, tc.routing+" "+string(tc.hold))
			}
			if len(w.published) != 0 || count(w.events, "publish") != 0 {
				t.Fatalf("a held revision was published: %q", w.events)
			}
			if applies := count(w.events, "apply"); (applies == 1) != tc.touched || applies > 1 {
				t.Fatalf("applies = %d, want touched=%v (and no rollback apply): %q", applies, tc.touched, w.events)
			}
			if count(w.events, "pause") != count(w.events, "resume") || (count(w.events, "pause") == 1) != tc.touched {
				t.Fatalf("maintenance not paused exactly around the cluster change: %q", w.events)
			}
			if count(w.events, "propose") != count(w.events, "release") {
				t.Fatalf("proposal leaked: %q", w.events)
			}
		})
	}
}

func TestAdvanceRetriesTransientFailuresWithoutAHold(t *testing.T) {
	infra := &infrastructureFailure{}
	for _, tc := range []struct {
		name   string
		mutate func(*advWorld)
		check  func(*testing.T, error)
	}{
		{name: "upstream unavailable", mutate: func(w *advWorld) { w.materializeErr = fmt.Errorf("fetch: %w", upstream.ErrUnavailable) }, check: func(t *testing.T, err error) {
			if !errors.Is(err, upstream.ErrUnavailable) {
				t.Fatalf("err = %v", err)
			}
		}},
		{name: "infrastructure failure on every try", mutate: func(w *advWorld) { w.applyErrs = []error{infra, infra, infra} }, check: func(t *testing.T, err error) {
			var exhausted *lifecycle.InfrastructureError
			if !errors.As(err, &exhausted) || exhausted.Attempts != 2 {
				t.Fatalf("err = %v", err)
			}
		}},
		{name: "broker rejects the publication", mutate: func(w *advWorld) { w.publishErr = errors.New("accepted HEAD moved") }, check: func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "accepted HEAD moved") {
				t.Fatalf("err = %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdvWorld(t)
			tc.mutate(w)
			result, err := w.run(t)
			tc.check(t, err)
			if result.Outcome == lifecycle.AdvanceHeld || result.FollowerOutcome().Kind != upstream.Retry {
				t.Fatalf("a transient failure held the revision: %+v", result)
			}
			if len(w.published) != 0 || count(w.events, "pause") != count(w.events, "resume") || count(w.events, "propose") != count(w.events, "release") {
				t.Fatalf("effects left behind: %q", w.events)
			}
		})
	}
}

func TestAdvanceRetriesAnInfrastructureFailureThenAccepts(t *testing.T) {
	w := newAdvWorld(t)
	w.applyErrs = []error{&infrastructureFailure{}, nil}
	w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationPending}, {Outcome: lifecycle.VerificationClear}}
	result, err := w.run(t)
	if err != nil || result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if count(w.events, "apply") != 2 || count(w.events, "verify") != 2 {
		t.Fatalf("events = %q", w.events)
	}
}

func TestAdvanceDefersWhileTheControllerIsBusy(t *testing.T) {
	w := newAdvWorld(t)
	w.busy = "incident inc-1 open"
	result, err := w.run(t)
	if err != nil || result.Outcome != lifecycle.AdvanceDeferred || result.Detail != w.busy || result.FollowerOutcome().Kind != upstream.Retry {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if count(w.events, "apply") != 0 || count(w.events, "resume") != 1 || count(w.events, "release") != 1 {
		t.Fatalf("events = %q", w.events)
	}
}

func TestAdvanceWithoutAnAcceptedPlanIsUnavailable(t *testing.T) {
	for name, mutate := range map[string]func(*advWorld){
		"no evidence":                  func(w *advWorld) { w.noEvidence = true },
		"no plan in the accepted tree": func(w *advWorld) { w.noPlan = true },
		"tree has no plan.yaml":        func(w *advWorld) { w.planAbsent = true },
	} {
		t.Run(name, func(t *testing.T) {
			w := newAdvWorld(t)
			mutate(w)
			_, err := w.run(t)
			if !errors.Is(err, lifecycle.ErrNoAcceptedPlan) || count(w.events, "propose") != 0 {
				t.Fatalf("err = %v, events = %q", err, w.events)
			}
		})
	}
}

// Negative control for the fake broker itself: evidence that is not of the
// candidate tree is refused, so the acceptance test above is not vacuous.
func TestFakeBrokerRefusesEvidenceOfAnotherTree(t *testing.T) {
	w := newAdvWorld(t)
	w.receipt = func(r lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt {
		r.BaseTree, r.EffectiveTree, r.ObservedRevision = strings.Repeat("9", 40), strings.Repeat("9", 40), strings.Repeat("9", 40)
		return r
	}
	w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationClear}}
	result, err := w.run(t)
	if err == nil || !strings.Contains(err.Error(), "not of the candidate tree") || result.Outcome == lifecycle.AdvanceAccepted || len(w.published) != 0 {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestAdvanceRejectsIncompleteWiring(t *testing.T) {
	w := newAdvWorld(t)
	advancer := w.advancer()
	advancer.Activity = nil
	if _, err := advancer.Advance(context.Background(), advConfig, advRequest(t)); err == nil || len(w.events) != 0 {
		t.Fatalf("err = %v, events = %q", err, w.events)
	}
	bad := advRequest(t)
	bad.Prev = ""
	if _, err := w.advancer().Advance(context.Background(), advConfig, bad); err == nil || len(w.events) != 0 {
		t.Fatalf("a request without the previous upstream commit started: %v %q", err, w.events)
	}
}

type infrastructureFailure struct{}

func (*infrastructureFailure) Error() string               { return "registry rate limited" }
func (*infrastructureFailure) InfrastructureOutcome() bool { return true }

func count(events []string, prefix string) int {
	n := 0
	for _, event := range events {
		if strings.HasPrefix(event, prefix) {
			n++
		}
	}
	return n
}

// A patch that does not apply is a typed hold decided before the cluster is
// touched: no pause, no apply, no publish, and the hold text names what an
// operator or the deployment agent needs. The broker raises it when it
// composes the candidate; the same error out of the impact render (which reads
// the effective tree) is the same hold.
func TestAdvanceHoldsAPatchThatDoesNotApplyBeforeAnyClusterEffect(t *testing.T) {
	conflict := &patchset.PatchError{Kind: patchset.KindConflict, Patch: "0001-pin-base.patch", Paths: []string{"vendor/lib.go"}, Detail: "hunk #1 failed\nat line 12"}
	for _, tc := range []struct {
		name   string
		mutate func(*advWorld)
		hold   lifecycle.HoldReason
		want   []string
	}{
		{name: "conflict from the broker", mutate: func(w *advWorld) {
			w.proposeErr = &broker.PatchHoldError{Candidate: advCandidate, Patch: conflict, Owner: "deployer", UpstreamCommits: []string{"abc1234 rewrite lib", "def5678 rename helper"}}
		}, hold: lifecycle.HoldPatchConflict, want: []string{"patch 0001-pin-base.patch (owner deployer): conflict, hunk #1 failed at line 12", "abc1234 rewrite lib, def5678 rename helper"}},
		{name: "conflict from the impact render", mutate: func(w *advWorld) {
			w.shapeErr = map[string]error{advCandidate: fmt.Errorf("render: %w", conflict)}
		}, hold: lifecycle.HoldPatchConflict, want: []string{"patch 0001-pin-base.patch: conflict"}},
		{name: "missing file is a conflict", mutate: func(w *advWorld) {
			w.proposeErr = &broker.PatchHoldError{Patch: &patchset.PatchError{Kind: patchset.KindMissingFile, Patch: "0002-x.patch", Paths: []string{"gone.go"}}}
		}, hold: lifecycle.HoldPatchConflict, want: []string{"missing_file"}},
		{name: "upstreamed", mutate: func(w *advWorld) {
			w.proposeErr = &broker.PatchHoldError{Patch: &patchset.PatchError{Kind: patchset.KindUpstreamed, Patch: "0001-fix.patch", Paths: []string{"a.go"}}, Owner: "responder"}
		}, hold: lifecycle.HoldPatchUpstreamed, want: []string{"owner responder", "retire this patch"}},
		{name: "invalid", mutate: func(w *advWorld) {
			w.proposeErr = &broker.PatchHoldError{Patch: &patchset.PatchError{Kind: patchset.KindForbidden, Patch: "0003-bad.patch", Detail: "binary"}}
		}, hold: lifecycle.HoldPatchInvalid, want: []string{"forbidden"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdvWorld(t)
			tc.mutate(w)
			result, err := w.run(t)
			if err != nil || result.Outcome != lifecycle.AdvanceHeld || result.Hold != tc.hold {
				t.Fatalf("result = %+v, err = %v; want hold %s", result, err, tc.hold)
			}
			out := result.FollowerOutcome()
			if out.Kind != upstream.Refused || !strings.HasPrefix(out.Reason, "cause=patch owner=deployer items=O1 "+string(tc.hold)+": ") {
				t.Fatalf("follower outcome = %+v", out)
			}
			for _, want := range tc.want {
				if !strings.Contains(out.Reason, want) {
					t.Errorf("hold %q lacks %q", out.Reason, want)
				}
			}
			for _, never := range []string{"pause", "apply", "verify", "publish"} {
				if count(w.events, never) != 0 {
					t.Fatalf("%s ran for a patch hold: %q", never, w.events)
				}
			}
		})
	}
}

// Negative control: an unrelated propose failure is a retry, never a patch hold.
func TestAdvanceDoesNotMistakeAProposeFailureForAPatchHold(t *testing.T) {
	w := newAdvWorld(t)
	w.proposeErr = errors.New("disk full")
	result, err := w.run(t)
	if err == nil || result.Outcome == lifecycle.AdvanceHeld {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

// A shape change no longer holds the advance: it is deployed, verified and
// accepted, then the detection designer refreshes for the new shape, after
// maintenance resumed (the controller detects while the designer works).
func TestAdvanceRoutesAShapeChangeToTheDetectionDesigner(t *testing.T) {
	w := newAdvWorld(t)
	w.designer = true
	w.shapes[advCandidate] = "shape-b"
	result, err := w.run(t)
	if err != nil || result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	id := lifecycle.AdvanceID(advRevision)
	want := []string{"materialize git " + advRevision, "change 1111..2222", "propose " + id, "shape 3333", "shape 4444", "offline", "pause", "apply", "live", "verify 4444", "publish " + id, "resume", "designer 2222"}
	if !slices.Equal(w.events, want) {
		t.Fatalf("events\n got %q\nwant %q", w.events, want)
	}
}

func TestAdvanceHoldsOnlyWhenTheDesignerRefreshFails(t *testing.T) {
	w := newAdvWorld(t)
	w.designer = true
	w.shapes[advCandidate] = "shape-b"
	w.designerErr = errors.New("operability gate rejected the designer's detectors")
	result, err := w.run(t)
	if err != nil || result.Outcome != lifecycle.AdvanceHeld || result.Hold != lifecycle.HoldDesigner {
		t.Fatalf("result = %+v, err = %v; want hold %s", result, err, lifecycle.HoldDesigner)
	}
	if result.Receipt.Commit == "" || len(w.published) != 1 || !strings.Contains(result.Detail, result.Receipt.Commit) || !strings.Contains(result.Detail, "operability gate") {
		t.Fatalf("the hold must name the accepted commit and the refresh failure: %+v", result)
	}
	if out := result.FollowerOutcome(); out.Kind != upstream.Refused || !strings.Contains(out.Reason, " "+string(lifecycle.HoldDesigner)+": ") {
		t.Fatalf("follower outcome = %+v", out)
	}
	if count(w.events, "apply") != 1 || count(w.events, "pause") != count(w.events, "resume") {
		t.Fatalf("events: %q", w.events)
	}
}

// The designer runs for an accepted shape change only: an unchanged shape, a
// held advance and a deferred advance never reach it.
func TestAdvanceDoesNotCallTheDesignerWithoutAnAcceptedShapeChange(t *testing.T) {
	for name, mutate := range map[string]func(*advWorld){
		"shape unchanged": func(*advWorld) {},
		"health regression": func(w *advWorld) {
			w.shapes[advCandidate] = "shape-b"
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationSourceFault, Summary: "firing"}}
		},
		"controller busy": func(w *advWorld) { w.shapes[advCandidate] = "shape-b"; w.busy = "incident open" },
		"candidate does not render": func(w *advWorld) {
			w.shapeErr = map[string]error{advCandidate: errors.New("template: bad")}
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newAdvWorld(t)
			w.designer = true
			mutate(w)
			if _, err := w.run(t); err != nil {
				t.Fatal(err)
			}
			if count(w.events, "designer") != 0 {
				t.Fatalf("designer ran: %q", w.events)
			}
		})
	}
}

// The broker receives the stage-advance report of what was deployed: the
// accepted head the merge was made on, the receipt's trees and digests, the
// deployment checks and the independent detector verdict.
func TestAdvancePublishesTheStageAdvanceReport(t *testing.T) {
	w := newAdvWorld(t)
	if result, err := w.run(t); err != nil || result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	report := w.reports[0]
	if report.Stage != operability.StageAdvance || report.Subject.AcceptedCommit != advHead || report.Subject.BaseTree != advTree || report.Subject.EffectiveTree != advEffective {
		t.Fatalf("report subject/stage = %s %+v", report.Stage, report.Subject)
	}
	var ids []string
	for _, item := range report.Items {
		for _, c := range item.Checks {
			ids = append(ids, c.ID)
		}
	}
	if want := []string{"O1.patches", "O1.render", "O1.deploy", "O1.ready", "O2.images", "O2.stamps", "O4.detectors", "O6.scope", "O6.budget"}; !slices.Equal(ids, want) {
		t.Fatalf("report checks = %v, want %v", ids, want)
	}
}

// A failing live deployment check is a hold: the detectors are not evaluated
// (the check says so, never a pass) and nothing is published.
func TestAdvanceHoldsAFailingLiveCheckWithoutEvaluatingDetectors(t *testing.T) {
	w := newAdvWorld(t)
	w.live = failLive("O1.deploy", operability.CauseRollout)
	result, err := w.run(t)
	if err != nil || result.Outcome != lifecycle.AdvanceHeld || !strings.Contains(result.Detail, "O1.deploy [rollout]") {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if count(w.events, "verify") != 0 || len(w.published) != 0 || count(w.events, "apply") != 1 {
		t.Fatalf("events = %q", w.events)
	}
}

func offlineFailure(id string, cause operability.Cause) []operability.Check {
	return []operability.Check{
		{ID: "O1.patches", Phase: operability.PhaseOffline, Status: operability.StatusPass},
		{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: cause, Detail: "synthetic " + string(cause), Paths: []string{"deploy/values.yaml"}},
		{ID: "O2.images", Phase: operability.PhaseOffline, Status: operability.StatusPass},
		{ID: "O6.allowlist", Phase: operability.PhaseOffline, Status: operability.StatusPass},
	}
}

// The offline subset runs before the maintenance pause: a failing offline check
// holds the revision with the cause, owner and items of the check and no
// cluster effect at all.
func TestAdvanceHoldsAnOfflineFailureBeforeAnyClusterEffect(t *testing.T) {
	for _, tc := range []struct {
		name    string
		checks  []operability.Check
		hold    lifecycle.HoldReason
		routing string
	}{
		{"render", offlineFailure("O1.render", operability.CauseRender), lifecycle.HoldRender, "cause=render owner=deployer items=O1"},
		{"nondeterministic render", offlineFailure("O1.deterministic", operability.CauseRenderNondeterministic), lifecycle.HoldRender, "cause=render_nondeterministic owner=deployer items=O1"},
		{"patch", offlineFailure("O1.patches", operability.CausePatch), lifecycle.HoldPatchConflict, "cause=patch owner=deployer items=O1"},
		{"image declaration", offlineFailure("O2.images", operability.CauseBuild), lifecycle.HoldBuild, "cause=build owner=deployer items=O2"},
		{"scope", offlineFailure("O6.scope", operability.CauseScope), lifecycle.HoldScope, "cause=scope owner=human items=O6"},
		{"registry allowlist", offlineFailure("O6.allowlist", operability.CauseAllowlist), lifecycle.HoldScope, "cause=allowlist owner=human items=O6"},
		{"an SDO defect", offlineFailure("O2.images", operability.CauseAttribution), lifecycle.HoldOperability, "cause=attribution owner=operator items=O2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdvWorld(t)
			w.offline = tc.checks
			result, err := w.run(t)
			if err != nil || result.Outcome != lifecycle.AdvanceHeld || result.Hold != tc.hold {
				t.Fatalf("result = %+v, err = %v; want hold %s", result, err, tc.hold)
			}
			if out := result.FollowerOutcome(); !strings.HasPrefix(out.Reason, tc.routing+" "+string(tc.hold)+": ") || !strings.Contains(out.Reason, "(deploy/values.yaml)") {
				t.Fatalf("reason = %q, want prefix %q", out.Reason, tc.routing)
			}
			for _, effect := range []string{"pause", "apply", "live", "verify", "publish"} {
				if count(w.events, effect) != 0 {
					t.Fatalf("%s ran after an offline failure: %q", effect, w.events)
				}
			}
			if count(w.events, "propose") != count(w.events, "release") {
				t.Fatalf("proposal leaked: %q", w.events)
			}
		})
	}
}

// An infrastructure failure of the offline checks is retried and never a hold;
// one that persists ends the attempt as an error for the follower to retry.
func TestAdvanceRetriesOfflineInfrastructureWithoutAHold(t *testing.T) {
	w := newAdvWorld(t)
	w.offlineErrs = []error{&infrastructureFailure{}}
	if result, err := w.run(t); err != nil || result.Outcome != lifecycle.AdvanceAccepted || count(w.events, "offline") != 2 {
		t.Fatalf("result = %+v, err = %v, events = %q", result, err, w.events)
	}
	w = newAdvWorld(t)
	w.offline = offlineFailure("O1.patches", operability.CauseInfrastructure)
	result, err := w.run(t)
	if err == nil || result.Outcome == lifecycle.AdvanceHeld || count(w.events, "pause") != 0 {
		t.Fatalf("result = %+v, err = %v, events = %q", result, err, w.events)
	}
}

type liveOnlyReadiness struct {
	lifecycle.SourceReadinessVerifier
	lifecycle.LiveChecker
}

// A verifier that cannot check offline contributes no offline checks (the live
// subset still runs); the offline checks of one that can are in the report the
// broker records.
func TestAdvanceOfflineChecksAreOptionalButRecorded(t *testing.T) {
	w := newAdvWorld(t)
	advancer := w.advancer()
	advancer.Readiness = liveOnlyReadiness{w, w}
	if result, err := advancer.Advance(context.Background(), advConfig, advRequest(t)); err != nil || result.Outcome != lifecycle.AdvanceAccepted || count(w.events, "offline") != 0 {
		t.Fatalf("result = %+v, err = %v, events = %q", result, err, w.events)
	}
	offline := newAdvWorld(t)
	if result, err := offline.run(t); err != nil || result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	var found bool
	for _, item := range offline.reports[0].Items {
		for _, c := range item.Checks {
			found = found || c.ID == "O1.patches"
		}
	}
	if !found {
		t.Fatal("the offline checks are not in the report the broker records")
	}
}

// E8 after the cluster changed: Go's provenance rule routes a firing detector
// the operator wrote (the floor) to the responder as an application fault, and
// only the designer's own detectors firing to the designer. Neither is accepted,
// and the cluster is left as it is.
func TestAdvanceRoutesAHealthRegressionByDetectorProvenance(t *testing.T) {
	for _, tc := range []struct {
		name    string
		verdict lifecycle.HealthVerdict
		routing string
	}{
		{"floor detector fires", firing, "cause=source_fault owner=responder items=O4"},
		{"only the designer's detector fires", judgeFiring, "cause=detector_fault owner=designer items=O4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdvWorld(t)
			w.verdicts = []lifecycle.HealthVerdict{tc.verdict}
			result, err := w.run(t)
			if err != nil || result.Outcome != lifecycle.AdvanceHeld || result.Hold != lifecycle.HoldHealth {
				t.Fatalf("result = %+v, err = %v", result, err)
			}
			if out := result.FollowerOutcome(); !strings.HasPrefix(out.Reason, tc.routing+" health: ") {
				t.Fatalf("reason = %q, want prefix %q", out.Reason, tc.routing)
			}
			if len(w.published) != 0 || count(w.events, "apply") != 1 {
				t.Fatalf("events = %q", w.events)
			}
		})
	}
}

// E7 after apply: a rollout failure is the responder's (an incident), never a
// correction for the deployer, whose candidate is already applied.
func TestAdvanceRoutesARolloutFailureToTheResponder(t *testing.T) {
	w := newAdvWorld(t)
	w.applyErrs = []error{errors.New("workload toy not ready")}
	result, err := w.run(t)
	if err != nil || result.Hold != lifecycle.HoldRollout || result.Owner != operability.OwnerResponder || result.Cause != operability.CauseRollout {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	w = newAdvWorld(t)
	w.live = failLive("O1.ready", operability.CauseRollout)
	if result, err := w.run(t); err != nil || result.Hold != lifecycle.HoldRollout || result.Owner != operability.OwnerResponder {
		t.Fatalf("a receipt whose workloads are not ready: result = %+v, err = %v", result, err)
	}
}
