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
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/upstream"
)

// Synthetic identities. The advance never interprets them beyond equality.
var (
	advPrev      = strings.Repeat("1", 40)
	advRevision  = strings.Repeat("2", 40)
	advHead      = strings.Repeat("3", 40)
	advCandidate = strings.Repeat("4", 40)
	advTree      = strings.Repeat("5", 40)
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

	applied   lifecycle.DeploymentReceipt
	proposed  map[string]bool
	published []broker.SourceEvidence
}

func (w *advWorld) log(format string, args ...any) { w.events = append(w.events, fmt.Sprintf(format, args...)) }

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

func (w *advWorld) AcceptedSourceEvidence(context.Context) (broker.SourceEvidence, bool, error) {
	if w.noEvidence {
		return broker.SourceEvidence{}, false, nil
	}
	evidence := broker.SourceEvidence{DeploymentSource: w.plan.GetSource(), Plan: w.plan, AcceptedCommit: advHead}
	if w.noPlan {
		evidence.Plan = nil
	}
	return evidence, true, nil
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

func (w *advWorld) PublishAdvance(_ context.Context, id, upstreamCommit, base string, evidence broker.SourceEvidence) (broker.Receipt, error) {
	w.log("publish %s", id)
	switch {
	case !w.proposed[id]:
		return broker.Receipt{}, errors.New("no open advance proposal")
	case upstreamCommit != advRevision || base != advPrev:
		return broker.Receipt{}, errors.New("trailers differ from the proposal's merge")
	case evidence.BaseTree != advTree || evidence.ObservedRevision != advTree || !evidence.Ready:
		return broker.Receipt{}, errors.New("evidence is not of the candidate tree")
	case !proto.Equal(evidence.Plan, w.plan) || !proto.Equal(evidence.DeploymentSource, w.plan.GetSource()):
		return broker.Receipt{}, errors.New("evidence does not carry the accepted plan")
	case evidence.RenderDigest == "" || evidence.EnvDigest == "" || len(evidence.Images) == 0 || len(evidence.ResourceIDs) == 0:
		return broker.Receipt{}, errors.New("incomplete evidence")
	case w.publishErr != nil:
		return broker.Receipt{}, w.publishErr
	}
	delete(w.proposed, id)
	w.published = append(w.published, evidence)
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
	receipt := lifecycle.DeploymentReceipt{Source: proto.Clone(plan.GetSource()).(*contracts.DeploymentSource), BaseTree: advTree, ObservedRevision: advTree, RenderDigest: advDigest, EnvDigest: advDigest, Images: []string{"toy@sha256:" + advDigest}, ResourceIDs: []string{"Deployment/toy/toy"}, Ready: true}
	if w.receipt != nil {
		receipt = w.receipt(receipt)
	}
	w.applied = receipt
	return receipt, nil
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
	return lifecycle.Advancer{Upstreams: w, Broker: w, Readiness: w, Verifier: w, Shape: w.shape, Maintenance: w, Activity: w, Wait: func(context.Context, time.Duration) error { return nil }}
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
	want := []string{"materialize git " + advRevision, "change 1111..2222", "propose " + id, "shape 3333", "shape 4444", "pause", "apply", "verify 4444", "publish " + id, "resume"}
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
	}{
		{name: "ref rewritten", mutate: func(w *advWorld) { w.notAncestor = true }, hold: lifecycle.HoldRefRewritten},
		{name: "upstream edits operational memory", mutate: func(w *advWorld) { w.changed = []string{"main.go", ".sdo/goal/goal.md"} }, hold: lifecycle.HoldMemoryTouched, paths: []string{".sdo/goal/goal.md"}},
		{name: "merge conflict", mutate: func(w *advWorld) { w.conflicts = []string{"Dockerfile"} }, hold: lifecycle.HoldMergeConflict, paths: []string{"Dockerfile"}},
		{name: "candidate does not render", mutate: func(w *advWorld) { w.shapeErr = map[string]error{advCandidate: errors.New("template: bad")} }, hold: lifecycle.HoldRender},
		{name: "shape change", mutate: func(w *advWorld) { w.shapes[advCandidate] = "shape-b" }, hold: lifecycle.HoldShapeChange},
		{name: "image build fails", mutate: func(w *advWorld) {
			w.applyErrs = []error{fmt.Errorf("resolve images: %w", &imagesrc.Error{Image: "toy", SourceID: "buildkit", Err: errors.New("compile error")})}
		}, hold: lifecycle.HoldBuild, touched: true},
		{name: "rollout fails", mutate: func(w *advWorld) { w.applyErrs = []error{errors.New("workload toy not ready")} }, hold: lifecycle.HoldRollout, touched: true},
		{name: "receipt of another source", mutate: func(w *advWorld) {
			w.receipt = func(r lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt {
				r.Source = &contracts.DeploymentSource{RendererId: "other", Config: r.Source.GetConfig()}
				return r
			}
		}, hold: lifecycle.HoldRollout, touched: true},
		{name: "incomplete receipt", mutate: func(w *advWorld) {
			w.receipt = func(r lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt { r.Ready = false; return r }
		}, hold: lifecycle.HoldRollout, touched: true},
		{name: "health regression", mutate: func(w *advWorld) {
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationSourceFault, Summary: "firing"}}
		}, hold: lifecycle.HoldHealth, touched: true},
		{name: "health never settles", mutate: func(w *advWorld) {
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationPending}}
		}, hold: lifecycle.HoldHealth, touched: true},
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
			if out := result.FollowerOutcome(); out.Kind != upstream.Refused || !strings.HasPrefix(out.Reason, string(tc.hold)) {
				t.Fatalf("follower outcome = %+v", out)
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
		"no evidence":          func(w *advWorld) { w.noEvidence = true },
		"evidence has no plan": func(w *advWorld) { w.noPlan = true },
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
		r.BaseTree, r.ObservedRevision = strings.Repeat("9", 40), strings.Repeat("9", 40)
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

func (*infrastructureFailure) Error() string              { return "registry rate limited" }
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
			if out.Kind != upstream.Refused || !strings.HasPrefix(out.Reason, string(tc.hold)+": ") {
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
