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
	"sdo.dev/operator/planfile"
)

// Synthetic identities. The advance never interprets them beyond equality.
var (
	advPrev      = strings.Repeat("1", 40)
	advRevision  = strings.Repeat("2", 40)
	advHead      = strings.Repeat("3", 40)
	advCandidate = strings.Repeat("4", 40)
	advCorrected = strings.Repeat("8", 40)
	advTree      = strings.Repeat("5", 40)
	advEffective = strings.Repeat("7", 40)
	advDigest    = strings.Repeat("6", 64)
)

const advCheckout = "/synthetic/proposals/advance/checkout"

// advWorld records every effect of an advance in one ordered log, so tests
// assert what happened and, as important, what never happened.
type advWorld struct {
	clock  time.Time
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
	expose         bool
	exposeErr      error
	exposed        []operability.Report
	// live replaces the live deployment checks CheckLive returns (nil: all
	// pass); liveCalls counts them.
	live      []operability.Check
	liveCalls int
	// offline replaces the offline checks CheckOffline returns (nil: all
	// pass); offlineErrs is consumed one per call (infrastructure failures).
	offline     []operability.Check
	offlineErrs []error
	// offlineSeq, when set, gives each CheckOffline call the next element (the
	// last repeats); correct, when set, answers each CorrectAdvance call.
	offlineSeq [][]operability.Check
	// unused is reported as the first offline check's unused image
	// declarations.
	unused    []string
	correct   func(call int, plan *contracts.DeploymentPlan) (rejection string, err error)
	corrected []*contracts.DeploymentPlan
	// keepOnPatchHold keeps the advance checkout when the proposal reports a
	// patch hold, as the broker does for a deployment agent to correct it.
	keepOnPatchHold bool
	candidateNow    string
	// checkRejection is what CheckAdvance reports (the publication validation).
	checkRejection string
	// ackAfter is how many Acknowledged polls answer "not yet" after the
	// pause; neverAck keeps answering so.
	ackAfter, ackPolls int
	neverAck           bool
	ackErr             error
	// resumeFailures is how many Resume calls fail before one succeeds;
	// pauseExpiresAt is the deadline the pause was declared with.
	resumeFailures int
	pauseExpiresAt time.Time

	applied   lifecycle.DeploymentReceipt
	proposed  map[string]bool
	published []broker.SourceEvidence
	reports   []operability.Report
}

func (w *advWorld) log(format string, args ...any) {
	w.events = append(w.events, fmt.Sprintf(format, args...))
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

func (w *advWorld) Pause(_ context.Context, reason string, expiresAt time.Time) error {
	w.log("pause")
	w.pauseExpiresAt = expiresAt
	return nil
}

func (w *advWorld) Acknowledged(context.Context) (bool, error) {
	if w.ackErr != nil {
		return false, w.ackErr
	}
	if w.neverAck || w.ackPolls < w.ackAfter {
		w.ackPolls++
		w.log("unacknowledged")
		return false, nil
	}
	if w.ackAfter > 0 {
		w.log("acknowledged")
	}
	return true, nil
}

func (w *advWorld) Resume(context.Context) error {
	w.log("resume")
	if w.resumeFailures > 0 {
		w.resumeFailures--
		return errors.New("API server unavailable")
	}
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
		var typed interface{ Cause() operability.Cause }
		if errors.As(outcome, &image) {
			cause = operability.CauseBuild
		} else if errors.As(outcome, &typed) {
			cause = typed.Cause()
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
	override := w.offline
	if len(w.offlineSeq) > 0 {
		override = w.offlineSeq[0]
		if len(w.offlineSeq) > 1 {
			w.offlineSeq = w.offlineSeq[1:]
		}
	}
	checks := slices.Clone(override)
	if override == nil {
		for _, id := range []string{"O1.patches", "O1.render", "O2.images", "O6.scope"} {
			checks = append(checks, operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusPass})
		}
	}
	unused := w.unused
	w.unused = nil
	return lifecycle.OfflineChecks{Checks: checks, UnusedImages: unused, BaseTree: advTree, EffectiveTree: advEffective, PatchesDigest: advDigest, RenderDigest: advDigest, EnvDigest: advDigest}, nil
}

func (w *advWorld) VerifyAccepted(context.Context, string, string, lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	return lifecycle.DeploymentReceipt{}, errors.New("an advance never re-verifies an accepted commit")
}

func (w *advWorld) VerifyCandidate(_ context.Context, root, commit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	w.log("verify %s", commit[:4])
	want := w.candidateNow
	if want == "" {
		want = advCandidate
	}
	if root != advCheckout || commit != want || namespace != "toy" || objective == "" || source.BaseTree != w.applied.BaseTree {
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

type liveOnlyReadiness struct {
	lifecycle.SourceReadinessVerifier
	lifecycle.LiveChecker
}

// causeFailure is an apply failure the live checks classify as its cause.
type causeFailure operability.Cause

func (c causeFailure) Error() string            { return "synthetic apply failure: " + string(c) }
func (c causeFailure) Cause() operability.Cause { return operability.Cause(c) }
