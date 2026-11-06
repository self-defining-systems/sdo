package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
)

// The staged candidate is the advance fake world's checkout: the fake
// deployer and verifier accept exactly that path and commit.
const repairCheckout = advCheckout

// repairWorld records every effect of one repair in an ordered log.
type repairWorld struct {
	*advWorld
	paths        []string
	stageErr     error
	staleAccepts int
	accepts      []broker.SourceEvidence
}

func newRepairWorld(t *testing.T, paths ...string) *repairWorld {
	t.Helper()
	return &repairWorld{advWorld: newAdvWorld(t), paths: paths}
}

func (w *repairWorld) ChangedPaths(_ context.Context, id string, phase broker.Phase) ([]string, error) {
	w.log("changed %s %s", id, phase)
	return w.paths, nil
}

func (w *repairWorld) Publish(_ context.Context, id string, actor broker.Actor, phase broker.Phase) (broker.Receipt, error) {
	w.log("publish %s %s %s", id, actor, phase)
	return broker.Receipt{IncidentID: id, Phase: phase, Commit: advCandidate}, nil
}

func (w *repairWorld) StageCandidate(_ context.Context, id string, actor broker.Actor, phase broker.Phase) (broker.Candidate, error) {
	w.log("stage %s", id)
	if w.stageErr != nil {
		return broker.Candidate{}, w.stageErr
	}
	return broker.Candidate{IncidentID: id, Actor: actor, Phase: phase, Path: repairCheckout, Parent: advHead, Commit: advCandidate, Tree: advTree}, nil
}

func (w *repairWorld) AcceptDeployed(_ context.Context, candidate broker.Candidate, evidence broker.SourceEvidence) (broker.Receipt, error) {
	w.log("accept %s", candidate.Commit[:4])
	if candidate.Path != repairCheckout || evidence.BaseTree != w.applied.BaseTree || evidence.EffectiveTree != w.applied.EffectiveTree {
		return broker.Receipt{}, errors.New("accepted evidence of something other than the deployed candidate")
	}
	if w.staleAccepts > 0 {
		w.staleAccepts--
		return broker.Receipt{}, broker.ErrCandidateStale
	}
	w.accepts = append(w.accepts, evidence)
	return broker.Receipt{IncidentID: candidate.IncidentID, Phase: candidate.Phase, Commit: candidate.Commit}, nil
}

func (w *repairWorld) DiscardCandidate(id string, phase broker.Phase) error {
	w.log("discard %s %s", id, phase)
	return nil
}

func (w *repairWorld) deployer() lifecycle.RepairDeployer {
	return lifecycle.RepairDeployer{Broker: w, Readiness: w, Verifier: w, Wait: func(context.Context, time.Duration) error { return nil }}
}

var repairConfig = lifecycle.RepairConfig{Namespace: "toy", HealthObjective: "serve requests", PendingEvaluations: 3, InfrastructureTries: 2}

func (w *repairWorld) run() (lifecycle.RepairResult, error) {
	return w.deployer().Repair(context.Background(), repairConfig, "inc-1")
}

const repairPatch = ".sdo/deployment/patches/0001-fix.patch"

// An accepted change to deployment inputs is deployed by the trusted Deployer
// before the broker accepts it: stage, apply the staged tree, verify its
// detectors, then accept with the deployment evidence. The responder applies
// nothing.
func TestRepairDeploysTheStagedTreeThenAccepts(t *testing.T) {
	w := newRepairWorld(t, repairPatch)
	result, err := w.run()
	if err != nil || result.Outcome != lifecycle.RepairDeployed || result.Receipt.Commit != advCandidate {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	want := []string{"changed inc-1 proposal", "stage inc-1", "apply", "verify 4444", "accept 4444"}
	if !slices.Equal(w.events, want) {
		t.Fatalf("events\n got %q\nwant %q", w.events, want)
	}
	if len(w.accepts) != 1 || w.accepts[0].EffectiveTree != advEffective || len(w.accepts[0].Images) == 0 {
		t.Fatalf("recorded evidence = %+v, want the deployment of the staged tree with its images", w.accepts)
	}
}

// Every deployment input class (values, manifests, build, overlays, patches)
// takes the deployed path; a source repair that changes none is published as
// before, and a proposal without a repair does nothing.
func TestRepairDeploysEveryDeploymentInputKind(t *testing.T) {
	for _, tc := range []struct {
		path    string
		outcome lifecycle.RepairOutcome
	}{
		{".sdo/deployment/patches/0002-x.patch", lifecycle.RepairDeployed},
		{".sdo/deployment/values/values.yaml", lifecycle.RepairDeployed},
		{".sdo/deployment/manifests/app.yaml", lifecycle.RepairDeployed},
		{".sdo/deployment/build/Dockerfile", lifecycle.RepairDeployed},
		{".sdo/deployment/overlays/telemetry/kustomization.yaml", lifecycle.RepairDeployed},
		{".sdo/deployment/plan.yaml", lifecycle.RepairPublished},
		{".sdo/playbooks/p/program.go", lifecycle.RepairNone},
	} {
		t.Run(tc.path, func(t *testing.T) {
			w := newRepairWorld(t, tc.path)
			result, err := w.run()
			if err != nil || result.Outcome != tc.outcome {
				t.Fatalf("outcome = %q (%v), want %q", result.Outcome, err, tc.outcome)
			}
			if tc.outcome != lifecycle.RepairDeployed && count(w.events, "apply") != 0 {
				t.Fatalf("a repair without deployment inputs touched the cluster: %q", w.events)
			}
		})
	}
}

// A candidate the cluster or the detectors refuse is never accepted: the stage
// is discarded and the repair is held with a typed reason. Go never rolls back.
func TestRepairHoldsACandidateTheDeploymentOrDetectorsRefuse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*repairWorld)
		hold  lifecycle.HoldReason
	}{
		{"rollout", func(w *repairWorld) { w.applyErrs = []error{errors.New("deployment never became ready")} }, lifecycle.HoldRollout},
		{"build", func(w *repairWorld) { w.applyErrs = []error{&imagesrc.Error{}} }, lifecycle.HoldBuild},
		{"incomplete evidence", func(w *repairWorld) {
			w.receipt = func(r lifecycle.DeploymentReceipt) lifecycle.DeploymentReceipt { r.Ready = false; return r }
		}, lifecycle.HoldRollout},
		{"detectors fire", func(w *repairWorld) {
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationSourceFault, Summary: "health detector fires"}}
		}, lifecycle.HoldHealth},
		{"never settles", func(w *repairWorld) {
			w.verdicts = []lifecycle.HealthVerdict{{Outcome: lifecycle.VerificationPending}}
		}, lifecycle.HoldHealth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRepairWorld(t, repairPatch)
			tc.setup(w)
			result, err := w.run()
			if err != nil || result.Outcome != lifecycle.RepairHeld || result.Hold != tc.hold || result.Detail == "" {
				t.Fatalf("result = %+v, err = %v, want a %s hold", result, err, tc.hold)
			}
			if count(w.events, "accept") != 0 || count(w.events, "discard") != 1 || count(w.events, "apply") > 1 {
				t.Fatalf("events = %q: nothing is accepted, the stage is discarded, one apply (no rollback)", w.events)
			}
		})
	}
}

// A transient infrastructure failure is retried inside the bound; one beyond
// it is an error for the caller to retry, never a hold.
func TestRepairRetriesInfrastructureFailures(t *testing.T) {
	w := newRepairWorld(t, repairPatch)
	w.applyErrs = []error{&infrastructureFailure{}, nil}
	if result, err := w.run(); err != nil || result.Outcome != lifecycle.RepairDeployed || count(w.events, "apply") != 2 {
		t.Fatalf("result = %+v, err = %v, events %q", result, err, w.events)
	}
	w = newRepairWorld(t, repairPatch)
	w.applyErrs = []error{&infrastructureFailure{}, &infrastructureFailure{}}
	result, err := w.run()
	var infrastructure *lifecycle.InfrastructureError
	if !errors.As(err, &infrastructure) || result.Outcome == lifecycle.RepairHeld {
		t.Fatalf("result = %+v, err = %v, want an infrastructure error and no hold", result, err)
	}
}

// The head moving between staging and acceptance restages and deploys the new
// stage; a head that keeps moving is an error.
func TestRepairRestagesWhenTheHeadMoves(t *testing.T) {
	w := newRepairWorld(t, repairPatch)
	w.staleAccepts = 1
	result, err := w.run()
	if err != nil || result.Outcome != lifecycle.RepairDeployed || count(w.events, "stage") != 2 || count(w.events, "apply") != 2 {
		t.Fatalf("result = %+v, err = %v, events %q", result, err, w.events)
	}
	w = newRepairWorld(t, repairPatch)
	w.staleAccepts = 100
	if _, err := w.run(); !errors.Is(err, broker.ErrCandidateStale) {
		t.Fatalf("err = %v, want ErrCandidateStale", err)
	}
}

// With no recorded SDO deployment (a seeded install) nothing owned by SDO can
// redeploy the repair: it is published without a deployment and says so.
func TestRepairWithoutARecordedDeploymentPublishesUndeployed(t *testing.T) {
	for name, setup := range map[string]func(*repairWorld){
		"no evidence": func(w *repairWorld) { w.noEvidence = true },
		"no plan":     func(w *repairWorld) { w.planAbsent = true },
		"plan unset":  func(w *repairWorld) { w.noPlan = true },
	} {
		t.Run(name, func(t *testing.T) {
			w := newRepairWorld(t, repairPatch)
			setup(w)
			result, err := w.run()
			if err != nil || result.Outcome != lifecycle.RepairUndeployed || !strings.Contains(result.Detail, "no deployment plan") || count(w.events, "apply") != 0 {
				t.Fatalf("result = %+v, err = %v, events %q", result, err, w.events)
			}
		})
	}
}

// A broker rejection of the proposal is returned, never swallowed as a hold.
func TestRepairReturnsABrokerRejection(t *testing.T) {
	w := newRepairWorld(t, repairPatch)
	w.stageErr = errors.New("candidate validation: patch.invalid")
	if _, err := w.run(); err == nil || !strings.Contains(err.Error(), "patch.invalid") || count(w.events, "apply") != 0 {
		t.Fatalf("err = %v, events %q", err, w.events)
	}
}

func TestRepairRejectsIncompleteWiring(t *testing.T) {
	w := newRepairWorld(t, repairPatch)
	d := w.deployer()
	d.Verifier = nil
	if _, err := d.Repair(context.Background(), repairConfig, "inc-1"); err == nil {
		t.Fatal("a repair without a verifier ran")
	}
	if _, err := w.deployer().Repair(context.Background(), lifecycle.RepairConfig{}, "inc-1"); err == nil {
		t.Fatal("a repair without a namespace ran")
	}
}
