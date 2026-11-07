package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
)

// deployerAgent is the deployment agent of an advance: it records each request
// and answers with the next plan (a clone of the accepted plan with another
// expected workload, so each answer is distinguishable).
type deployerAgent struct {
	requests []lifecycle.DeploymentCorrection
	err      error
}

func (d *deployerAgent) CorrectDeployment(_ context.Context, request lifecycle.DeploymentCorrection) (*contracts.DeploymentPlan, error) {
	d.requests = append(d.requests, request)
	if d.err != nil {
		return nil, d.err
	}
	plan := proto.Clone(request.Plan).(*contracts.DeploymentPlan)
	plan.ExpectedWorkloadIds = append(plan.ExpectedWorkloadIds, "Deployment/toy/fix"+string(rune('0'+len(d.requests))))
	return plan, nil
}

func (w *advWorld) advancerWith(agent lifecycle.DeploymentCorrector) lifecycle.Advancer {
	a := w.advancer()
	a.Deployer = agent
	return a
}

func (w *advWorld) runWith(t *testing.T, agent lifecycle.DeploymentCorrector) (lifecycle.AdvanceResult, error) {
	t.Helper()
	return w.advancerWith(agent).Advance(context.Background(), advConfig, advRequest(t))
}

// E2: a render that breaks is the deployment agent's. The hold names owner
// deployer, so the agent runs with the hold as its request on the advance
// checkout; its corrected plan and checkout are checked again offline and the
// advance then deploys and is accepted.
func TestDeployerOwnedHoldStartsADeployerTaskAndTheCorrectionIsAccepted(t *testing.T) {
	w := newAdvWorld(t)
	w.offlineSeq = [][]operability.Check{offlineFailure("O1.render", operability.CauseRender), nil}
	agent := &deployerAgent{}
	result, err := w.runWith(t, agent)
	if err != nil || result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("result = %+v, err = %v; want accepted after one correction", result, err)
	}
	if len(agent.requests) != 1 {
		t.Fatalf("deployer tasks = %d, want 1", len(agent.requests))
	}
	request := agent.requests[0]
	if request.Proposal.Path != advCheckout || request.Attempt != 1 || request.AdvanceID == "" {
		t.Errorf("request = %+v", request)
	}
	for _, want := range []string{"cause=render owner=deployer items=O1", "synthetic render", "deploy/values.yaml"} {
		if !strings.Contains(request.Feedback, want) {
			t.Errorf("feedback %q lacks %q: the hold is the request", request.Feedback, want)
		}
	}
	// The accepted evidence is of the corrected plan; nothing touched the cluster before the correction.
	if len(w.corrected) != 1 || !slices.Equal(w.plan.GetExpectedWorkloadIds(), w.corrected[0].GetExpectedWorkloadIds()) {
		t.Fatalf("corrected plans = %v", w.corrected)
	}
	if first := slices.Index(w.events, "correct advance-"+advRevision[:12]); first < 0 || slices.Index(w.events, "pause") < first || slices.Index(w.events, "apply") < first {
		t.Fatalf("a cluster effect preceded the correction: %q", w.events)
	}
}

// A patch that does not apply is the same: the broker keeps the checkout for
// the agent, which corrects the patches.
func TestPatchHoldStartsADeployerTaskOnTheKeptCheckout(t *testing.T) {
	w := newAdvWorld(t)
	w.keepOnPatchHold = true
	conflict := &patchset.PatchError{Patch: "0001-fix.patch", Kind: patchset.KindConflict, Detail: "context no longer matches", Paths: []string{"lib/vendor.txt"}}
	w.proposeErr = &broker.PatchHoldError{Candidate: advCandidate, Patch: conflict, Owner: "deployer"}
	agent := &deployerAgent{}
	result, err := w.runWith(t, agent)
	if err != nil || result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if len(agent.requests) != 1 || !strings.Contains(agent.requests[0].Feedback, "0001-fix.patch") || !strings.Contains(agent.requests[0].Feedback, "cause=patch owner=deployer") {
		t.Fatalf("requests = %+v", agent.requests)
	}
}

// The agent is bounded like the lifecycle's other rounds: the same hold twice
// in a row is agent_not_converging (no third task), and so is the budget.
func TestDeployerCorrectionIsBounded(t *testing.T) {
	t.Run("repeated hold", func(t *testing.T) {
		w := newAdvWorld(t)
		w.offline = offlineFailure("O1.render", operability.CauseRender)
		agent := &deployerAgent{}
		result, err := w.runWith(t, agent)
		if err != nil || result.Outcome != lifecycle.AdvanceHeld || result.Hold != lifecycle.HoldDeployer {
			t.Fatalf("result = %+v, err = %v; want the deployer_not_converging hold", result, err)
		}
		if len(agent.requests) != 1 || result.Cause != operability.CauseAgentNotConverging || result.Owner != operability.OwnerHuman {
			t.Fatalf("tasks = %d cause = %s owner = %s", len(agent.requests), result.Cause, result.Owner)
		}
		if count(w.events, "apply") != 0 || count(w.events, "pause") != 0 {
			t.Fatalf("a held advance touched the cluster: %q", w.events)
		}
	})
	t.Run("budget", func(t *testing.T) {
		w := newAdvWorld(t)
		// Each correction changes the failure (never a repeat), and never fixes it.
		var seq [][]operability.Check
		for _, id := range []string{"O1.render", "O1.deterministic", "O2.images", "O1.workloads"} {
			checks := offlineFailure(id, operability.CauseRender)
			checks[1].Detail = "synthetic " + id
			seq = append(seq, checks)
		}
		w.offlineSeq = seq
		agent := &deployerAgent{}
		a := w.advancerWith(agent)
		a.MaxCorrections = 3
		result, err := a.Advance(context.Background(), advConfig, advRequest(t))
		if err != nil || result.Hold != lifecycle.HoldDeployer {
			t.Fatalf("result = %+v, err = %v", result, err)
		}
		if len(agent.requests) != 3 {
			t.Fatalf("deployer tasks = %d, want the budget of 3", len(agent.requests))
		}
		if count(w.events, "propose") != count(w.events, "release") {
			t.Fatalf("proposal leaked: %q", w.events)
		}
	})
	t.Run("rejected correction feeds the next request", func(t *testing.T) {
		w := newAdvWorld(t)
		w.offlineSeq = [][]operability.Check{offlineFailure("O1.render", operability.CauseRender), nil}
		w.correct = func(call int, _ *contracts.DeploymentPlan) (string, error) {
			if call == 1 {
				return "ownership.denied@app/main.go: app files are never edited", nil
			}
			return "", nil
		}
		agent := &deployerAgent{}
		result, err := w.runWith(t, agent)
		if err != nil || result.Outcome != lifecycle.AdvanceAccepted || len(agent.requests) != 2 {
			t.Fatalf("result = %+v, err = %v, tasks = %d", result, err, len(agent.requests))
		}
		if !strings.Contains(agent.requests[1].Feedback, "app files are never edited") || agent.requests[1].Attempt != 2 {
			t.Errorf("second request = %+v", agent.requests[1])
		}
	})
	t.Run("agent failure", func(t *testing.T) {
		w := newAdvWorld(t)
		w.offline = offlineFailure("O1.render", operability.CauseRender)
		agent := &deployerAgent{err: errors.New("worker job_failed")}
		result, err := w.runWith(t, agent)
		if err != nil || result.Outcome != lifecycle.AdvanceHeld {
			t.Fatalf("result = %+v, err = %v", result, err)
		}
		if len(agent.requests) < 1 || len(agent.requests) > lifecycle.DefaultMaxCorrections+1 {
			t.Fatalf("deployer tasks = %d", len(agent.requests))
		}
	})
}

// A deployer-owned hold the apply discovers (the API server refuses an
// immutable change; nothing of the phase was applied) goes through the same
// dispatcher as an offline one: one deployer task with the hold as its
// request, then the corrected candidate is screened and deployed again, with
// the same bounds.
func TestApplyPhaseDeployerHoldStartsADeployerTask(t *testing.T) {
	immutable := func() error {
		return &deployment.ImmutableChangeError{Phase: "main", Refused: []deployment.RefusedObject{{Reason: deployment.RefuseNoPolicy}}}
	}
	t.Run("corrected", func(t *testing.T) {
		w := newAdvWorld(t)
		w.applyErrs = []error{immutable(), nil}
		agent := &deployerAgent{}
		result, err := w.runWith(t, agent)
		if err != nil || result.Outcome != lifecycle.AdvanceAccepted {
			t.Fatalf("result = %+v, err = %v; want accepted after one correction", result, err)
		}
		if len(agent.requests) != 1 || !strings.Contains(agent.requests[0].Feedback, "cause=immutable_change owner=deployer") {
			t.Fatalf("deployer requests = %+v", agent.requests)
		}
		if count(w.events, "apply") != 2 || count(w.events, "pause") != count(w.events, "resume") {
			t.Fatalf("events %q", w.events)
		}
	})
	t.Run("same hold twice stops", func(t *testing.T) {
		w := newAdvWorld(t)
		w.applyErrs = []error{immutable(), immutable(), immutable(), immutable()}
		agent := &deployerAgent{}
		result, err := w.runWith(t, agent)
		if err != nil || result.Hold != lifecycle.HoldDeployer || result.Cause != operability.CauseAgentNotConverging {
			t.Fatalf("result = %+v, err = %v", result, err)
		}
		if len(agent.requests) > lifecycle.DefaultMaxCorrections {
			t.Fatalf("deployer tasks = %d, over the budget", len(agent.requests))
		}
	})
	t.Run("without an agent the hold is named for its cause", func(t *testing.T) {
		w := newAdvWorld(t)
		w.applyErrs = []error{immutable()}
		result, err := w.run(t)
		if err != nil || result.Hold != lifecycle.HoldImmutable || result.Owner != operability.OwnerDeployer {
			t.Fatalf("result = %+v, err = %v", result, err)
		}
	})
	t.Run("a data-bearing refusal is the human's", func(t *testing.T) {
		w := newAdvWorld(t)
		w.applyErrs = []error{&deployment.ImmutableChangeError{Phase: "main", Refused: []deployment.RefusedObject{{Reason: deployment.RefuseDataBearing}}}}
		agent := &deployerAgent{}
		result, err := w.runWith(t, agent)
		if err != nil || result.Hold != lifecycle.HoldImmutable || result.Owner != operability.OwnerHuman || len(agent.requests) != 0 {
			t.Fatalf("result = %+v, err = %v, tasks %d", result, err, len(agent.requests))
		}
	})
}

// Only the deployer's holds start the deployment agent. A hold owned by
// anyone else, and a deployer-owned one with no agent configured, is the hold
// it always was.
func TestOnlyDeployerOwnedHoldsStartAnAgent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*advWorld)
	}{
		{"scope is the human's", func(w *advWorld) { w.offline = offlineFailure("O6.scope", operability.CauseScope) }},
		{"an SDO defect is the operator's", func(w *advWorld) { w.offline = offlineFailure("O2.images", operability.CauseAttribution) }},
		{"a shape change is the designer's", func(w *advWorld) { w.shapes[advCandidate] = "shape-b" }},
		{"a conflict with the accepted source", func(w *advWorld) { w.conflicts = []string{"app.txt"} }},
		{"a rewritten ref is the human's", func(w *advWorld) { w.notAncestor = true }},
		{"upstream touching memory is the human's", func(w *advWorld) { w.changed = []string{".sdo/goal/goal.md"} }},
		{"a rollout failure is the responder's", func(w *advWorld) {
			w.applyErrs = []error{errors.New("rollout stuck")}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newAdvWorld(t)
			tc.mutate(w)
			agent := &deployerAgent{}
			result, err := w.runWith(t, agent)
			if err != nil || result.Outcome != lifecycle.AdvanceHeld {
				t.Fatalf("result = %+v, err = %v", result, err)
			}
			if len(agent.requests) != 0 || len(w.corrected) != 0 {
				t.Fatalf("a %s hold started the deployment agent", result.Owner)
			}
		})
	}
	t.Run("no agent configured", func(t *testing.T) {
		w := newAdvWorld(t)
		w.offline = offlineFailure("O1.render", operability.CauseRender)
		result, err := w.run(t)
		if err != nil || result.Hold != lifecycle.HoldRender || result.Owner != operability.OwnerDeployer || count(w.events, "correct") != 0 {
			t.Fatalf("result = %+v, err = %v, events %q", result, err, w.events)
		}
	})
}

// Every owner a hold can name has a disposition, and the deployer's is
// dispatched here: this table is the one place that says who acts on a hold,
// so a new owner or cause cannot be added without saying who runs for it.
func TestEveryAdvanceHoldOwnerHasADisposition(t *testing.T) {
	disposition := map[operability.Owner]string{
		operability.OwnerDeployer:  "dispatched: Advancer.Deployer (DeploymentCorrector), bounded by MaxCorrections and the repeated-hold stop",
		operability.OwnerResponder: "the cluster is left as is; ordinary detection opens the incident and the responder runs",
		operability.OwnerDesigner:  "designer_refresh: an owed obligation the host retries; other designer holds wait for a newer revision",
		operability.OwnerHuman:     "terminal: a person acts",
		operability.OwnerOperator:  "terminal: SDO's own defect, no agent",
		operability.OwnerRetry:     "infrastructure: the follower retries the revision",
	}
	for _, cause := range operability.Causes() {
		owner := operability.RouteIn(operability.ModeAdvance, cause)
		if _, ok := disposition[owner]; !ok {
			t.Errorf("cause %s routes to owner %q in an advance, which has no disposition", cause, owner)
		}
	}
}

// The disposition holds wherever the hold arises: for every cause an apply
// can fail with, a deployer-owned one dispatches the deployment agent and
// nobody else's does, and a deployer-owned one is never an unnamed operability hold.
func TestEveryApplyPhaseCauseHasTheDispositionOfItsOwner(t *testing.T) {
	for _, cause := range operability.Causes() {
		owner := operability.RouteIn(operability.ModeAdvance, cause)
		t.Run(string(cause), func(t *testing.T) {
			w := newAdvWorld(t)
			for range 10 {
				w.applyErrs = append(w.applyErrs, causeFailure(cause))
			}
			agent := &deployerAgent{}
			result, err := w.runWith(t, agent)
			if cause == operability.CauseInfrastructure {
				return // retried, not held; covered by the infrastructure tests
			}
			if err != nil || result.Outcome != lifecycle.AdvanceHeld {
				t.Fatalf("result = %+v, err = %v", result, err)
			}
			dispatched := len(agent.requests) > 0
			if dispatched != (owner == operability.OwnerDeployer) {
				t.Fatalf("owner %s: dispatched = %v", owner, dispatched)
			}
			if result.Hold == lifecycle.HoldOperability && owner == operability.OwnerDeployer {
				t.Fatalf("cause %s (owner %s) is held as an unnamed operability hold", cause, owner)
			}
		})
	}
}
