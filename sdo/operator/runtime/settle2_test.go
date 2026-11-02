package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

// Whole-incident observation is never negative application evidence.
func TestSettlementUnconfirmedApplicationIsNeutral(t *testing.T) {
	for _, classification := range []string{"unconfirmed", "failed", "cleared_without_action", "success"} {
		outcome := &contracts.OutcomeRecord{Classification: classification, SourceCommit: "synthetic", PlanApplications: []*contracts.PlanApplication{{ApplicationId: "a", Applied: true, ProgramIds: []string{"p"}, VerificationCompleted: true, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}}}
		if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})["p"]; got.Failures != 0 || got.VerifiedSuccesses != 0 {
			t.Fatalf("%s: observation became evidence: %+v", classification, got)
		}
	}
}

// Logical health recovery depends on elapsed time, never on how often the
// stage samples it. Sampling cadence may change without changing the fault.
type settle3TimedHealth struct {
	now   func() time.Time
	ready time.Time
}

func (settle3TimedHealth) Spec() sdk.DetectorSpec { return (&pipelineLaggingHealthDetector{}).Spec() }

func (d settle3TimedHealth) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.now().Before(d.ready) {
		return nil, nil
	}
	return (&pipelineLaggingHealthDetector{remaining: -1}).Detect(ctx, view)
}

type settle2ControllerDispatcher struct {
	pipelineScenarioDispatcher
	controller *Controller
	stage      *DeterministicResponseStage
}

func (d *settle2ControllerDispatcher) DeterministicResponseStage() ResponseStage {
	d.stage.NewLoop = func(request IncidentRequest) *ResponderLoop {
		d.loop.Request = request
		d.controller.mu.Lock()
		d.controller.responderLoop = d.loop
		d.controller.mu.Unlock()
		return d.loop
	}
	return d.stage
}

type settle2DelayedRollout struct {
	base  PlanRollouts
	now   func() time.Time
	ready time.Time
}

func (r *settle2DelayedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Observe(ctx, refs)
}

func (r *settle2DelayedRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.now().Before(r.ready) {
		return context.DeadlineExceeded
	}
	return r.base.Wait(ctx, refs)
}

type settle2UnknownDiagnoser struct{ spec program.ProgramSpec }

func (p settle2UnknownDiagnoser) Spec() program.ProgramSpec { return p.spec }

func (p settle2UnknownDiagnoser) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Unknown}, nil
}

type settle2BlockedRollout struct{}

func (settle2BlockedRollout) Wait(context.Context, []sdk.ObjectRef) error {
	return &rolloutBlockedError{cause: errRolloutStalled}
}

func (r settle2BlockedRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Wait(ctx, refs)
}

func TestSettlementNonpendingHealthObservationIsNeutral(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, false)
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || loop.PlanApplications()[0].Result.VerificationPassed {
		t.Fatal("fixture must produce a landed unconfirmed repair")
	}
	runs := loop.ProgramRuns()
	fillProgramRevisions(context.Background(), runs, loop.Request.RepositoryWorktree, loop.Request.SourceCommit)
	typed, err := incidentResultContract(IncidentResult{PlanApplications: loop.ApplicationEvidence(), ProgramRuns: runs, ProgramFailureSignals: loop.FailureSignals()})
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{Classification: "unconfirmed", SourceCommit: loop.Request.SourceCommit, PlanApplications: typed.PlanApplications, ProgramRuns: typed.ProgramRuns, ProgramFailureSignals: typed.ProgramFailureSignals}
	if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})["service-targetport-mismatch"]; got.Failures != 0 || got.VerifiedSuccesses != 0 {
		t.Fatalf("health observation became adverse execution evidence: %+v signals=%v", got, loop.FailureSignals())
	}
}
