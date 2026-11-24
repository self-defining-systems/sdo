package runtime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type planningTestProgram struct {
	restartProgram
	mitigate   func() (program.Plan, error)
	deployment string
}

func (p planningTestProgram) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	if p.deployment != "" && p.deployment != bindings.Objects["deployment"].Name {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return p.restartProgram.Diagnose(ctx, view, bindings)
}

func (p planningTestProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return p.mitigate()
}

func remotePlanningTestProgram(t *testing.T, fixture planningTestProgram) *remoteProgram {
	t.Helper()
	host, err := memoryhost.NewServer("accepted", "binary", nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{fixture}, []program.Mitigator{fixture})
	if err != nil {
		t.Fatal(err)
	}
	host.WithPrograms(registry)
	return &remoteProgram{spec: fixture.Spec(), remote: &RemoteMemory{rpc: acceptedReadsHost{host}, memoryCommit: "accepted"}}
}

func TestAutomaticPlanningFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		plan     program.Plan
		err      error
		reason   string
		failures int
		calls    int
	}{
		{name: "refused", plan: program.Plan{Actions: []program.Action{program.RestartRollout{}}}, reason: stagePlanNotExecutable, failures: 1, calls: 1},
		{name: "unavailable", err: errors.New("live read unavailable"), calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c, world, _ := automaticRecoveryWorld(t, settlingCase{testMitigate: func() (program.Plan, error) {
				calls++
				return tc.plan, tc.err
			}}, settings.AfterPlaybookAgent)
			report := autoReport(c)
			attempts := report.GetMitigations()
			if len(attempts) != 1 || attempts[0].GetSkipReason() != tc.reason || attempts[0].GetError() == "" || world.restarts.Load() != 0 || calls != tc.calls {
				t.Fatalf("planning attempts = %v, revisions ran %d times, writes = %d", attempts, calls, world.restarts.Load())
			}
			attempt := attempts[0]
			outcome := &contracts.OutcomeRecord{Classification: outcomeclass.Success, PriorStages: []*contracts.StageReport{report}}
			trust := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})[attempt.GetPlaybookId()]
			if trust.Failures != tc.failures || tc.failures != 0 && trust.LastChange != attempt.GetProgramRevision() {
				t.Errorf("trust for %s@%s = %+v, want %d failures", attempt.GetPlaybookId(), attempt.GetProgramRevision(), trust, tc.failures)
			}
			autoStep(t, c, world.clock, 0)
			report = autoReport(c)
			want := alertKeys(c.currentIncidentRequest)
			if tc.failures == 0 {
				want = findingKeys(c.currentIncidentRequest.Findings)
			}
			if !report.GetHandoffSelected() || !slices.Equal(report.GetHandoffKeys(), want) || calls != tc.calls {
				t.Errorf("handoff = %v, planning calls = %d, want handoff scoped to %v with no further planning", report, calls, want)
			}
			if tc.reason == stagePlanNotExecutable {
				evidence := buildDetectSummary(detectInput{Request: *c.currentIncidentRequest, View: detectApp(), Now: world.clock.Now()})
				if !strings.Contains(evidence.Text, attempt.GetPlaybookId()+"): plan validation refused: RestartRollout requires") {
					t.Fatalf("responder evidence lost the refusing check: %s", evidence.Text)
				}
				state := &IncidentState{Request: *c.currentIncidentRequest, PriorStages: []*contracts.StageReport{report}}
				stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop {
					t.Fatal("resumed automatic pass after final refusal")
					return nil
				}}
				if _, err := RunStageAgain(t.Context(), state, stage); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestPlanRefusalPreservesEarlierWriteRecovery(t *testing.T) {
	for _, policy := range []string{settings.AfterPlaybookAgent, settings.AfterPlaybookWait, settings.AfterPlaybookEscalate} {
		t.Run(policy, func(t *testing.T) {
			for _, scenario := range []struct {
				name      string
				healAfter time.Duration
			}{
				{name: "recovers", healAfter: 15 * time.Second},
				{name: "expires"},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: scenario.healAfter}, policy)
					report := autoReport(c)
					bound := report.GetRecoveryDeadline().AsTime()
					report.Mitigations = append(report.Mitigations, &contracts.StageMitigation{
						PlaybookId: "second-playbook", ProgramRevision: "second-revision", Result: "apply_error",
						SkipReason: stagePlanNotExecutable, Error: "validate plan: invalid restart selector",
					})
					autoStep(t, c, world.clock, time.Second)
					report = autoReport(c)
					if !report.GetRecoveryDeadline().AsTime().Equal(bound) || report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
						t.Fatalf("refusal interrupted recovery before %s: handoff=%v escalated=%s deadline=%v", bound, report.GetHandoffSelected(), c.escalatedAt, report.GetRecoveryDeadline())
					}
					if scenario.healAfter > 0 {
						autoStep(t, c, world.clock, scenario.healAfter-time.Second)
						autoStep(t, c, world.clock, c.config.ClosureStabilityWindow)
						autoStep(t, c, world.clock, time.Second)
						report := deterministicStageReport(closed.Request.PriorStages)
						if closed.Result == nil || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED || !closed.EscalatedAt.IsZero() {
							t.Fatalf("the earlier write did not recover: closed=%t exit=%s escalated=%s", closed.Result != nil, report.GetRecoveryWait().GetExitReason(), closed.EscalatedAt)
						}
						return
					}
					autoStep(t, c, world.clock, bound.Sub(world.clock.Now()))
					wantHandoff := policy == settings.AfterPlaybookAgent
					if report.GetHandoffSelected() != wantHandoff || !wantHandoff && !c.escalatedAt.Equal(bound) {
						t.Fatalf("at deadline: handoff=%v escalated=%s, policy=%s", report.GetHandoffSelected(), c.escalatedAt, policy)
					}
				})
			}
		})
	}
}

func TestAutomaticStagePlansOnlyCandidatesWithTargets(t *testing.T) {
	stage, state, _, world := settlingStage(t, settlingCase{churnFrom: time.Minute, churnUntil: 2 * time.Minute})
	loop := stage.NewLoop(state.Request)
	repair := restartProgram{id: "api-request-stall"}
	calls := 0
	clueOnly := remotePlanningTestProgram(t, planningTestProgram{
		restartProgram: restartProgram{id: "a-clue-playbook"},
		deployment:     "web",
		mitigate: func() (program.Plan, error) {
			calls++
			return program.Plan{Actions: []program.Action{program.RestartRollout{}}}, nil
		},
	})
	registry, err := program.NewRegistry([]program.Diagnoser{clueOnly, repair}, []program.Mitigator{clueOnly, repair})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry

	// The live diagnosis also sees a clue outside this stage's request. Its
	// binding matches, but it has no target among the requested findings.
	clue := state.Findings[0]
	clue.DetectorID, clue.RuleID = "extra-clue", "extra-clue"
	clue.PrimaryResource = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	clue.Playbooks = []string{clueOnly.Spec().ID}
	state.Findings = slices.DeleteFunc(state.Findings, func(f sdk.Finding) bool {
		return f.PrimaryResource.Name == "web"
	})
	state.Request.Findings = state.Findings
	loop.Request = state.Request
	loop.findings = append([]sdk.Finding{clue}, state.Findings...)

	report, err := stage.Run(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(report.GetDiagnoses(), func(run *contracts.PlaybookVerdict) bool {
		return run.GetPlaybookId() == clueOnly.Spec().ID && run.GetResult() == "match"
	}) {
		t.Fatal("fixture did not diagnose the clue-only playbook")
	}
	if world.restarts.Load() != 1 || stagePlanRefused(report) || calls != 0 {
		t.Fatalf("writes=%d, clue-only plans=%d, mitigations=%v", world.restarts.Load(), calls, report.GetMitigations())
	}
}
