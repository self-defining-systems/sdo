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
	"sdo.dev/operator/sdk/program"
)

type planningTestProgram struct {
	restartProgram
	mitigate func() (program.Plan, error)
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
		{name: "unavailable", err: errors.New("live read unavailable"), calls: 3},
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

func TestPlanRefusalEndsRecoveryWaitAfterEarlierWrite(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	bound := report.GetRecoveryDeadline().AsTime()
	report.Mitigations = append(report.Mitigations, &contracts.StageMitigation{
		PlaybookId: "second-playbook", ProgramRevision: "second-revision", Result: "apply_error",
		SkipReason: stagePlanNotExecutable, Error: "validate plan: invalid restart selector",
	})
	autoStep(t, c, world.clock, time.Second)
	if !world.clock.Now().Before(bound) || !autoReport(c).GetHandoffSelected() {
		t.Fatalf("refusal did not select handoff before %s: %v", bound, autoReport(c))
	}
	if !strings.Contains(autoReport(c).GetMitigations()[1].GetError(), "invalid restart selector") {
		t.Fatal("handoff lost the refusing check")
	}
}
