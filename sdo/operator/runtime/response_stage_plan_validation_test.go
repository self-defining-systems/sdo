package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/program"
)

func TestStageRequiresValidationOfTheExecutingRevision(t *testing.T) {
	for _, name := range []string{"unverified accepted", "verified accepted", "working edit of verified"} {
		t.Run(name, func(t *testing.T) {
			testStageCapturedPlan(t, name)
		})
	}
}

func testStageCapturedPlan(t *testing.T, name string) {
	t.Helper()
	_, stage, state := keyedStageLoop(t, nil)
	loop := stage.NewLoop(state.Request)
	playbook := rolelessRestart{id: "captured-repair", target: "api"}
	registry, err := program.NewRegistry([]program.Diagnoser{playbook}, []program.Mitigator{playbook})
	if err != nil {
		t.Fatal(err)
	}
	verified := name != "unverified accepted"
	accepted := withSourceRestore(&remoteProgramRegistry{ProgramRegistry: registry, verifiedPlans: map[string]bool{playbook.id: verified}}, loop.restoreEvidence)
	loop.Programs = accepted
	if name == "working edit of verified" {
		loop.Programs = &workingProgramRegistry{accepted: accepted, overrides: map[string]*workingProgram{
			playbook.id: {spec: playbook.Spec(), canMitigate: true},
		}}
		// Start with the accepted Match; the stage must not run the working override.
		view := stagePlanTestView(t, loop)
		runs := SelectProgramCandidates(context.Background(), accepted, view, state.Findings, DefaultDiagnosisDeadline)
		state.Request.InitialProgramRuns = runs
		state.Coverage = buildPlaybookCoverage(state.Findings, runs, view, nil)
	}
	for i := range state.Findings {
		state.Findings[i].Playbooks = []string{playbook.id}
	}
	state.Request.Findings = state.Findings
	loop.Request = state.Request
	loop.findings = state.Findings

	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range report.GetMitigations() {
		if attempt.GetPlaybookId() == playbook.id {
			if name == "verified accepted" {
				if attempt.GetResult() != "applied_cleared" || len(loop.ApplicationEvidence()) == 0 {
					t.Fatalf("verified playbook did not apply: %v", attempt)
				}
				return
			}
			if attempt.GetResult() != "skipped" || attempt.GetSkipReason() != "plan_unverified" || len(loop.ApplicationEvidence()) != 0 {
				t.Fatalf("unverified playbook attempted a repair: %v", attempt)
			}
			if _, ok := loop.Programs.Mitigator(playbook.id); !ok {
				t.Fatal("unverified playbook is unavailable to the responder")
			}
			return
		}
	}
	t.Fatalf("missing skip for matched playbook: %v", report)
}

func stagePlanTestView(t *testing.T, loop *ResponderLoop) program.ReadView {
	t.Helper()
	view, err := loop.snapshotView(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return view
}
