package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func splitFindingsByPlaybook(t *testing.T, findings []sdk.Finding) (first, second sdk.Finding) {
	t.Helper()
	if len(findings) != 2 {
		t.Fatalf("fixture has %d findings", len(findings))
	}
	return findings[0], findings[1]
}

// The fixture's two faults stand for a fault the opening stage could see and
// one a learned detector reports later. A later pass applies only what is new:
// the opening attempt keeps its place in the one report and is never repeated.
func TestLaterStagePassAppliesOnlyTheNewMatch(t *testing.T) {
	ctx := context.Background()
	loop, state := pipelineTestLoop(t, false)
	early, late := splitFindingsByPlaybook(t, state.Findings)
	both := append([]sdk.Finding(nil), state.Findings...)
	state.Findings = []sdk.Finding{early}
	state.Request.Findings = []sdk.Finding{early}
	state.Request.PlaybookCoverage, state.Request.InitialProgramRuns, state.Coverage = nil, nil, nil
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	report, err := stage.Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	state.PriorStages = []*contracts.StageReport{report}
	// The other fault's playbook matches the live cluster but its finding has
	// not activated, so the opening pass records it as skipped, unbound.
	opening := map[string]string{}
	for _, attempt := range report.Mitigations {
		opening[attempt.PlaybookId] = attempt.Result
	}
	if len(report.Mitigations) != 2 || opening["service-targetport-mismatch"] != "applied_cleared" || opening["image-pull-failure"] != "skipped" || len(loop.PlanApplications()) != 1 {
		t.Fatalf("opening pass: %v", report)
	}
	firstApplication := loop.PlanApplications()[0].ID

	// The later finding activates; the same request now carries both.
	state.Request.Findings = both
	state.Findings = append([]sdk.Finding(nil), late)
	again, err := RunStageAgain(ctx, state, stage)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Mitigations) != 2 || len(loop.PlanApplications()) != 2 {
		t.Fatalf("later pass: want 2 attempts and 2 applications, got %d and %d: %v", len(again.Mitigations), len(loop.PlanApplications()), again)
	}
	for _, attempt := range again.Mitigations {
		if attempt.Result != "applied_cleared" {
			t.Fatalf("attempt not applied: %v", attempt)
		}
		if attempt.PlaybookId == "service-targetport-mismatch" && attempt.GetApplyReceipt().GetApplicationId() != firstApplication {
			t.Fatalf("opening attempt not kept as it was: %v", attempt)
		}
	}
	if len(state.PriorStages) != 1 || state.PriorStages[0] != again {
		t.Fatalf("later pass must update the one stage report, got %d reports", len(state.PriorStages))
	}
	if len(state.Findings) != 0 {
		t.Fatalf("findings remain: %v", state.Findings)
	}

	// A third pass with nothing new writes nothing: both bindings were attempted.
	third, err := RunStageAgain(ctx, state, stage)
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Mitigations) != 2 || len(loop.PlanApplications()) != 2 {
		t.Fatalf("an attempted binding was applied again: %v", third)
	}
}
