package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Independent recovery must not depend on whether an old write can earn credit.
func TestRecoveredClaimSkipsWorkerDespiteUnattributedWrite(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "clear", true: "partial"}[pending], func(t *testing.T) {
			finding := sdk.Finding{DetectorID: "health", RuleID: "broken", Status: sdk.FindingActive}
			sample := sdk.Finding{}
			if pending {
				sample = finding
			}
			detector := controllerDetector("health", time.Second, sample)
			request := IncidentRequest{IncidentID: "synthetic", Findings: []sdk.Finding{finding}, PriorStages: []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{Result: "rollout_failed", ApplyReceipt: &contracts.PlanApplication{Applied: true}}}}}}
			loop := NewResponderLoop(request, nil, staticProvider{}, []sdk.Detector{detector}, PlanExecutor{}, 2)
			loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
			loop.stageProgress = map[string]responseStageProgress{}
			state, err := (KubernetesJobDispatcher{}).beforeFirstResponderClaim(context.Background(), request, &contracts.AgentTask{}, loop)
			if err != nil {
				t.Fatal(err)
			}
			cleared := deterministicStageReport(state.PriorStages).GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED
			if cleared == pending {
				t.Fatalf("pending=%v cleared=%v", pending, cleared)
			}
		})
	}
}
