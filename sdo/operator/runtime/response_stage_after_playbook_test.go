package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// An alert that was not among the incident's opening findings, and that the
// stage was never given, never holds the stage's recovery wait or follows the
// applied playbook to the agent: Recovered is judged on the opening alerts.
func TestAlertActivatingDuringTheWaitNeverFollowsAnAppliedPlaybook(t *testing.T) {
	// The restart fixes the traffic at 1 s; an unrelated component's alert
	// activates at 5 s and stays up past the recovery bound.
	stage, state, clock, _ := settlingStage(t, settlingCase{healAfter: 20 * time.Second, traffic: &settlingTraffic{}, churnFrom: 5 * time.Second, churnUntil: 10 * time.Minute})
	agent := &recordingStage{}
	started := clock.Now()
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	report := state.PriorStages[0]
	if !playbookWrote(report) {
		t.Fatalf("the stage must apply the restart: %v", report)
	}
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatalf("exit = %s after %s, want CLEARED", got, clock.Now().Sub(started))
	}
	if agent.dispatched {
		t.Fatalf("the agent was dispatched for an alert that did not open the incident: %v", report.RemainingFindingFingerprints)
	}
}

func withPolicy(state *IncidentState, policy string) *IncidentState {
	state.AfterPlaybook = policy
	return state
}

// response.after_playbook decides, once a playbook applied and Recovered did not
// hold within the bound, whether the agent follows.
func TestAfterPlaybookPolicyDecidesWhetherTheAgentFollows(t *testing.T) {
	for _, tc := range []struct {
		policy         string
		wantDispatched bool
		wantExit       contracts.RecoveryWait_ExitReason
	}{
		{"", true, contracts.RecoveryWait_EXIT_REASON_FAILED},
		{settings.AfterPlaybookAgent, true, contracts.RecoveryWait_EXIT_REASON_FAILED},
		{settings.AfterPlaybookEscalate, false, contracts.RecoveryWait_EXIT_REASON_FAILED},
		{settings.AfterPlaybookWait, false, contracts.RecoveryWait_EXIT_REASON_DEADLINE},
	} {
		t.Run("policy "+tc.policy, func(t *testing.T) {
			// The fault outlives every window and the whole bound.
			stage, state, clock, _ := settlingStage(t, settlingCase{stallWindow: 60 * time.Second})
			if tc.policy == settings.AfterPlaybookWait {
				stage.WaitBound = 5 * time.Minute
			}
			agent := &recordingStage{}
			if err := RunResponseStages(context.Background(), withPolicy(state, tc.policy), []ResponseStage{stage, agent}); err != nil {
				t.Fatal(err)
			}
			report := state.PriorStages[0]
			if got := report.GetRecoveryWait().GetExitReason(); got != tc.wantExit {
				t.Fatalf("exit = %s after %s, want %s", got, clock.Now(), tc.wantExit)
			}
			if agent.dispatched != tc.wantDispatched {
				t.Fatalf("agent dispatched = %v, want %v", agent.dispatched, tc.wantDispatched)
			}
			if len(state.Findings) == 0 {
				t.Fatal("the unrecovered opening findings must stay with the incident")
			}
		})
	}
}

// wait keeps waiting for Recovered past the stage's own give-ups, up to its
// bound; the same slow recovery is handed to the agent otherwise.
func TestAfterPlaybookWaitKeepsWaitingForASlowRecovery(t *testing.T) {
	slow := settlingCase{healAfter: 150 * time.Second, stallWindow: 60 * time.Second}
	stage, state, clock, _ := settlingStage(t, slow)
	stage.WaitBound = 5 * time.Minute
	agent := &recordingStage{}
	started := clock.Now()
	if err := RunResponseStages(context.Background(), withPolicy(state, settings.AfterPlaybookWait), []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	report := state.PriorStages[0]
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED || agent.dispatched {
		t.Fatalf("exit = %s dispatched = %v after %s, want CLEARED without the agent: %v", got, agent.dispatched, clock.Now().Sub(started), state.Findings)
	}
	if waited := clock.Now().Sub(started); waited < 150*time.Second {
		t.Fatalf("recovered after %s, before the fault healed", waited)
	}

	stage, state, _, _ = settlingStage(t, slow)
	agent = &recordingStage{}
	if err := RunResponseStages(context.Background(), withPolicy(state, settings.AfterPlaybookAgent), []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	if !agent.dispatched {
		t.Fatal("without wait the stage's own bound hands a slow recovery to the agent")
	}
}

// The policy holds the agent back after a playbook applied, even one the stage
// then reverted; with no such playbook the agent runs as it always did.
func TestAfterPlaybookDispatchRequiresAnAppliedPlaybook(t *testing.T) {
	applied := &contracts.StageReport{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{Applied: true}}}}
	rolledBack := &contracts.StageReport{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{Applied: true, RolledBack: true}}}}
	for name, tc := range map[string]struct {
		policy string
		report *contracts.StageReport
		want   bool
	}{
		"no stage report, escalate":  {settings.AfterPlaybookEscalate, nil, true},
		"nothing applied, escalate":  {settings.AfterPlaybookEscalate, &contracts.StageReport{Stage: "deterministic"}, true},
		"rolled back, wait":          {settings.AfterPlaybookWait, rolledBack, false},
		"applied, agent":             {settings.AfterPlaybookAgent, applied, true},
		"applied, unset means agent": {"", applied, true},
		"applied, escalate":          {settings.AfterPlaybookEscalate, applied, false},
		"applied, wait":              {settings.AfterPlaybookWait, applied, false},
	} {
		if got := agentMayFollowPlaybook(tc.policy, []*contracts.StageReport{tc.report}); got != tc.want {
			t.Errorf("%s: agentMayFollowPlaybook = %v, want %v", name, got, tc.want)
		}
	}
}

// An unverified stage-only incident is handed to the agent by findings. After an
// applied playbook only the opening alerts count, and the policy decides; with no
// applied playbook any active health finding counts, as before.
func TestStageHandoffAfterAPlaybookConsidersOnlyOpeningAlerts(t *testing.T) {
	applied := []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{Applied: true}}}}}
	none := []*contracts.StageReport{{Stage: "deterministic"}}
	opening := sdk.Finding{DetectorID: "traffic-probe", RuleID: "r", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "api"}}
	other := sdk.Finding{DetectorID: "workload-health", RuleID: "r", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "web"}}
	scope := newRecoveryScope([]string{FindingStateKey(opening.DetectorID, FindingFingerprint(opening))})
	for name, tc := range map[string]struct {
		policy                        string
		prior                         []*contracts.StageReport
		findings                      []sdk.Finding
		deadline, openingUp, healthUp bool
		want                          bool
	}{
		"other alert only, agent":             {settings.AfterPlaybookAgent, applied, []sdk.Finding{other}, false, false, true, false},
		"other alert only, past the deadline": {settings.AfterPlaybookAgent, applied, []sdk.Finding{other}, true, false, true, false},
		"opening alert, agent":                {settings.AfterPlaybookAgent, applied, []sdk.Finding{opening, other}, false, true, true, true},
		"opening alert, escalate":             {settings.AfterPlaybookEscalate, applied, []sdk.Finding{opening}, false, true, true, false},
		"opening alert, wait":                 {settings.AfterPlaybookWait, applied, []sdk.Finding{opening}, true, true, true, false},
		"no playbook, other alert":            {settings.AfterPlaybookEscalate, none, []sdk.Finding{other}, false, false, true, true},
		"no playbook, health at the deadline": {settings.AfterPlaybookAgent, none, nil, true, false, true, true},
	} {
		if got := stageHandoffWarranted(tc.policy, tc.prior, scope, tc.findings, tc.deadline, tc.openingUp, tc.healthUp); got != tc.want {
			t.Errorf("%s: warranted = %v, want %v", name, got, tc.want)
		}
	}
}

// A clue that was already firing when the incident opened, from a detector the
// stage re-observes, is not an opening alert: it neither holds the stage's
// clearance nor follows the applied playbook to the agent.
func TestStandingClueNeverHoldsTheStageOrFollowsAPlaybook(t *testing.T) {
	stage, state, clock, _ := settlingStage(t, settlingCase{healAfter: 20 * time.Second, traffic: &settlingTraffic{}, churnUntil: 10 * time.Minute})
	if len(state.Request.OpeningKeys) == 0 || len(state.Findings) <= len(state.Request.OpeningKeys) {
		t.Fatalf("the incident must open with a clue beside its opening alerts: %v", state.Request.OpeningKeys)
	}
	agent := &recordingStage{}
	started := clock.Now()
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	if got := state.PriorStages[0].GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED || agent.dispatched {
		t.Fatalf("exit = %s dispatched = %v after %s, want CLEARED without the agent: %v", got, agent.dispatched, clock.Now().Sub(started), state.Findings)
	}
}

// With wait, a controller that restarts mid-wait resumes the wait against the
// bound it saved; the restart neither shortens it to the stage default nor grants a new one.
func TestAfterPlaybookWaitSurvivesControllerRestart(t *testing.T) {
	tc := settlingCase{healAfter: 150 * time.Second, stallWindow: 60 * time.Second}
	world := newSettlingWorld(tc)
	var saved [][]byte
	world.saved = &saved
	stage, state := settlingStageOn(t, tc, world)
	stage.WaitBound = 5 * time.Minute
	started := world.clock.Now()
	ctx, stop := context.WithCancel(context.Background())
	stage.recoveryWait = func(ctx context.Context, d time.Duration) error {
		if err := world.clock.advance(ctx, d); err != nil {
			return err
		}
		if world.clock.Now().Sub(started) >= 100*time.Second {
			stop()
			return context.Canceled
		}
		return nil
	}
	if _, err := stage.Run(ctx, withPolicy(state, settings.AfterPlaybookWait)); err == nil {
		t.Fatal("the interrupted stage must report its cancellation")
	}
	world.saved = nil
	resumed, restored := settlingStageOn(t, tc, world)
	resumed.WaitBound = 5 * time.Minute
	loop := resumed.NewLoop(restored.Request)
	if err := loop.restoreToolSnapshot(saved[len(saved)-1]); err != nil {
		t.Fatal(err)
	}
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), withPolicy(restored, settings.AfterPlaybookWait), []ResponseStage{resumed, agent}); err != nil {
		t.Fatal(err)
	}
	if got := restored.PriorStages[0].GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED || agent.dispatched {
		t.Fatalf("resumed wait exit = %s dispatched = %v, want CLEARED without the agent", got, agent.dispatched)
	}
}
