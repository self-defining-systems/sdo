package runtime

import (
	"slices"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// The first-try multi-fault shape: the stage's only write is SDO's built-in
// restore of api, and three store causes sit behind it. No learned plan was
// bound, so nothing learned is worth waiting for: the agent is handed the
// incident as the apply returns, with no recovery wait. Its first prompt
// states what the stage wrote. The stage's later recovery decision never
// starts it again.
func TestABuiltInOnlyStageHandsOffAsItsApplyReturns(t *testing.T) {
	clues := []sdk.Finding{storeStateClue("StatefulSet", "orders"), storeStateClue("StatefulSet", "users"), storeStateClue("StatefulSet", "cache")}
	c, world, _ := automaticRecoveryWorld(t, settlingCase{builtinOnly: true, clues: clues}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	if !playbookWrote(report) || learnedPlanBound(report) {
		t.Fatalf("the stage must have written only the built-in restore: %v", report)
	}
	completed := report.GetApplyCompletedAt().AsTime()
	if !report.GetHandoffSelected() || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_BUILTIN_ONLY ||
		c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING || c.responseEntry(*c.currentIncidentRequest) != dispatchStagedHandoff {
		t.Fatalf("no handoff at apply return: %v, dispatch %s", report, c.dispatchState)
	}
	if waited := report.GetRecoveryWait().GetDuration().AsDuration(); waited > time.Second || report.GetRecoveryDeadline().AsTime().Sub(completed) < time.Minute {
		t.Fatalf("handed off %s after the apply, recovery deadline %s after it: want at once, well before the deadline",
			waited, report.GetRecoveryDeadline().AsTime().Sub(completed))
	}
	agent := stageDispatchRequest(*c.currentIncidentRequest)
	if len(agent.RecentChanges) == 0 || !strings.HasPrefix(agent.RecentChanges[0], "stage: by sdo (playbook "+SourceRestoreID+") on Deployment/api at ") {
		t.Fatalf("the first prompt does not state the stage's write: %q", agent.RecentChanges)
	}

	// The agent starts; the stage's recovery deadline passes while it works.
	c.mu.Lock()
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.mu.Unlock()
	for range 3 * 60 {
		autoStep(t, c, world.clock, time.Second)
	}
	if world.clock.Now().Before(report.GetRecoveryDeadline().AsTime()) {
		t.Fatal("the test did not pass the stage's recovery deadline")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_RUNNING || c.responderDone || !c.escalatedAt.IsZero() ||
		autoReport(c).GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_BUILTIN_ONLY || world.restarts.Load() != 1 {
		t.Fatalf("the agent was started again or the stage acted after handoff: dispatch %s, done %t, writes %d, report %v",
			c.dispatchState, c.responderDone, world.restarts.Load(), autoReport(c))
	}
}

// The repeat shape: no learned detector fired, only the judge's health
// detector, and a learned playbook tried on its finding matched and applied.
// A bound learned plan keeps the serial path: the agent never starts and the
// stage's recovery closes the incident at no agent cost.
func TestABoundLearnedPlanWithNoLearnedDetectorClosesWithoutTheAgent(t *testing.T) {
	c, world, closed := automaticRecoveryWorld(t, settlingCase{unnamed: true, healAfter: 20 * time.Second}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	if slices.ContainsFunc(c.currentIncidentRequest.Findings, func(f sdk.Finding) bool { return f.DetectorID == "api-request-stall" }) {
		t.Fatal("a learned detector fired")
	}
	if !playbookWrote(report) || !learnedPlanBound(report) {
		t.Fatalf("the learned playbook did not apply: %v", report)
	}
	for closed.Result == nil && world.clock.Now().Before(report.GetRecoveryDeadline().AsTime()) {
		if autoReport(c).GetHandoffSelected() {
			t.Fatalf("the agent was handed a repeat a learned plan repairs: %v", autoReport(c))
		}
		autoStep(t, c, world.clock, time.Second)
	}
	final := deterministicStageReport(closed.Request.PriorStages)
	if closed.Result == nil || final.GetHandoffSelected() || final.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED || world.restarts.Load() != 1 {
		t.Fatalf("the stage's recovery did not close the incident without the agent: %+v", closed)
	}
}

// The repeat-with-a-new-fault shape: a learned plan was bound (serial path)
// and a store cause it did not cover is active. The serial path keeps its
// early exit: the agent starts once the writes settle and the user test fails
// again, with the cause among its findings.
func TestABoundLearnedPlanStillHandsOffACauseItDidNotCover(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.gitRead = buyMap("web, api, store", "{from: web, to: api}, {from: web, to: store}")
	if report := autoReport(c); !learnedPlanBound(report) || report.GetHandoffSelected() {
		t.Fatalf("a bound learned plan must keep the serial path: %v", report)
	}
	clue := storeStateClue("StatefulSet", "store")
	addClue(t, c, clue, true)
	at, ok := handoffAfter(t, c, world, test.start, horizon(test))
	report := autoReport(c)
	if !ok || at >= c.config.ClosureStabilityWindow+2*time.Second {
		t.Fatalf("handoff = %v at %s, want right after the settle", ok, at)
	}
	if !slices.Contains(report.GetHandoffKeys(), key) || !slices.Contains(report.GetHandoffKeys(), findingKey(clue)) ||
		report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED {
		t.Fatalf("report = %v", report)
	}
}

// On the serial path, a user test that joins the alert set and that no write
// or plan of the stage reaches starts the agent at once: it is a cause the
// stage is not repairing, and this is the agent's first start. One whose path
// runs through or calls the written component keeps the wait.
func TestAJoinerNoStageWriteReachesHandsOffAtOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		path, edges string
		want        bool
	}{
		"off the written component":      {path: "web, store", edges: "{from: web, to: store}", want: true},
		"on a caller of the written one": {path: "web, store", edges: "{from: web, to: api}, {from: web, to: store}"},
		"through the written one":        {path: "web, api, store", edges: "{from: web, to: api}, {from: web, to: store}"},
	} {
		t.Run(name, func(t *testing.T) {
			test := &buyTest{failing: alwaysFails}
			c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
			c.gitRead = buyMap(tc.path, tc.edges)
			at, ok := handoffAfter(t, c, world, test.start, 3*time.Second)
			if ok != tc.want {
				t.Fatalf("handoff = %v at %s, want %v", ok, at, tc.want)
			}
			if !tc.want {
				return
			}
			report := autoReport(c)
			if !slices.Contains(report.GetHandoffKeys(), key) || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED {
				t.Fatalf("report = %v", report)
			}
		})
	}
}

// sdo detect reads the stage's report as the controller holds it now, not as
// it was frozen into the agent's request.
func TestLiveRequestCarriesTheCurrentStageReport(t *testing.T) {
	current := IncidentRequest{IncidentID: "i", PriorStages: []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{PlaybookId: "p", Result: "applied_still_matching"}}}}}
	loop := &ResponderLoop{Request: IncidentRequest{IncidentID: "i"}, CurrentEvidence: func() *IncidentRequest { return &current }}
	if got := loop.liveRequest().PriorStages; len(got) != 1 || got[0].GetMitigations()[0].GetPlaybookId() != "p" {
		t.Fatalf("live request stages = %v", got)
	}
	lines := stageReportLines(current.PriorStages)
	if len(lines) != 1 || lines[0] != "stage: by sdo (playbook p): applied_still_matching" {
		t.Fatalf("stage lines = %q", lines)
	}
}

// Without an agent to start, a built-in-only stage waits for its recovery as
// before: no early handoff under escalate or wait, and none when the agent is
// not a selected stage; the restore that heals closes the incident.
func TestABuiltInOnlyStageWithoutAnAgentWaitsForRecovery(t *testing.T) {
	for name, tc := range map[string]struct {
		policy string
		stages []string
	}{
		"escalate":       {policy: settings.AfterPlaybookEscalate},
		"wait":           {policy: settings.AfterPlaybookWait},
		"no agent stage": {policy: settings.AfterPlaybookAgent, stages: []string{"deterministic"}},
	} {
		t.Run(name, func(t *testing.T) {
			c, world, closed := automaticRecoveryWorld(t, settlingCase{builtinOnly: true, healAfter: 20 * time.Second, stages: tc.stages}, tc.policy)
			for closed.Result == nil && world.clock.Now().Before(autoReport(c).GetRecoveryDeadline().AsTime()) {
				if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
					t.Fatalf("handed off or escalated a healing restore: %v", autoReport(c))
				}
				autoStep(t, c, world.clock, time.Second)
			}
			if final := deterministicStageReport(closed.Request.PriorStages); closed.Result == nil || final.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
				t.Fatalf("the restore's recovery did not close the incident: %+v", closed)
			}
		})
	}
}

// A full candidate block cedes its last lines, not the stage's report.
func TestTheStageReportLeadsAFullCandidateBlock(t *testing.T) {
	full := make([]string, 0, 1024)
	for len(full) < 1024 {
		full = append(full, "candidate")
	}
	request := IncidentRequest{RecentChanges: full, PriorStages: []*contracts.StageReport{{Stage: "deterministic", HandoffSelected: true,
		Mitigations: []*contracts.StageMitigation{{PlaybookId: "p", Result: "applied_still_matching"}}}}}
	got := stageDispatchRequest(request).RecentChanges
	if len(got) != 1024 || got[0] != "stage: by sdo (playbook p): applied_still_matching" || !recentChangesWithin(got) {
		t.Fatalf("candidate block = %d lines, first %q", len(got), got[0])
	}
}
