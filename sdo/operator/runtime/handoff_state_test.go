package runtime

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func handoffReport(diagnose contracts.Verdict_Result, applied bool) *contracts.StageReport {
	return &contracts.StageReport{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{
		PlaybookId: "p", Result: "applied_still_matching",
		ApplyReceipt:      &contracts.PlanApplication{Applied: applied, CompletedAt: contractTimestamp(time.Unix(100, 0))},
		PostApplyDiagnose: &contracts.ProgramRun{ProgramId: "p", Verdict: &contracts.Verdict{Result: diagnose}},
	}}}
}

func standingClue(severity sdk.FindingSeverity, summary string) sdk.Finding {
	return sdk.Finding{DetectorID: "memory-saturation", RuleID: "saturation", Status: sdk.FindingActive, Severity: severity, Summary: summary,
		PrimaryResource: sdk.ObjectRef{Kind: "StatefulSet", Namespace: "app", Name: "store"}}
}

// A finding that was active before the incident opened and has not changed is
// baseline; the controller says so, and tells the responder not to open a new
// repair for it. A changed clue, a finding that arose after open and an
// opening alert are active.
func TestHandoffStateClassifiesBaselineAndActive(t *testing.T) {
	clue := standingClue(sdk.SeverityWarning, "memory is high")
	opening := sdk.Finding{DetectorID: "workload-health", RuleID: "crash", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}, Summary: "api is crashing"}
	joined := standingClue(sdk.SeverityWarning, "memory is high")
	joined.DetectorID = "other-detector"
	worse := clue
	worse.Severity = sdk.SeverityCritical
	key := func(f sdk.Finding) string { return FindingStateKey(f.DetectorID, FindingFingerprint(f)) }
	opened := time.Unix(50, 0).UTC()
	request := IncidentRequest{DetectedAt: opened, Findings: []sdk.Finding{opening, clue}, OpeningKeys: []string{key(opening)}, StandingKeys: []string{key(clue)}}
	now := time.Unix(200, 0).UTC()
	build := func(diagnose contracts.Verdict_Result, now []sdk.Finding) *contracts.HandoffState {
		return buildHandoffState(&IncidentState{Request: request, Findings: now}, handoffReport(diagnose, true), now2(now))
	}
	_ = now
	classes := func(h *contracts.HandoffState) map[string]contracts.HandoffState_ResidualClass {
		out := map[string]contracts.HandoffState_ResidualClass{}
		for _, r := range h.GetResidualFindings() {
			out[r.GetKey()] = r.GetResidualClass()
		}
		return out
	}

	h := build(contracts.Verdict_RESULT_NO_MATCH, []sdk.Finding{clue})
	if !h.GetObserveOnly() || classes(h)[key(clue)] != contracts.HandoffState_RESIDUAL_CLASS_BASELINE_CLUE {
		t.Fatalf("an unchanged standing clue is baseline and observe-only: %v", h)
	}
	if got := h.GetResidualFindings()[0].GetFirstSeen().AsTime(); !got.Equal(opened) {
		t.Fatalf("a standing clue was first seen at or before open: %s", got)
	}
	if h := build(contracts.Verdict_RESULT_NO_MATCH, []sdk.Finding{worse}); h.GetObserveOnly() || !h.GetResidualFindings()[0].GetChangedSinceOpen() ||
		classes(h)[key(worse)] != contracts.HandoffState_RESIDUAL_CLASS_ACTIVE {
		t.Fatalf("a clue that changed since open is active: %v", h)
	}
	if h := build(contracts.Verdict_RESULT_NO_MATCH, []sdk.Finding{joined}); h.GetObserveOnly() {
		t.Fatalf("a finding that arose after open is active: %v", h)
	}
	if h := build(contracts.Verdict_RESULT_NO_MATCH, []sdk.Finding{opening, clue}); h.GetObserveOnly() || h.GetMutationRoundsPerFinding() != 1 {
		t.Fatalf("an opening alert is active: %v", h)
	}
	if h := build(contracts.Verdict_RESULT_MATCH, []sdk.Finding{clue}); h.GetObserveOnly() ||
		h.GetPostApplyDiagnose() != contracts.HandoffState_POST_APPLY_DIAGNOSE_MATCH {
		t.Fatalf("a program that still matches is not observe-only: %v", h)
	}
	if h := buildHandoffState(&IncidentState{Request: request, Findings: []sdk.Finding{clue}}, handoffReport(contracts.Verdict_RESULT_NO_MATCH, false), now); h != nil {
		t.Fatalf("no applied repair, no hand-off state: %v", h)
	}
	if h := buildHandoffState(&IncidentState{Request: request}, handoffReport(contracts.Verdict_RESULT_NO_MATCH, true), now); h != nil {
		t.Fatalf("nothing pending, no hand-off state: %v", h)
	}
}

func now2(_ []sdk.Finding) time.Time { return time.Unix(200, 0).UTC() }

// The agent stage attaches the state to the deterministic report it hands
// over when the stage did not (a resumed stage), and never rewrites one the
// stage built.
func TestAgentStageAttachesHandoffStateOnce(t *testing.T) {
	clue := standingClue(sdk.SeverityWarning, "memory is high")
	key := FindingStateKey(clue.DetectorID, FindingFingerprint(clue))
	request := IncidentRequest{DetectedAt: time.Unix(50, 0).UTC(), Findings: []sdk.Finding{clue}, StandingKeys: []string{key}}
	report := handoffReport(contracts.Verdict_RESULT_NO_MATCH, true)
	state := &IncidentState{Request: request, Findings: []sdk.Finding{clue}, PriorStages: []*contracts.StageReport{report}}
	attachHandoffState(state, time.Unix(200, 0).UTC())
	built := report.GetHandoffState()
	if !built.GetObserveOnly() {
		t.Fatalf("not attached: %v", built)
	}
	prior := proto.Clone(built).(*contracts.HandoffState)
	attachHandoffState(state, time.Unix(900, 0).UTC())
	if !proto.Equal(prior, report.GetHandoffState()) {
		t.Fatal("an existing state must not be rewritten")
	}
}
