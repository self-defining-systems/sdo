package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// lagProbe is a probe-scenario finding on Deployment api whose error rate
// follows the clock: it declines (a repair that is taking effect), rises (a
// fix that did not take) or stays flat, until healAt.
type lagProbe struct {
	clock  *settlingClock
	start  time.Time
	healAt time.Time
	slope  float64
}

type windowedLagProbe struct{ lagProbe }

func (windowedLagProbe) EvaluationWindow(sdk.DetectionContext) time.Duration {
	return 60 * time.Second
}

func (p lagProbe) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "traffic-health", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"api-request-stall"}, OriginatingCommit: "seed"}
}

func (p lagProbe) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	now := p.clock.Now()
	if !p.healAt.IsZero() && !now.Before(p.healAt) {
		return []sdk.Finding{}, nil
	}
	rate := 0.5 + p.slope*now.Sub(p.start).Seconds()
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		findings = append(findings, sdk.Finding{DetectorID: "traffic-health", RuleID: traffic.RuleIDPrefix + "read", Status: sdk.FindingActive,
			Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			Summary: "synthetic scenario read violates its SLO", Evidence: "synthetic window", Playbooks: []string{"api-request-stall"},
			Metadata: map[string]any{"error_rate": rate}})
	}
	return findings, nil
}

// lagStage is the quieted-worker shape: the stage applies a program, the
// program no longer matches, and one probe finding still lags at the recovery
// bound.
func lagStage(t *testing.T, slope float64, healAfter time.Duration) (*DeterministicResponseStage, *IncidentState, *settlingClock) {
	t.Helper()
	world := newSettlingWorld(settlingCase{traffic: &settlingTraffic{}})
	clock := world.clock
	probe := lagProbe{clock: clock, start: clock.Now(), slope: slope}
	if healAfter > 0 {
		probe.healAt = clock.Now().Add(healAfter)
	}
	detectors := []sdk.Detector{windowedLagProbe{probe}}
	restart := restartProgram{id: "api-request-stall"}
	registry, err := program.NewRegistry([]program.Diagnoser{restart}, []program.Mitigator{restart})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := world.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := probe.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "synthetic-lag", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings,
		DetectedAt: clock.Now(), RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		Budget: 30 * time.Second, ObservationInterval: time.Second, recoveryNow: clock.Now, recoveryWait: clock.advance}
	return stage, &IncidentState{Request: request, Findings: findings}, clock
}

// A probe finding that lags the repair at the recovery bound is observed for
// the rest of one detector window before the responder is dispatched, so a
// repair that is taking effect is not handed to a model as a new problem.
func TestLagSettlementExtendsToTheDetectorWindowBeforeHandoff(t *testing.T) {
	// The error rate declines and the probe clears after the original 30 s
	// bound but inside its 60 s window.
	stage, state, clock := lagStage(t, -0.005, 50*time.Second)
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	report := state.PriorStages[0]
	if agent.dispatched {
		t.Fatalf("a lagging probe that clears inside its window must not reach the responder: %v", report)
	}
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatalf("recovery wait exit = %s, want CLEARED", got)
	}
	if report.GetHandoffState() != nil {
		t.Fatalf("a confirmed repair carries no pending hand-off state: %v", report.GetHandoffState())
	}
	_ = clock
}

// The extension is bounded by one detector window. A probe that is still red
// then (its rate flat) goes to the responder with a typed observe-only state
// that ends in a time limit.
func TestLagSettlementIsBoundedAndHandsOffObserveOnly(t *testing.T) {
	stage, state, clock := lagStage(t, 0, 0)
	started := clock.Now()
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	if !agent.dispatched {
		t.Fatal("a probe that never clears must reach the responder")
	}
	if waited := clock.Now().Sub(started); waited > 30*time.Second+64*time.Second+2*time.Second {
		t.Fatalf("the extension exceeded one detector window: observed %s", waited)
	}
	handoff := state.PriorStages[0].GetHandoffState()
	if handoff.GetRepairState() != contracts.HandoffState_REPAIR_STATE_PENDING_CONFIRMATION ||
		handoff.GetPostApplyDiagnose() != contracts.HandoffState_POST_APPLY_DIAGNOSE_NO_MATCH {
		t.Fatalf("hand-off state = %v", handoff)
	}
	if !handoff.GetObserveOnly() || handoff.GetObserveUntil() == nil || handoff.GetMutationRoundsPerFinding() != 0 {
		t.Fatalf("a non-increasing lagging probe is observe-only with a time limit: %v", handoff)
	}
	if len(handoff.GetResidualFindings()) != 1 || handoff.GetResidualFindings()[0].GetResidualClass() != contracts.HandoffState_RESIDUAL_CLASS_PROBE_LAG {
		t.Fatalf("residual findings = %v", handoff.GetResidualFindings())
	}
	if until := handoff.GetObserveUntil().AsTime(); !until.After(clock.Now()) || until.Sub(clock.Now()) > 2*time.Minute {
		t.Fatalf("observe_until %s must be a bounded future time (now %s)", until, clock.Now())
	}
	if len(handoff.GetAppliedPlaybookIds()) != 1 || handoff.GetAppliedPlaybookIds()[0] != "api-request-stall" || handoff.GetAppliedAt() == nil {
		t.Fatalf("applied repair not recorded: %v", handoff)
	}
}

// A probe whose error rate rises during the extension is not lag: it is a red
// probe after an incomplete fix. The extension stops and the responder keeps
// its ordinary mandate with one further mutation round.
func TestRisingProbeIsNotLag(t *testing.T) {
	stage, state, clock := lagStage(t, 0.005, 0)
	started := clock.Now()
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	if !agent.dispatched {
		t.Fatal("a rising probe must reach the responder")
	}
	if waited := clock.Now().Sub(started); waited > 30*time.Second+5*time.Second {
		t.Fatalf("a rising rate must end the extension at once: observed %s", waited)
	}
	handoff := state.PriorStages[0].GetHandoffState()
	if handoff.GetObserveOnly() || handoff.GetObserveUntil() != nil || handoff.GetMutationRoundsPerFinding() != 1 {
		t.Fatalf("a rising probe is active, not observe-only: %v", handoff)
	}
	if got := handoff.GetResidualFindings()[0].GetResidualClass(); got != contracts.HandoffState_RESIDUAL_CLASS_ACTIVE {
		t.Fatalf("residual class = %s, want ACTIVE", got)
	}
}

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
		return buildHandoffState(&IncidentState{Request: request, Findings: now}, handoffReport(diagnose, true), nil, now2(now), 0)
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
	if h := buildHandoffState(&IncidentState{Request: request, Findings: []sdk.Finding{clue}}, handoffReport(contracts.Verdict_RESULT_NO_MATCH, false), nil, now, 0); h != nil {
		t.Fatalf("no applied repair, no hand-off state: %v", h)
	}
	if h := buildHandoffState(&IncidentState{Request: request}, handoffReport(contracts.Verdict_RESULT_NO_MATCH, true), nil, now, 0); h != nil {
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
