package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// outOfReachProgram localizes the stall to a cause outside the namespace: its
// Diagnose matches with an out-of-reach verdict on the bound Deployment.
type outOfReachProgram struct {
	trafficStallProgram
	plan *program.Plan
}

func (p outOfReachProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	verdict, err := p.trafficStallProgram.Diagnose(ctx, view, b)
	if err != nil || verdict.Result != program.Match {
		return verdict, err
	}
	verdict.CauseClass, verdict.Reach = "network-delay", program.ReachOutOfReach
	verdict.Evidence = []program.Evidence{{Resource: b.Objects["deployment"], Fact: "every link into the dependency is slow", Observed: "p50 900ms", Expected: "baseline 5ms"}}
	return verdict, nil
}

type escalatingProgram struct{ outOfReachProgram }

func (p escalatingProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return *p.plan, nil
}

func escalationStage(t *testing.T, mitigator program.Mitigator, diagnoser program.Diagnoser) (*DeterministicResponseStage, *IncidentState) {
	t.Helper()
	tc := settlingCase{healAfter: time.Hour, traffic: &settlingTraffic{}}
	settling := newSettlingWorld(tc)
	settling.effectAt = settling.clock.Now().Add(24 * time.Hour)
	settling.observed.Store(1)
	clock := settling.clock
	stall := trafficLearnedStall{settlingDetector{id: "api-acl", class: sdk.DetectorClassIncident, playbooks: []string{"api-acl"}, clock: clock}}
	detectors := []sdk.Detector{stall}
	var mitigators []program.Mitigator
	if mitigator != nil {
		mitigators = append(mitigators, mitigator)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{diagnoser}, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settling.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := stall.Detect(context.Background(), snapshot)
	if err != nil || len(findings) == 0 {
		t.Fatalf("the fault must be detected: %v", err)
	}
	request := IncidentRequest{IncidentID: "synthetic-escalation", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: settling.client}, Source: settling, Rollouts: settling, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: settling}}
	loop := NewResponderLoop(request, registry, settling, detectors, executor, 5)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		ObservationInterval: time.Second, recoveryNow: clock.Now, recoveryWait: clock.advance}
	return stage, &IncidentState{Request: request, Findings: findings}
}

// A playbook that localizes an out-of-reach cause closes the incident as
// escalated without the responder: a Diagnose-only playbook on its verdict,
// and a playbook with a mitigation through an Escalate plan. A plan that
// repairs instead proceeds as usual.
func TestOutOfReachDiagnosisEscalatesWithoutTheResponder(t *testing.T) {
	stall := trafficStallProgram{restartProgram{id: "api-acl"}}
	escalate := program.Plan{Actions: []program.Action{program.Escalate{Reason: "the dependency's network path is slow outside the namespace"}}}
	for name, programs := range map[string]struct {
		diagnoser program.Diagnoser
		mitigator program.Mitigator
	}{
		"diagnose-only playbook": {diagnoser: outOfReachProgram{trafficStallProgram: stall}},
		"escalate plan":          {diagnoser: escalatingProgram{outOfReachProgram{trafficStallProgram: stall, plan: &escalate}}, mitigator: escalatingProgram{outOfReachProgram{trafficStallProgram: stall, plan: &escalate}}},
	} {
		t.Run(name, func(t *testing.T) {
			stage, state := escalationStage(t, programs.mitigator, programs.diagnoser)
			agent := &recordingStage{}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
				t.Fatal(err)
			}
			if agent.dispatched {
				t.Fatal("the responder was dispatched for an escalated out-of-reach cause")
			}
			attempt := state.PriorStages[0].GetMitigations()[0]
			if attempt.GetResult() != stageEscalated || attempt.GetApplyReceipt() != nil || attempt.GetEscalation().GetVerdict().GetReach() != program.ReachOutOfReach {
				t.Fatalf("attempt = %v", attempt)
			}
			result, err := responsePipelineResult(state, time.Unix(0, 0), time.Unix(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			message, err := incidentResultContract(result)
			if err != nil {
				t.Fatal(err)
			}
			request, err := incidentRequestContract(state.Request)
			if err != nil {
				t.Fatal(err)
			}
			if len(message.GetConfirmedRootCauses()) != 1 || message.GetConfirmedRootCauses()[0].GetCauseClass() != "network-delay" {
				t.Fatalf("causes = %v", message.GetConfirmedRootCauses())
			}
			if !outcome.Escalation(message.GetConfirmedRootCauses(), request.GetFindings(), message.GetRepairActions()) {
				t.Fatalf("the stage's cause does not derive an escalated outcome: %v", message.GetConfirmedRootCauses())
			}
		})
	}
	t.Run("in-scope verdict without a mitigation is still skipped", func(t *testing.T) {
		stage, state := escalationStage(t, nil, stall)
		agent := &recordingStage{}
		if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
			t.Fatal(err)
		}
		if !agent.dispatched || state.PriorStages[0].GetMitigations()[0].GetSkipReason() != "no_mitigation" {
			t.Fatalf("dispatched=%v report=%v", agent.dispatched, state.PriorStages[0])
		}
	})
}
