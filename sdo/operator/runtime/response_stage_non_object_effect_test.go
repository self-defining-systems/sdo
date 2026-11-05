package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// aclWorld is the R6' fault: a Redis user lost its read permission. The
// repair is a command in the Pod's container, so it writes no Kubernetes
// object. Requests time out until the repair runs, then succeed after the
// prober's next sample.
type aclWorld struct {
	*settlingWorld
	mu       sync.Mutex
	fixed    bool
	ensures  int
	ensureOK bool // whether the ensure command really restores the grant
	// stillFailing keeps requests failing after the grant is restored: the
	// command took effect but was not the incident's cause.
	stillFailing bool
}

func (w *aclWorld) RunOperation(_ context.Context, op program.ExternalOperation) (int32, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if op.Exec != nil && op.Exec.Args[0] == "restore-grant" {
		w.ensures++
		if w.ensureOK {
			w.fixed = true
			// The repair reaches the prober's samples a second later, so the
			// program's own post-apply diagnosis still sees the fault (R6').
			if !w.stillFailing {
				w.effectAt = w.clock.Now().Add(time.Second)
			}
		}
		return 0, nil
	}
	if w.fixed {
		return 0, nil
	}
	return 1, nil
}

// aclClient executes plans against the fake cluster and the world's commands.
type aclClient struct {
	TypedPlanClient
	world *aclWorld
}

func (c aclClient) RunOperation(ctx context.Context, op program.ExternalOperation) (int32, error) {
	return c.world.RunOperation(ctx, op)
}

// aclProgram is the learned ACL playbook: it diagnoses from the latest health
// sample, as the learned program did, and restores the grant in the bound
// workload's Pod.
type aclProgram struct{ trafficStallProgram }

func (aclProgram) Mitigate(_ context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
	observe := program.Command{Pod: pod, Container: "api", Args: []string{"probe-grant"}}
	ensure := program.Command{Pod: pod, Container: "api", Args: []string{"restore-grant"}}
	return program.Plan{Actions: []program.Action{program.EnsureExec{Observe: observe, Ensure: ensure, Expect: 0, Reason: "restore the read grant"}}}, nil
}

func aclStage(t *testing.T, ensureOK, stillFailing bool) (*DeterministicResponseStage, *IncidentState, *aclWorld) {
	t.Helper()
	tc := settlingCase{healAfter: time.Hour, traffic: &settlingTraffic{}}
	settling := newSettlingWorld(tc)
	settling.effectAt = settling.clock.Now().Add(24 * time.Hour) // until repaired
	settling.observed.Store(1)                                   // no rollout to wait for
	world := &aclWorld{settlingWorld: settling, ensureOK: ensureOK, stillFailing: stillFailing}
	clock := settling.clock
	stall := trafficLearnedStall{settlingDetector{id: "api-acl", class: sdk.DetectorClassIncident, playbooks: []string{"api-acl"}, clock: clock}}
	health := trafficLearnedStall{settlingDetector{id: "traffic-health", class: sdk.DetectorClassHealth, clock: clock}}
	detectors := []sdk.Detector{health, stall}
	acl := aclProgram{trafficStallProgram{restartProgram{id: "api-acl"}}}
	registry, err := program.NewRegistry([]program.Diagnoser{acl}, []program.Mitigator{acl})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settling.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var findings []sdk.Finding
	for _, detector := range detectors {
		found, err := detector.Detect(context.Background(), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		findings = append(findings, found...)
	}
	if len(findings) == 0 {
		t.Fatal("the fault must be detected")
	}
	sortFindings(findings)
	request := IncidentRequest{IncidentID: "synthetic-acl", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: aclClient{TypedPlanClient: TypedPlanClient{Client: settling.client}, world: world}, Source: settling, Rollouts: settling, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		ObservationInterval: time.Second, recoveryNow: clock.Now, recoveryWait: clock.advance}
	return stage, &IncidentState{Request: request, Findings: findings}, world
}

// A learned playbook whose repair changes state outside Kubernetes objects
// (here a command in a container) is proved the same way as a script: its
// action succeeded and its own diagnosis turned from MATCH to NO_MATCH after
// it. The in-stage wait then treats it like a settled write and clears on
// fresh traffic, without the responder. An ensure that does not take effect,
// or one that does while the fault persists, earns no credit and goes to the
// responder.
func TestNonObjectRepairSettlesOnObservedTransition(t *testing.T) {
	t.Run("exec repair takes effect", func(t *testing.T) {
		stage, state, world := aclStage(t, true, false)
		started := world.clock.Now()
		agent := &recordingStage{}
		if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
			t.Fatal(err)
		}
		report := state.PriorStages[0]
		if len(report.Mitigations) != 1 || world.ensures != 1 {
			t.Fatalf("the stage must apply the ACL repair once (ensures=%d): %v", world.ensures, report)
		}
		attempt := report.Mitigations[0]
		receipt := attempt.GetApplyReceipt()
		if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED {
			t.Fatalf("recovery wait exit = %s after %s, want CLEARED: %v", got, world.clock.Now().Sub(started), report)
		}
		if agent.dispatched {
			t.Fatalf("the responder was dispatched for a repair the stage proved: %v", report)
		}
		if attempt.Result != "applied_cleared" || receipt.GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED ||
			receipt.GetFinalVerificationBasis() != "observed-transition" || !receipt.GetVerificationPassed() || !finalApplicationVerified(receipt) {
			t.Fatalf("attempt %s, settlement %s, basis %q, verified %v: want applied_cleared on an observed transition", attempt.Result, receipt.GetSettlementRejectionReason(), receipt.GetFinalVerificationBasis(), receipt.GetVerificationPassed())
		}
	})
	t.Run("exec repair without effect", func(t *testing.T) {
		stage, state, world := aclStage(t, false, false)
		agent := &recordingStage{}
		if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
			t.Fatal(err)
		}
		report := state.PriorStages[0]
		if world.ensures != 1 || !agent.dispatched || report.GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED {
			t.Fatalf("an ensure that changed nothing must go to the responder (ensures=%d, dispatched=%v): %v", world.ensures, agent.dispatched, report)
		}
		if got := report.Mitigations[0].Result; got == "applied_cleared" {
			t.Fatalf("an ineffective ensure was credited: %v", report.Mitigations[0])
		}
	})
	t.Run("exec repair takes effect but the fault persists", func(t *testing.T) {
		stage, state, world := aclStage(t, true, true)
		started := world.clock.Now()
		agent := &recordingStage{}
		if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
			t.Fatal(err)
		}
		report := state.PriorStages[0]
		if world.ensures != 1 || !agent.dispatched || report.GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED {
			t.Fatalf("a persisting fault must go to the responder (ensures=%d, dispatched=%v): %v", world.ensures, agent.dispatched, report)
		}
		if waited := world.clock.Now().Sub(started); waited > 90*time.Second {
			t.Fatalf("the stage observed for %s, past the recovery bound", waited)
		}
		if got := report.Mitigations[0]; got.Result == "applied_cleared" || finalApplicationVerified(got.GetApplyReceipt()) {
			t.Fatalf("a repair that did not clear its diagnosis was credited: %v", got)
		}
	})
}
