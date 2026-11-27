package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// stateDetector is a learned detector that names its playbook. Its first busy
// evaluations fail as the memory host answers while an earlier evaluation of
// the same detector is still running; fail makes every evaluation fail; hang
// makes every evaluation wait for its deadline.
type stateDetector struct {
	world     *sameWriteWorld
	playbooks []string
	busy      int32
	fail      bool
	hang      bool
	calls     *atomic.Int32
}

func (d stateDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "store-state", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: 30 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: d.playbooks, OriginatingCommit: "seed"}
}

func (d stateDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	call := d.calls.Add(1)
	switch {
	case d.hang:
		<-ctx.Done()
		return nil, ctx.Err()
	case d.fail || call <= d.busy:
		return nil, errors.New("detector is still running a previous evaluation")
	}
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		findings = append(findings, sdk.Finding{DetectorID: "store-state", RuleID: "state-drift", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
			PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "store state differs from healthy", Evidence: "synthetic state", Playbooks: d.playbooks})
	}
	return findings, nil
}

// playbookDetectorLoop is an incident that opened on a user-facing alert that
// binds no playbook, while the learned playbook's own state detector has not
// evaluated since the fault began.
func playbookDetectorLoop(t *testing.T, programs []restartProgram, detector stateDetector) (*sameWriteWorld, *ResponderLoop, *IncidentState, *instantRecoveryClock) {
	t.Helper()
	world, _, _ := sameWriteLoop(t)
	detector.world = world
	if detector.calls == nil {
		detector.calls = &atomic.Int32{}
	}
	diagnosers, mitigators := []program.Diagnoser{}, []program.Mitigator{}
	for _, p := range programs {
		diagnosers, mitigators = append(diagnosers, p), append(mitigators, p)
	}
	registry, err := program.NewRegistry(diagnosers, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}}
	alert := sdk.Finding{DetectorID: "user-test", RuleID: "scenario-slo", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRefFrom("Service", "v1", &service), Summary: "writes are not visible", Evidence: "synthetic user test"}
	request := IncidentRequest{IncidentID: "synthetic-playbook-detector", Namespace: "app", SourceCommit: "synthetic-accepted",
		Findings: []sdk.Finding{alert}, RepairPolicy: "recorded-actions", OpeningKeys: []string{"user-test\x00" + FindingFingerprint(alert)}}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{sameWriteDetector{world: world}}, Provider: world}}
	loop := NewResponderLoop(request, registry, world, []sdk.Detector{detector}, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	instant := newInstantRecoveryClock()
	loop.Now = instant.now
	return world, loop, &IncidentState{Request: request, Findings: request.Findings}, instant
}

// The learned playbook's detector is still busy when the incident opens. The
// stage asks it again until it answers, then the playbook it names matches
// and applies: the repeat is not a first try.
func TestDeterministicStageWaitsForBusyPlaybookDetector(t *testing.T) {
	calls := &atomic.Int32{}
	world, loop, state, instant := playbookDetectorLoop(t, []restartProgram{{id: "aware-restart"}},
		stateDetector{playbooks: []string{"aware-restart"}, busy: 2, calls: calls})
	started := instant.now()
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("the playbook's detector was evaluated %d times, want two busy answers and one result", got)
	}
	if waited := instant.now().Sub(started); waited < 2*stageDiagnoseBackoff {
		t.Fatalf("the stage waited %s before diagnosing, want the two backoffs", waited)
	}
	if len(report.Mitigations) != 1 || !report.Mitigations[0].GetApplyReceipt().GetApplied() {
		t.Fatalf("the learned playbook must apply once its detector answers: %v", report)
	}
	if got := world.restarts.Load(); got != 1 {
		t.Fatalf("the Deployment was written %d times, want the playbook's one restart", got)
	}
}

// A detector that never answers delays the handoff by the diagnosis deadline
// at most, and the stage then hands off exactly as it did before.
func TestDeterministicStageHandsOffWhenPlaybookDetectorFails(t *testing.T) {
	calls := &atomic.Int32{}
	world, loop, state, instant := playbookDetectorLoop(t, []restartProgram{{id: "aware-restart"}},
		stateDetector{playbooks: []string{"aware-restart"}, fail: true, calls: calls})
	loop.DiagnosisDeadline = 3 * time.Second
	started := instant.now()
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if waited := instant.now().Sub(started); waited > loop.DiagnosisDeadline {
		t.Fatalf("the stage waited %s for the detector, beyond the %s diagnosis deadline", waited, loop.DiagnosisDeadline)
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("the failing detector was asked %d times, want it asked again within the bound", got)
	}
	assertNothingApplied(t, world, report, state)
}

// A detector that hangs is abandoned at the same bound.
func TestDeterministicStageHandsOffWhenPlaybookDetectorHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world, loop, state, _ := playbookDetectorLoop(t, []restartProgram{{id: "aware-restart"}},
			stateDetector{playbooks: []string{"aware-restart"}, hang: true})
		loop.Now = nil
		loop.DiagnosisDeadline = 2 * time.Second
		stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
		started := time.Now()
		report, err := stage.Run(context.Background(), state)
		if err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(started); waited > loop.DiagnosisDeadline+stageDiagnoseBackoff {
			t.Fatalf("the stage took %s, beyond the %s diagnosis deadline", waited, loop.DiagnosisDeadline)
		}
		assertNothingApplied(t, world, report, state)
	})
}

// With no learned playbook a detector names, nothing is read and nothing waits.
func TestDeterministicStageReadsNoDetectorWithoutLearnedPlaybook(t *testing.T) {
	for name, programs := range map[string][]restartProgram{"no learned playbook": nil, "playbook the detector does not name": {{id: "aware-restart"}}} {
		t.Run(name, func(t *testing.T) {
			calls := &atomic.Int32{}
			world, loop, state, instant := playbookDetectorLoop(t, programs, stateDetector{playbooks: []string{"other-playbook"}, calls: calls})
			started := instant.now()
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
			report, err := stage.Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("the detector was evaluated %d times, want none", got)
			}
			if waited := instant.now().Sub(started); waited != 0 {
				t.Fatalf("the stage waited %s, want no delay", waited)
			}
			assertNothingApplied(t, world, report, state)
		})
	}
}

func assertNothingApplied(t *testing.T, world *sameWriteWorld, report *contracts.StageReport, state *IncidentState) {
	t.Helper()
	if got := world.restarts.Load(); got != 0 {
		t.Fatalf("the Deployment was written %d times, want none", got)
	}
	if len(report.Mitigations) != 0 {
		t.Fatalf("nothing should be attempted: %v", report.Mitigations)
	}
	if report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED {
		t.Fatalf("the stage must hand off as nothing applied: %v", report)
	}
	if len(state.Findings) != 1 || state.Findings[0].DetectorID != "user-test" {
		t.Fatalf("the opening findings must stand: %v", state.Findings)
	}
}
