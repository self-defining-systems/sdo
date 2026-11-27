package runtime

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// stateDetector is a learned detector that names its playbook. Its first busy
// evaluations fail as the memory host answers while an earlier evaluation of
// the same detector is still running; fail makes every evaluation fail; hang
// makes every evaluation wait for its deadline.
type stateDetector struct {
	world     *sameWriteWorld
	class     sdk.DetectorClass
	playbooks []string
	busy      int32
	fail      bool
	hang      bool
	calls     *atomic.Int32
}

func (d stateDetector) Spec() sdk.DetectorSpec {
	class := d.class
	if class == "" {
		class = sdk.DetectorClassHealth
	}
	return sdk.DetectorSpec{ID: "store-state", Class: class, Owner: sdk.DetectorOwnerHealthJudge,
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

// A learned detector names a playbook with no program (a README and scripts).
// Its finding is read at open and joins the incident's findings, so the
// responder's task names the playbook; nothing is applied.
func TestDeterministicStageReadsDetectorOfReadmeOnlyPlaybook(t *testing.T) {
	calls := &atomic.Int32{}
	world, loop, state, instant := playbookDetectorLoop(t, nil,
		stateDetector{class: sdk.DetectorClassLearned, playbooks: []string{"validator-restore"}, busy: 1, calls: calls})
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("the learned detector was evaluated %d times, want one busy answer and one result", got)
	}
	if !slices.ContainsFunc(state.Request.Findings, func(f sdk.Finding) bool { return slices.Contains(f.Playbooks, "validator-restore") }) {
		t.Fatalf("the responder's findings must carry the README-only playbook's finding: %v", state.Request.Findings)
	}
	if got := world.restarts.Load(); got != 0 || len(report.Mitigations) != 0 {
		t.Fatalf("a playbook with no program applies nothing: %d writes, %v", got, report.Mitigations)
	}
}

// freshReadsProvider stands in for the pod-read observer: the first fresh
// request it sees leaves the store read unread, as a cycle over its load
// budget does; later ones answer.
type freshReadsProvider struct {
	SnapshotProvider
	requests *atomic.Int32
	asked    *[]bool
}

func (p freshReadsProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	if request, _ := ctx.Value(freshPodReadsKey{}).(*freshPodReads); request != nil {
		store := podread.Query{ID: storestate.QueryID(storestate.EngineMongoDB, "orders-db")}
		other := podread.Query{ID: "designer-read"}
		*p.asked = append(*p.asked, request.want(store), request.want(other))
		if p.requests.Add(1) == 1 {
			request.miss(store)
		}
	}
	return p.SnapshotProvider.Snapshot(ctx)
}

// The open-time read is a fresh one: a detector whose store read the cycle
// left unread is not evaluated on the kept answer; it is asked again and
// answers on the next read. The fresh request selects the store-state reads,
// not reads the detector does not use.
func TestDeterministicStageReadsPlaybookDetectorOnFreshStoreReads(t *testing.T) {
	calls := &atomic.Int32{}
	world, loop, state, instant := playbookDetectorLoop(t, []restartProgram{{id: "aware-restart"}},
		stateDetector{playbooks: []string{"aware-restart"}, calls: calls})
	requests, asked := &atomic.Int32{}, &[]bool{}
	loop.Provider = freshReadsProvider{SnapshotProvider: world, requests: requests, asked: asked}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("the stage made %d fresh reads, want one left unread and one answered", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("the detector was evaluated %d times, want only on the answered read", got)
	}
	if len(*asked) < 2 || !(*asked)[0] || (*asked)[1] {
		t.Fatalf("the fresh read must select the store-state read and no other: %v", *asked)
	}
	if len(report.Mitigations) != 1 || !report.Mitigations[0].GetApplyReceipt().GetApplied() {
		t.Fatalf("the learned playbook must apply on the fresh answer: %v", report)
	}
}

// hangingProgram never answers a diagnosis before its deadline.
type hangingProgram struct{ restartProgram }

func (hangingProgram) Diagnose(ctx context.Context, _ program.ReadView, _ program.Bindings) (program.Verdict, error) {
	<-ctx.Done()
	return program.Verdict{}, ctx.Err()
}

// The open-time read and the opening diagnosis (with its re-asks) share one
// diagnosis deadline: a hanging detector and a hanging diagnosis together
// delay the handoff by that bound, not by one bound each.
func TestDeterministicStageOpeningShareOneDiagnosisDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world, _, _ := sameWriteLoop(t)
		detector := stateDetector{world: world, playbooks: []string{"aware-restart"}, hang: true, calls: &atomic.Int32{}}
		registry, err := program.NewRegistry([]program.Diagnoser{hangingProgram{restartProgram{id: "aware-restart"}}}, []program.Mitigator{hangingProgram{restartProgram{id: "aware-restart"}}})
		if err != nil {
			t.Fatal(err)
		}
		deployment := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}
		alert := sdk.Finding{DetectorID: "user-test", RuleID: "scenario-slo", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
			PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "writes are not visible", Evidence: "synthetic user test", Playbooks: []string{"aware-restart"}}
		request := IncidentRequest{IncidentID: "synthetic-shared-deadline", Namespace: "app", SourceCommit: "synthetic-accepted",
			Findings: []sdk.Finding{alert}, RepairPolicy: "recorded-actions", OpeningKeys: []string{"user-test\x00" + FindingFingerprint(alert)}}
		loop := NewResponderLoop(request, registry, world, []sdk.Detector{detector}, PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world}, 5)
		loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
			return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
		}
		loop.saveToolState = func(context.Context, []byte) error { return nil }
		loop.DiagnosisDeadline = 4 * time.Second
		state := &IncidentState{Request: request, Findings: request.Findings}
		stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
		started := time.Now()
		report, err := stage.Run(context.Background(), state)
		if err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(started); waited > loop.DiagnosisDeadline {
			t.Fatalf("the opening took %s, beyond the one %s diagnosis deadline", waited, loop.DiagnosisDeadline)
		}
		if detector.calls.Load() == 0 {
			t.Fatal("the playbook detector must be read at open")
		}
		if got := world.restarts.Load(); got != 0 || len(report.Mitigations) != 0 {
			t.Fatalf("nothing answered, so nothing applies: %d writes, %v", got, report.Mitigations)
		}
	})
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
