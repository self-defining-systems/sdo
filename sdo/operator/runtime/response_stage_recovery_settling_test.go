package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// settlingClock is the one clock of a recovery-settling world: the stage's
// recovery wait advances it instead of sleeping.
type settlingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *settlingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *settlingClock) advance(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return nil
}

// settlingDetector judges a rolling window of requests to Deployment api: it
// keeps firing until the world's fault has been gone for its window.
type settlingDetector struct {
	id        string
	class     sdk.DetectorClass
	window    time.Duration
	playbooks []string
	clock     *settlingClock
	// healAt is when the detector's window no longer holds failed requests; zero never.
	healAt time.Time
}

type windowedSettlingDetector struct{ settlingDetector }

func (d windowedSettlingDetector) EvaluationWindow(sdk.DetectionContext) time.Duration {
	return d.window
}

func (d settlingDetector) Spec() sdk.DetectorSpec {
	owner := sdk.DetectorOwnerHealthJudge
	if d.class == sdk.DetectorClassIncident {
		owner = sdk.DetectorOwnerResponder
	}
	return sdk.DetectorSpec{ID: d.id, Class: d.class, Owner: owner,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: d.playbooks, OriginatingCommit: "seed"}
}

func (d settlingDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.healAt.IsZero() && !d.clock.Now().Before(d.healAt) {
		return []sdk.Finding{}, nil
	}
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		findings = append(findings, sdk.Finding{DetectorID: d.id, RuleID: d.id, Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "requests to api time out", Evidence: "synthetic window", Playbooks: d.playbooks})
	}
	return findings, nil
}

// recordingStage stands in for the responder and records whether it was dispatched.
type recordingStage struct{ dispatched bool }

func (*recordingStage) Name() string { return "agent" }
func (s *recordingStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	s.dispatched = true
	return &contracts.StageReport{}, nil
}

type settlingCase struct {
	// healAfter is when, after the restart, the windowed detectors stop
	// reporting the fault; zero means the fault persists.
	healAfter time.Duration
	// stallWindow is the learned incident detector's window; zero means it
	// judges current state only.
	stallWindow time.Duration
	blind       bool
}

// settlingStage builds the R3b shape: a learned incident detector reports a
// request stall on Deployment api and names the learned restart playbook, the
// judge's traffic-health detector reports the same journey unexplained, and
// both judge a rolling window. The restart's first rollout sample is still in
// progress; every later one has completed.
func settlingStage(t *testing.T, tc settlingCase) (*DeterministicResponseStage, *IncidentState, *settlingClock, *sameWriteWorld) {
	t.Helper()
	replicas := int32(1)
	world := &sameWriteWorld{client: fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", UID: "synthetic-deployment", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "api:v1"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", UID: "synthetic-pod", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "api:v1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	)}
	world.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			world.restarts.Add(1)
		}
		return false, nil, nil
	})
	// The recovery bound is also a real context deadline, so the synthetic
	// clock starts far enough ahead that only the synthetic bound can end it.
	clock := &settlingClock{now: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
	var healAt time.Time
	if tc.healAfter > 0 {
		healAt = clock.Now().Add(tc.healAfter)
	}
	traffic := windowedSettlingDetector{settlingDetector{id: "traffic-health", class: sdk.DetectorClassHealth, window: 60 * time.Second, clock: clock, healAt: healAt}}
	stall := settlingDetector{id: "api-request-stall", class: sdk.DetectorClassIncident, window: tc.stallWindow, playbooks: []string{"api-request-stall"}, clock: clock, healAt: healAt}
	var stallDetector sdk.Detector = stall
	if tc.stallWindow > 0 {
		stallDetector = windowedSettlingDetector{stall}
	}
	detectors := []sdk.Detector{traffic, stallDetector}
	restart := restartProgram{id: "api-request-stall", blind: tc.blind}
	registry, err := program.NewRegistry([]program.Diagnoser{restart}, []program.Mitigator{restart})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := world.Snapshot(context.Background())
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
	sortFindings(findings)
	request := IncidentRequest{IncidentID: "synthetic-settling", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		ObservationInterval: time.Second, recoveryNow: clock.Now, recoveryWait: clock.advance}
	return stage, &IncidentState{Request: request, Findings: findings}, clock, world
}

// An in-stage recovery wait does not give up while the write it applied is
// still taking effect or while windowed detectors are still catching up with
// it. When recovery follows within their window, the stage credits its own
// restart and the responder is not dispatched. A fault that persists past the
// window, or an incident finding that judges current state only, still goes
// to the responder.
func TestRecoveryWaitSettlesWhileEffectsAndWindowsCatchUp(t *testing.T) {
	for _, tc := range []struct {
		name string
		settlingCase
		wantExit       contracts.RecoveryWait_ExitReason
		wantResult     string
		wantDispatched bool
		// minWait and maxWait bound how long the stage observed before handing off.
		minWait, maxWait time.Duration
	}{
		{"windows catch up after the rollout", settlingCase{healAfter: 40 * time.Second, stallWindow: 60 * time.Second},
			contracts.RecoveryWait_EXIT_REASON_CLEARED, "applied_cleared", false, 40 * time.Second, 45 * time.Second},
		// The fault outlives the windows (60 s + one clearing interval after the
		// rollout), which end before the 90 s recovery bound.
		{"fault persists past the windows", settlingCase{stallWindow: 60 * time.Second},
			contracts.RecoveryWait_EXIT_REASON_FAILED, "applied_still_matching", true, 61 * time.Second, 65 * time.Second},
		{"incident finding without a window", settlingCase{healAfter: 40 * time.Second},
			contracts.RecoveryWait_EXIT_REASON_FAILED, "applied_still_matching", true, 0, 5 * time.Second},
		// Recovery is not credited to a program whose diagnosis still matches.
		{"program blind to its own repair", settlingCase{healAfter: 40 * time.Second, stallWindow: 60 * time.Second, blind: true},
			contracts.RecoveryWait_EXIT_REASON_CLEARED, "applied_still_matching", false, 40 * time.Second, 45 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stage, state, clock, world := settlingStage(t, tc.settlingCase)
			started := clock.Now()
			agent := &recordingStage{}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
				t.Fatal(err)
			}
			report := state.PriorStages[0]
			if len(report.Mitigations) != 1 || !report.Mitigations[0].GetApplyReceipt().GetApplied() || world.restarts.Load() != 1 {
				t.Fatalf("the stage must apply one restart (writes=%d): %v", world.restarts.Load(), report)
			}
			attempt := report.Mitigations[0]
			if got := report.GetRecoveryWait().GetExitReason(); got != tc.wantExit {
				t.Fatalf("recovery wait exit = %s after %s, want %s: %v", got, clock.Now().Sub(started), tc.wantExit, report)
			}
			if waited := clock.Now().Sub(started); waited < tc.minWait || waited > tc.maxWait {
				t.Fatalf("the stage observed for %s, want %s to %s", waited, tc.minWait, tc.maxWait)
			}
			if attempt.Result != tc.wantResult {
				t.Fatalf("attempt result = %s, want %s: %v", attempt.Result, tc.wantResult, attempt)
			}
			if agent.dispatched != tc.wantDispatched {
				t.Fatalf("responder dispatched = %v, want %v (remaining %v)", agent.dispatched, tc.wantDispatched, report.RemainingFindingFingerprints)
			}
		})
	}
}
