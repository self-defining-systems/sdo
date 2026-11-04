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
	"sdo.dev/operator/sdk/traffic"
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
	// traffic replaces the learned stall detector and program with ones that
	// read the "health" workload's samples, as learned memory does.
	traffic *settlingTraffic
}

// settlingTraffic is the health workload's samples: one per second for scenario
// read, timing out until the restart takes effect, then OK, or none at all when
// the prober is starved (a loaded host, a burst of skipped arrivals).
type settlingTraffic struct {
	starved bool
}

// settlingWorld adds the health workload's traffic window to the same-write world.
type settlingWorld struct {
	*sameWriteWorld
	clock   *settlingClock
	traffic *settlingTraffic
	// effectAt is when the restart's effect reaches the samples.
	effectAt time.Time
	// saved collects the controller's tool-state checkpoints when set.
	saved *[][]byte
}

func (w *settlingWorld) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := w.sameWriteWorld.Snapshot(ctx)
	if err != nil || w.traffic == nil {
		return view, err
	}
	snapshot := view.(sdktest.Snapshot)
	now := w.clock.Now()
	var samples []traffic.Sample
	for at := now.Add(-time.Minute).Truncate(time.Second); !at.After(now); at = at.Add(time.Second) {
		switch {
		case at.Before(w.effectAt):
			samples = append(samples, traffic.Sample{At: at, Latency: 2 * time.Second, Outcome: traffic.OutcomeTimeout, Requests: 1})
		case !w.traffic.starved:
			samples = append(samples, traffic.Sample{At: at, Latency: 10 * time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK, Requests: 1})
		}
	}
	if len(samples) > traffic.DefaultWindow {
		samples = samples[len(samples)-traffic.DefaultWindow:]
	}
	snapshot.Traffic = map[string]traffic.Window{"health": {
		Workload:   traffic.Workload{Name: "health", Purpose: traffic.PurposeHealthProbe, RatePerSecond: 1, Scenarios: []traffic.WorkloadScenario{{ID: "read", Weight: 1}}},
		ObservedAt: now,
		Scenarios:  []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "read"}, Samples: samples, Qualified: true}},
	}}
	return snapshot, nil
}

// trafficLearnedStall is a learned stall detector as the memory host serves
// it: it reads the health workload's samples with the SDK's SLO judgement and
// declares neither a window nor a confirmation contract of its own.
type trafficLearnedStall struct{ settlingDetector }

func (trafficLearnedStall) TrafficWorkloads() []string { return []string{"health"} }

func (d trafficLearnedStall) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	window, ok := traffic.WindowFrom(view, "health")
	if !ok {
		return []sdk.Finding{}, nil
	}
	findings := []sdk.Finding{}
	for _, verdict := range traffic.Judge(window) {
		if verdict.Healthy {
			continue
		}
		for _, deployment := range view.Deployments() {
			findings = append(findings, sdk.Finding{DetectorID: d.id, RuleID: d.id, Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "requests to api time out", Evidence: "synthetic traffic", Playbooks: d.playbooks})
		}
	}
	return findings, nil
}

// trafficStallProgram restarts its Deployment and diagnoses the stall from the
// latest health sample, however old: it matches while the last request it saw
// timed out.
type trafficStallProgram struct{ restartProgram }

func (p trafficStallProgram) Diagnose(_ context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	window, ok := view.TrafficWindow("health")
	if !ok || len(window.Scenarios) == 0 || len(window.Scenarios[0].Samples) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	samples := window.Scenarios[0].Samples
	if samples[len(samples)-1].Outcome == traffic.OutcomeTimeout {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

// settlingStage builds the R3b shape: a learned incident detector reports a
// request stall on Deployment api and names the learned restart playbook, the
// judge's traffic-health detector reports the same journey unexplained, and
// both judge a rolling window. The restart's first rollout sample is still in
// progress; every later one has completed.
func settlingStage(t *testing.T, tc settlingCase) (*DeterministicResponseStage, *IncidentState, *settlingClock, *sameWriteWorld) {
	t.Helper()
	world := newSettlingWorld(tc)
	stage, state := settlingStageOn(t, tc, world)
	return stage, state, world.clock, world.sameWriteWorld
}

func newSettlingWorld(tc settlingCase) *settlingWorld {
	replicas := int32(1)
	inner := &sameWriteWorld{client: fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", UID: "synthetic-deployment", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "api:v1"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", UID: "synthetic-pod", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "api:v1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	)}
	inner.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			inner.restarts.Add(1)
		}
		return false, nil, nil
	})
	// The recovery bound is also a real context deadline, so the synthetic
	// clock starts far enough ahead that only the synthetic bound can end it.
	clock := &settlingClock{now: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
	return &settlingWorld{sameWriteWorld: inner, clock: clock, traffic: tc.traffic, effectAt: clock.Now().Add(time.Second)}
}

// settlingStageOn builds the stage and incident over world, as a controller
// (re)starting on that cluster would.
func settlingStageOn(t *testing.T, tc settlingCase, world *settlingWorld) (*DeterministicResponseStage, *IncidentState) {
	t.Helper()
	clock := world.clock
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
	var restart interface {
		program.Diagnoser
		program.Mitigator
	} = restartProgram{id: "api-request-stall", blind: tc.blind}
	if tc.traffic != nil {
		stallDetector = trafficLearnedStall{stall}
		restart = trafficStallProgram{restartProgram{id: "api-request-stall"}}
	}
	detectors := []sdk.Detector{traffic, stallDetector}
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
	if world.saved != nil {
		loop.saveToolState = func(_ context.Context, payload []byte) error {
			*world.saved = append(*world.saved, append([]byte(nil), payload...))
			return nil
		}
	}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		ObservationInterval: time.Second, recoveryNow: clock.Now, recoveryWait: clock.advance}
	return stage, &IncidentState{Request: request, Findings: findings}
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
		// Detector clearance does not outrank the applied program's own
		// diagnosis: a program still matching once the windows have rolled
		// over (61 s) hands the incident to the responder, uncredited.
		{"program blind to its own repair", settlingCase{healAfter: 40 * time.Second, stallWindow: 60 * time.Second, blind: true},
			contracts.RecoveryWait_EXIT_REASON_FAILED, "applied_still_matching", true, 61 * time.Second, 65 * time.Second},
		// Requests succeed again after the restart: fresh samples prove it.
		{"traffic recovers after the restart", settlingCase{healAfter: 20 * time.Second, traffic: &settlingTraffic{}},
			contracts.RecoveryWait_EXIT_REASON_CLEARED, "applied_cleared", false, 0, 25 * time.Second},
		// The prober is starved after the restart: the learned detector's window
		// empties and its finding goes quiet, while the program's latest sample
		// still times out. Silence is not recovery; the responder gets it.
		{"samples stop arriving after the restart", settlingCase{healAfter: 20 * time.Second, traffic: &settlingTraffic{starved: true}},
			contracts.RecoveryWait_EXIT_REASON_FAILED, "applied_still_matching", true, 28 * time.Second, 40 * time.Second},
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

// trafficStallDetector is a learned stall detector as the memory host serves
// it: it consumes a traffic workload but declares no window of its own.
type trafficStallDetector struct{ settlingDetector }

func (trafficStallDetector) TrafficWorkloads() []string { return []string{"health"} }

// A traffic consumer that declares no window still lags behind a repair by
// its workload's window, so the wait gives its findings that window; a
// detector that judges current state gets none.
func TestSettlingHorizonOfTrafficConsumers(t *testing.T) {
	view := sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {Workload: traffic.Workload{Name: "health", RatePerSecond: 3, Scenarios: []traffic.WorkloadScenario{{ID: "read", Weight: 1}}}}}}
	stall := settlingDetector{id: "learned-stall", class: sdk.DetectorClassIncident}
	if got := settlingHorizon(trafficStallDetector{stall}, view); got < traffic.DefaultMaxAge {
		t.Fatalf("traffic consumer horizon = %s, want at least the workload window %s", got, traffic.DefaultMaxAge)
	}
	if got := settlingHorizon(stall, view); got != 0 {
		t.Fatalf("current-state detector horizon = %s, want 0", got)
	}
}

// A controller restart mid-wait resumes the write's settling window from when
// its effect was first seen; it does not grant a fresh one.
func TestRecoveryWaitWindowSurvivesControllerRestart(t *testing.T) {
	tc := settlingCase{stallWindow: 60 * time.Second}
	world := newSettlingWorld(tc)
	var saved [][]byte
	world.saved = &saved
	stage, state := settlingStageOn(t, tc, world)
	started := world.clock.Now()
	// The controller dies 50 s into the wait.
	ctx, stop := context.WithCancel(context.Background())
	stage.recoveryWait = func(ctx context.Context, d time.Duration) error {
		if err := world.clock.advance(ctx, d); err != nil {
			return err
		}
		if world.clock.Now().Sub(started) >= 50*time.Second {
			stop()
			return context.Canceled
		}
		return nil
	}
	if _, err := stage.Run(ctx, state); err == nil {
		t.Fatal("the interrupted stage must report its cancellation")
	}
	if len(saved) == 0 {
		t.Fatal("no checkpoint before the restart")
	}
	world.saved = nil
	resumed, restored := settlingStageOn(t, tc, world)
	loop := resumed.NewLoop(restored.Request)
	if err := loop.restoreToolSnapshot(saved[len(saved)-1]); err != nil {
		t.Fatal(err)
	}
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), restored, []ResponseStage{resumed, agent}); err != nil {
		t.Fatal(err)
	}
	report := restored.PriorStages[0]
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_FAILED || !agent.dispatched {
		t.Fatalf("resumed wait exit = %s (dispatched %v), want FAILED to the responder: %v", got, agent.dispatched, report)
	}
	if waited := world.clock.Now().Sub(started); waited > 65*time.Second {
		t.Fatalf("the restart granted a fresh window: the wait ended %s after the write, want at most 65 s", waited)
	}
	if world.restarts.Load() != 1 {
		t.Fatalf("the resumed stage repeated the restart (writes=%d)", world.restarts.Load())
	}
}

// Positive evidence of recovery is fresh samples: enough of them, taken after
// the write took effect, within the scenario's SLO. Samples that predate the
// write or stopped arriving prove nothing. A learned traffic detector without
// its own confirmation contract is confirmed by the same samples, and its
// window bounds the closure recheck.
func TestTrafficRecoveryEvidenceNeedsFreshSamples(t *testing.T) {
	now := time.Date(2100, 1, 1, 0, 1, 0, 0, time.UTC)
	effect := now.Add(-10 * time.Second)
	view := func(at ...time.Duration) sdktest.Snapshot {
		var samples []traffic.Sample
		for _, offset := range at {
			samples = append(samples, traffic.Sample{At: now.Add(offset), Latency: 10 * time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK, Requests: 1})
		}
		return sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {
			Workload:   traffic.Workload{Name: "health", Purpose: traffic.PurposeHealthProbe, RatePerSecond: 1, Scenarios: []traffic.WorkloadScenario{{ID: "read", Weight: 1}}},
			ObservedAt: now,
			Scenarios:  []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "read"}, Samples: samples, Qualified: true}},
		}}}
	}
	learned := trafficLearnedStall{settlingDetector{id: "learned-stall", class: sdk.DetectorClassIncident}}
	for _, tc := range []struct {
		name      string
		view      sdktest.Snapshot
		evidence  bool
		confirmed bool
	}{
		{"fresh samples after the write", view(-6*time.Second, -4*time.Second, -2*time.Second), true, true},
		{"samples only before the write", view(-20*time.Second, -15*time.Second, -12*time.Second), false, true},
		{"too few samples after the write", view(-20*time.Second, -15*time.Second, -2*time.Second), false, true},
		{"samples stopped arriving", view(-50*time.Second, -45*time.Second, -40*time.Second), false, false},
		{"no observations", sdktest.Snapshot{NamespaceName: "app"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if reason := recoveryTrafficEvidence(learned, tc.view, effect); (reason == "") != tc.evidence {
				t.Fatalf("evidence = %q, want proven %v", reason, tc.evidence)
			}
			if got := detectorConfirmed(learned, tc.view); got != tc.confirmed {
				t.Fatalf("confirmed = %v (%s), want %v", got, detectorConfirmationReason(learned, tc.view), tc.confirmed)
			}
		})
	}
	if got := detectorClearingHorizon(learned, view()); got < traffic.DefaultMaxAge {
		t.Fatalf("closure bound of a learned traffic detector = %s, want at least its workload window %s", got, traffic.DefaultMaxAge)
	}
	current := settlingDetector{id: "current", class: sdk.DetectorClassIncident}
	if reason := recoveryTrafficEvidence(current, view(), effect); reason != "" || !detectorConfirmed(current, view()) {
		t.Fatalf("a detector that reads no traffic needs no traffic evidence: %q", reason)
	}
}
