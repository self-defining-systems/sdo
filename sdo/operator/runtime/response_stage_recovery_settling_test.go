package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
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
	if d.class == sdk.DetectorClassLearned {
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
	// stallWindow is the learned detector's window; zero means it
	// judges current state only.
	stallWindow time.Duration
	blind       bool
	// unknownDiagnoses is how many stage diagnoses fail as an unanswered read.
	unknownDiagnoses int32
	// unknownAtOpen starts those failures at the incident's opening diagnosis
	// instead of after it.
	unknownAtOpen bool
	// undecided makes the program answer Unknown itself on every diagnosis.
	undecided bool
	// traffic replaces the learned stall detector and program with ones that
	// read the "health" workload's samples, as learned memory does.
	traffic *settlingTraffic
	// storeRestart is how long, after the write, a health detector's store
	// read finds the restarted store's container not running; zero never.
	storeRestart time.Duration
	// churnFrom and churnUntil bound a critical health finding on
	// Deployment/web, a component no stage write touches, measured from the
	// start; zero churnUntil adds none. The incident's accepted map then
	// names web and api as separate components.
	churnFrom, churnUntil time.Duration
	// churnJoined makes the churn alert a user test that joined the incident's
	// alert set instead of a clue.
	churnJoined bool
	// unexplainedDrift adds an event-sourced drift.unexplained finding on an
	// object no stage write touches and no detector re-observes: a clue that
	// can never clear by detector evaluation.
	unexplainedDrift bool
	// startup is how long the api workload was observed to take to become
	// Ready; its restart settles only that long after the write.
	startup time.Duration
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
	startup time.Duration
	// effectAt is when the restart's effect reaches the samples.
	effectAt time.Time
	// saved collects the controller's tool-state checkpoints when set.
	saved *[][]byte
}

func (w *settlingWorld) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := w.sameWriteWorld.Snapshot(ctx)
	if err != nil {
		return view, err
	}
	snapshot := view.(sdktest.Snapshot)
	if w.startup > 0 {
		snapshot.ObservedStartupTimes = map[string]time.Duration{"Deployment/api": w.startup}
	}
	if w.traffic == nil {
		return snapshot, nil
	}
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

// settlingStage builds the R3b shape: a learned detector reports a
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
	if tc.churnUntil > 0 {
		// A second component no stage write touches.
		web := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app", UID: "synthetic-web", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web:v1"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1}}
		if _, err := inner.client.AppsV1().Deployments("app").Create(context.Background(), web, metav1.CreateOptions{}); err != nil {
			panic(err)
		}
	}
	inner.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			inner.restarts.Add(1)
		}
		return false, nil, nil
	})
	// The recovery bound is also a real context deadline, so the synthetic
	// clock starts far enough ahead that only the synthetic bound can end it.
	clock := &settlingClock{now: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
	return &settlingWorld{sameWriteWorld: inner, clock: clock, traffic: tc.traffic, startup: tc.startup, effectAt: clock.Now().Add(time.Second)}
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
	stall := settlingDetector{id: "api-request-stall", class: sdk.DetectorClassLearned, window: tc.stallWindow, playbooks: []string{"api-request-stall"}, clock: clock, healAt: healAt}
	var stallDetector sdk.Detector = stall
	if tc.stallWindow > 0 {
		stallDetector = windowedSettlingDetector{stall}
	}
	var restart interface {
		program.Diagnoser
		program.Mitigator
	} = restartProgram{id: "api-request-stall", blind: tc.blind, unknown: unknownCounter(tc.unknownDiagnoses), answered: unknownCounter(1)}
	if tc.unknownAtOpen {
		restart = restartProgram{id: "api-request-stall", blind: tc.blind, unknown: unknownCounter(tc.unknownDiagnoses), answered: &atomic.Int32{}}
	}
	if tc.undecided {
		restart = restartProgram{id: "api-request-stall", undecided: true}
	}
	if tc.traffic != nil {
		stallDetector = trafficLearnedStall{stall}
		restart = trafficStallProgram{restartProgram{id: "api-request-stall"}}
	}
	detectors := []sdk.Detector{traffic, stallDetector}
	if tc.storeRestart > 0 {
		detectors = append(detectors, restartingStoreDetector{world: world, until: clock.Now().Add(tc.storeRestart)})
	}
	if tc.churnUntil > 0 {
		detectors = append(detectors, churnDetector{clock: clock, from: clock.Now().Add(tc.churnFrom), until: clock.Now().Add(tc.churnUntil)})
	}
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
	if tc.unexplainedDrift {
		findings = append(findings, sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, Severity: sdk.SeverityLatent, Summary: "unexplained drift", PrimaryResource: sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "synthetic-policy"}})
	}
	sortFindings(findings)
	request := IncidentRequest{IncidentID: "synthetic-settling", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, RepairPolicy: "recorded-actions"}
	if tc.churnUntil > 0 {
		// The churn detector's alert is a clue, never an opening alert.
		for _, finding := range findings {
			switch {
			case finding.DetectorID != "workload-health":
				request.OpeningKeys = append(request.OpeningKeys, findingKey(finding))
			case tc.churnJoined:
				request.JoinedKeys = append(request.JoinedKeys, findingKey(finding))
			}
		}
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	if tc.churnUntil > 0 {
		loop.GitRead = fakeArchitecture(churnArch)
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	if world.saved != nil {
		loop.saveToolState = func(_ context.Context, payload []byte) error {
			*world.saved = append(*world.saved, append([]byte(nil), payload...))
			return nil
		}
	}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		diagnoseWait: clock.advance}
	return stage, &IncidentState{Request: request, Findings: findings}
}

// trafficStallDetector is a learned detector that consumes the health
// workload without specifying an evaluation window of its own.
type trafficStallDetector struct{ settlingDetector }

func (trafficStallDetector) TrafficWorkloads() []string { return []string{"health"} }

func unknownCounter(n int32) *atomic.Int32 {
	if n == 0 {
		return nil
	}
	c := &atomic.Int32{}
	c.Store(n)
	return c
}

// A diagnosis that fails because a read did not answer is asked again; a
// playbook that matches on the next try is applied, not skipped.
func TestStageRetriesADiagnosisThatDidNotAnswer(t *testing.T) {
	stage, state, _, world := settlingStage(t, settlingCase{healAfter: 20 * time.Second, unknownDiagnoses: stageDiagnoseTries - 1})
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, &recordingStage{}}); err != nil {
		t.Fatal(err)
	}
	if stageDiagnoseTries != 3 {
		t.Fatalf("the stage asks %d times, want 3", stageDiagnoseTries)
	}
	if world.restarts.Load() != 1 {
		t.Fatalf("the stage must apply the playbook after the read answers: %v", state.PriorStages[0])
	}
}

// A diagnosis that never answers skips the playbook and says why.
func TestStageRecordsWhyADiagnosisStayedUnknown(t *testing.T) {
	stage, state, _, world := settlingStage(t, settlingCase{unknownDiagnoses: 100})
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, &recordingStage{}}); err != nil {
		t.Fatal(err)
	}
	attempt := state.PriorStages[0].Mitigations[0]
	if world.restarts.Load() != 0 || attempt.SkipReason != "diagnose_unavailable" || !strings.Contains(attempt.Error, "memory host unavailable") {
		t.Fatalf("skip %q error %q writes %d", attempt.SkipReason, attempt.Error, world.restarts.Load())
	}
}

// The opening diagnosis is the one the stage's candidates come from: when its
// read did not answer (a slow exec or a busy memory host at incident open), the
// stage asks again within its bound instead of exiting with nothing applied.
func TestStageRetriesAnOpeningDiagnosisThatDidNotAnswer(t *testing.T) {
	stage, state, _, world := settlingStage(t, settlingCase{healAfter: 20 * time.Second, unknownDiagnoses: 2, unknownAtOpen: true})
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, &recordingStage{}}); err != nil {
		t.Fatal(err)
	}
	if world.restarts.Load() != 1 {
		t.Fatalf("the stage must apply the playbook once the opening read answers: %v", state.PriorStages[0])
	}
	unknown := 0
	for _, run := range state.Request.InitialProgramRuns {
		if run.ProgramID == "api-request-stall" && run.Verdict.Result == program.Unknown {
			unknown++
		}
	}
	if unknown == 0 {
		t.Fatalf("the unanswered opening diagnoses must stay as evidence: %+v", state.Request.InitialProgramRuns)
	}
}

// An opening diagnosis that never answers leaves the stage with nothing to
// apply, after a bounded number of tries.
func TestStageGivesUpAnOpeningDiagnosisThatNeverAnswers(t *testing.T) {
	stage, state, _, world := settlingStage(t, settlingCase{unknownDiagnoses: 100, unknownAtOpen: true})
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	// Each diagnosis is one run per finding it binds through; count one finding's.
	runs := 0
	for _, run := range state.Request.InitialProgramRuns {
		if run.ProgramID == "api-request-stall" && run.FindingFingerprint == state.Request.InitialProgramRuns[0].FindingFingerprint {
			runs++
		}
	}
	if world.restarts.Load() != 0 || !agent.dispatched || runs != stageDiagnoseTries {
		t.Fatalf("writes=%d agent=%v opening runs=%d, want 0, true, %d", world.restarts.Load(), agent.dispatched, runs, stageDiagnoseTries)
	}
}

// An Unknown the program answered itself, with every read answered, is a
// diagnosis even when it explains itself: the stage does not ask again, and
// the agent is not delayed.
func TestStageDoesNotReaskAProgramsOwnUnknown(t *testing.T) {
	stage, state, _, world := settlingStage(t, settlingCase{undecided: true})
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, &recordingStage{}}); err != nil {
		t.Fatal(err)
	}
	runs := 0
	for _, run := range state.Request.InitialProgramRuns {
		if run.ProgramID == "api-request-stall" && run.FindingFingerprint == state.Request.InitialProgramRuns[0].FindingFingerprint {
			runs++
		}
	}
	if world.restarts.Load() != 0 || runs != 1 {
		t.Fatalf("writes=%d opening runs=%d, want 0 and 1", world.restarts.Load(), runs)
	}
}

func TestAutomaticStagePublishesBeforeObservingRecovery(t *testing.T) {
	stage, state, clock, world := settlingStage(t, settlingCase{healAfter: time.Minute, stallWindow: time.Minute})
	start := clock.Now()
	report, err := stage.Run(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if world.restarts.Load() != 1 || !report.GetMitigations()[0].GetApplyReceipt().GetApplied() {
		t.Fatalf("stage did not publish its only write: %v", report)
	}
	if !clock.Now().Equal(start) {
		t.Fatalf("stage ran a recovery wait for %s before publishing receipts", clock.Now().Sub(start))
	}
	if report.GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatal("the stage made a recovery decision")
	}
}

func TestAutomaticStageRecoversWriteBeforeReceiptWasPublished(t *testing.T) {
	tc := settlingCase{}
	world := newSettlingWorld(tc)
	var saved [][]byte
	world.saved = &saved
	stage, state := settlingStageOn(t, tc, world)
	if _, err := stage.Run(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	var checkpoint []byte
	for _, raw := range saved {
		var snapshot responderToolState
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		progress := snapshot.StageProgress["deterministic"]
		if len(snapshot.ApplicationEvidence) != 1 || len(progress.Report.GetMitigations()) != 1 || progress.Report.Mitigations[0].GetApplyReceipt() != nil {
			continue
		}
		var receipt contracts.PlanApplication
		if err := protojson.Unmarshal(snapshot.ApplicationEvidence[0].Receipt, &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.GetApplied() {
			checkpoint = raw
			break
		}
	}
	if checkpoint == nil {
		t.Fatal("missing checkpoint between executor publication and stage receipt")
	}
	want := world.clock.Now().Add(90 * time.Second)
	_ = world.clock.advance(t.Context(), 40*time.Second)
	world.saved = nil
	resumed, restored := settlingStageOn(t, tc, world)
	loop := resumed.NewLoop(restored.Request)
	if err := loop.restoreToolSnapshot(checkpoint); err != nil {
		t.Fatal(err)
	}
	report, err := resumed.Run(t.Context(), restored)
	if err != nil {
		t.Fatal(err)
	}
	if world.restarts.Load() != 1 || !playbookWrote(report) || !report.GetRecoveryDeadline().AsTime().Equal(want) {
		t.Fatalf("resumed stage lost the write or replayed it: %v, writes %d", report, world.restarts.Load())
	}
}
