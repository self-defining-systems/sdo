package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Simulated costs in the bubble: one rollout sample, and one detector round
// (the verify a stage apply used to run after every plan).
const (
	batchRolloutSample = time.Second
	batchDetectorRound = 2 * time.Second
)

// batchWorld is an application with one stalled Deployment per fault. A
// Deployment recovers when it is restarted; a data fault (a bad cache entry
// in a Pod) recovers when its flush command runs. It records the order of
// every write.
type batchWorld struct {
	client *fake.Clientset
	mu     sync.Mutex
	writes []string
	// flushed is the set of Pods whose data repair ran.
	flushed map[string]bool
	// dirty is the set of Pods holding bad data.
	dirty map[string]bool
	// sleep is the bubble's clock.
	sleep func(time.Duration)
}

func newBatchWorld(t *testing.T, names []string, failPatch string) *batchWorld {
	t.Helper()
	replicas, controller := int32(1), true
	var objects []kruntime.Object
	for _, name := range names {
		labels := map[string]string{"app": name}
		objects = append(objects,
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("synthetic-deployment-" + name), Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":v1"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: "app", UID: types.UID("synthetic-pod-" + name), Labels: labels,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: name, UID: types.UID("synthetic-deployment-" + name), Controller: &controller}}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":v1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	}
	w := &batchWorld{client: fake.NewSimpleClientset(objects...), flushed: map[string]bool{}, dirty: map[string]bool{}}
	w.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() != "" || (action.GetVerb() != "update" && action.GetVerb() != "patch") {
			return false, nil, nil
		}
		name := ""
		switch a := action.(type) {
		case ktesting.PatchAction:
			name = a.GetName()
		case ktesting.UpdateAction:
			name = a.GetObject().(metav1.Object).GetName()
		}
		if name == failPatch {
			return true, nil, errors.New("synthetic admission webhook refused the write")
		}
		w.mu.Lock()
		w.writes = append(w.writes, "restart "+name)
		w.mu.Unlock()
		return false, nil, nil
	})
	return w
}

func (w *batchWorld) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	deployments, err := w.client.AppsV1().Deployments("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods, err := w.client.CoreV1().Pods("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return sdktest.Snapshot{NamespaceName: "app", DeploymentList: deployments.Items, PodList: pods.Items}, nil
}

// Observe plays the deployment controller: each sample takes a second and
// finds the rollout complete.
func (w *batchWorld) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	w.sleep(batchRolloutSample)
	for _, ref := range refs {
		if ref.Kind != "Deployment" {
			continue
		}
		deployment, err := w.client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
		if _, err := w.client.AppsV1().Deployments(ref.Namespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (w *batchWorld) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return w.Observe(ctx, refs)
}

func (w *batchWorld) AcceptedManifest(ctx context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return TypedPlanClient{Client: w.client}.Get(ctx, ref)
}

func (w *batchWorld) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, nil
}

// RunOperation runs a Pod command: "flush" repairs the Pod's data, "probe"
// exits 0 once it is clean.
func (w *batchWorld) RunOperation(_ context.Context, op program.ExternalOperation) (int32, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	pod := op.Exec.Pod.Name
	if op.Exec.Args[0] == "flush" {
		w.writes = append(w.writes, "flush "+pod)
		w.flushed[pod] = true
		return 0, nil
	}
	if w.dirty[pod] && !w.flushed[pod] {
		return 1, nil
	}
	return 0, nil
}

func (w *batchWorld) dataDirty(pod string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dirty[pod] && !w.flushed[pod]
}

func (w *batchWorld) writeOrder() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.writes)
}

type batchClient struct {
	TypedPlanClient
	world *batchWorld
}

func (c batchClient) RunOperation(ctx context.Context, op program.ExternalOperation) (int32, error) {
	return c.world.RunOperation(ctx, op)
}

func batchRestarted(deployment appsv1.Deployment) bool {
	return deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] != ""
}

// batchDetector is the user-facing health check: each stalled Deployment and
// each Pod holding bad data is one failing finding, naming its playbook. One
// round costs batchDetectorRound.
type batchDetector struct {
	world     *batchWorld
	playbooks map[string][]string // Deployment name -> playbooks
	// restore names the Deployments whose fault a restart fixes.
	restore map[string]bool
	rounds  *atomic.Int32
}

func (d batchDetector) Spec() sdk.DetectorSpec {
	var ids []string
	for _, list := range d.playbooks {
		ids = append(ids, list...)
	}
	slices.Sort(ids)
	return sdk.DetectorSpec{ID: "workload-health", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: ids, OriginatingCommit: "seed"}
}

func (d batchDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	d.rounds.Add(1)
	d.world.sleep(batchDetectorRound)
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		ids, ok := d.playbooks[deployment.Name]
		if !ok {
			continue
		}
		stalled := d.restore[deployment.Name] && !batchRestarted(deployment) || d.world.dataDirty(deployment.Name+"-pod")
		if stalled {
			findings = append(findings, sdk.Finding{DetectorID: "workload-health", RuleID: "request-stall", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
				PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "requests stall", Evidence: "synthetic stall", Playbooks: ids})
		}
	}
	return findings, nil
}

// targetRestart restarts its one Deployment. A blind one cannot see its own
// restart, so its Diagnose keeps matching.
type targetRestart struct {
	id, target string
	blind      bool
}

func (p targetRestart) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: ComponentRole, Kind: "Deployment", Required: true}}}
}

func (p targetRestart) Diagnose(_ context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == p.target && b.Objects[ComponentRole].Name == p.target && (p.blind || !batchRestarted(deployment)) {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p targetRestart) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: b.Objects[ComponentRole], Reason: p.id + " restarts the stalled service"}}}, nil
}

// targetFlush repairs its Deployment's bad cache entry with a Pod command: a
// data repair that writes no Kubernetes object.
type targetFlush struct {
	id, target string
	world      *batchWorld
}

func (p targetFlush) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: ComponentRole, Kind: "Deployment", Required: true}}}
}

func (p targetFlush) Diagnose(_ context.Context, _ program.ReadView, b program.Bindings) (program.Verdict, error) {
	if b.Objects[ComponentRole].Name == p.target && p.world.dataDirty(p.target+"-pod") {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p targetFlush) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: p.target + "-pod"}
	return program.Plan{Actions: []program.Action{program.EnsureExec{
		Observe: program.Command{Pod: pod, Container: p.target, Args: []string{"probe"}},
		Ensure:  program.Command{Pod: pod, Container: p.target, Args: []string{"flush"}},
		Expect:  0, Reason: "flush the bad cache entry"}}}, nil
}

type batchStage struct {
	world  *batchWorld
	now    func() time.Time
	loop   *ResponderLoop
	state  *IncidentState
	stage  *DeterministicResponseStage
	rounds *atomic.Int32
}

// newBatchStage builds an incident in which every program in programs binds
// one open finding. names are the Deployments; dirty are those whose Pod
// holds bad data instead of a stall. now and sleep are the bubble's clock.
func newBatchStage(t *testing.T, now func() time.Time, sleep func(time.Duration), names []string, dirty []string, failPatch string, programs func(*batchWorld) []program.Diagnoser) *batchStage {
	t.Helper()
	world := newBatchWorld(t, names, failPatch)
	world.sleep = sleep
	for _, name := range dirty {
		world.dirty[name+"-pod"] = true
	}
	diagnosers := programs(world)
	var mitigators []program.Mitigator
	playbooks, restore := map[string][]string{}, map[string]bool{}
	for _, d := range diagnosers {
		mitigators = append(mitigators, d.(program.Mitigator))
		switch p := d.(type) {
		case targetRestart:
			playbooks[p.target] = append(playbooks[p.target], p.id)
			restore[p.target] = true
		case targetFlush:
			playbooks[p.target] = append(playbooks[p.target], p.id)
		}
	}
	registry, err := program.NewRegistry(diagnosers, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	rounds := &atomic.Int32{}
	detector := batchDetector{world: world, playbooks: playbooks, restore: restore, rounds: rounds}
	snapshot, err := world.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != len(playbooks) {
		t.Fatalf("every fault must be detected: %v", findings)
	}
	request := IncidentRequest{IncidentID: "synthetic-batch", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings,
		OpeningKeys: findingKeys(findings), RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: batchClient{TypedPlanClient: TypedPlanClient{Client: world.client}, world: world}, Source: world, Rollouts: world,
		Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: world}}
	loop := NewResponderLoop(request, registry, world, []sdk.Detector{detector}, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	rounds.Store(0)
	return &batchStage{world: world, now: now, loop: loop, state: &IncidentState{Request: request, Findings: findings}, stage: stage, rounds: rounds}
}

func (b *batchStage) run(t *testing.T) (*contracts.StageReport, time.Duration) {
	t.Helper()
	started := b.now()
	report, err := b.stage.Run(t.Context(), b.state)
	if err != nil {
		t.Fatal(err)
	}
	return report, b.now().Sub(started)
}

// settle runs recovery settlement as the controller does once the alert set
// cleared and stayed clear.
func (b *batchStage) settle(t *testing.T, report *contracts.StageReport) *contracts.StageReport {
	t.Helper()
	at := b.loop.clock()
	markAutomaticRecovery(report, at, contracts.RecoveryWait_EXIT_REASON_CLEARED)
	_, settled := b.loop.SettleRecoveryApplications(t.Context(), IncidentResult{}, []*contracts.StageReport{report}, at)
	return settled[0]
}

func attemptFor(report *contracts.StageReport, id string) *contracts.StageMitigation {
	for _, attempt := range report.GetMitigations() {
		if attempt.GetPlaybookId() == id {
			return attempt
		}
	}
	return nil
}

func restarts(names ...string) func(*batchWorld) []program.Diagnoser {
	return func(*batchWorld) []program.Diagnoser {
		var programs []program.Diagnoser
		for _, name := range names {
			programs = append(programs, targetRestart{id: "fix-" + name, target: name})
		}
		return programs
	}
}

// Five learned playbooks bind five disjoint faults. The stage applies them
// as one batch: five applications back to back, then one verify (one
// detector round), so the stage takes the applies plus one verify rather
// than an apply and a verify per plan. Each plan keeps its own application.
func TestDeterministicStageAppliesBoundPlansAsOneBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		names := []string{"a", "b", "c", "d", "e"}
		b := newBatchStage(t, time.Now, time.Sleep, names, nil, "", restarts(names...))
		report, took := b.run(t)
		applications := map[string]bool{}
		for _, name := range names {
			attempt := attemptFor(report, "fix-"+name)
			if attempt == nil || !attempt.GetApplyReceipt().GetApplied() || attempt.GetApplyReceipt().GetRolledBack() {
				t.Fatalf("fix-%s must apply: %v", name, attempt)
			}
			if got := attempt.GetPostApplyDiagnose().GetVerdict().GetResult(); got != contracts.Verdict_RESULT_NO_MATCH {
				t.Fatalf("fix-%s must carry its own post-apply Diagnose, got %v", name, got)
			}
			applications[attempt.GetApplyReceipt().GetApplicationId()] = true
		}
		if len(applications) != len(names) {
			t.Fatalf("each plan must keep its own application, got %d for %d plans", len(applications), len(names))
		}
		if got := b.rounds.Load(); got != 1 {
			t.Fatalf("the batch ran %d detector rounds, want one verify for the whole batch", got)
		}
		serial := time.Duration(len(names)) * (batchRolloutSample + batchDetectorRound)
		if want := time.Duration(len(names))*batchRolloutSample + batchDetectorRound; took != want {
			t.Fatalf("the stage took %s, want the applies plus one verify, %s (serial verify after each apply: %s)", took, want, serial)
		}
		settled := b.settle(t, report)
		for _, name := range names {
			attempt := attemptFor(settled, "fix-"+name)
			if attempt.GetResult() != "applied_cleared" || attempt.GetApplyReceipt().GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
				t.Fatalf("fix-%s cleared its own Diagnose and must be credited: result=%s rejection=%s", name, attempt.GetResult(), attempt.GetApplyReceipt().GetSettlementRejectionReason())
			}
		}
	})
}

// A plan is credited only when its own Diagnose clears, never for sharing a
// batch whose alerts cleared: the blind playbook still matches after its
// write, so it earns nothing, and the two that cleared are each credited.
func TestDeterministicStageCreditsEachBatchedPlanOnItsOwnDiagnose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBatchStage(t, time.Now, time.Sleep, []string{"a", "b", "c"}, nil, "", func(*batchWorld) []program.Diagnoser {
			return []program.Diagnoser{targetRestart{id: "fix-a", target: "a"}, targetRestart{id: "fix-b", target: "b", blind: true}, targetRestart{id: "fix-c", target: "c"}}
		})
		report, _ := b.run(t)
		if got := b.rounds.Load(); got != 1 {
			t.Fatalf("the batch ran %d detector rounds, want one", got)
		}
		settled := b.settle(t, report)
		for _, id := range []string{"fix-a", "fix-c"} {
			attempt := attemptFor(settled, id)
			if attempt.GetResult() != "applied_cleared" || attempt.GetApplyReceipt().GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
				t.Fatalf("%s must be credited on its own Diagnose: result=%s rejection=%s", id, attempt.GetResult(), attempt.GetApplyReceipt().GetSettlementRejectionReason())
			}
		}
		blind := attemptFor(settled, "fix-b")
		if blind.GetResult() == "applied_cleared" || blind.GetApplyReceipt().GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_DIAGNOSIS_NOT_CLEAR {
			t.Fatalf("the blind plan's Diagnose still matches; it must not be credited: result=%s rejection=%s", blind.GetResult(), blind.GetApplyReceipt().GetSettlementRejectionReason())
		}
	})
}

// A write refused part-way through the batch fails only its own
// application: the plans before and after it stay applied (one application
// per plan, so nothing compensates them) and are credited on their own
// Diagnose.
func TestDeterministicStageBatchPartialFailureKeepsTheOtherPlans(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBatchStage(t, time.Now, time.Sleep, []string{"a", "b", "c"}, nil, "b", restarts("a", "b", "c"))
		report, _ := b.run(t)
		failed := attemptFor(report, "fix-b")
		if failed.GetResult() != "apply_error" || failed.GetApplyReceipt().GetApplied() {
			t.Fatalf("the refused write must fail its own attempt: %v", failed)
		}
		if got := b.world.writeOrder(); !slices.Equal(got, []string{"restart a", "restart c"}) {
			t.Fatalf("writes = %v, want a and c restarted and b refused", got)
		}
		settled := b.settle(t, report)
		for _, id := range []string{"fix-a", "fix-c"} {
			attempt := attemptFor(settled, id)
			if attempt.GetApplyReceipt().GetRolledBack() || attempt.GetResult() != "applied_cleared" {
				t.Fatalf("%s must stay applied and be credited on its own Diagnose: result=%s receipt=%v", id, attempt.GetResult(), attempt.GetApplyReceipt())
			}
		}
	})
}

// Source restores apply before data repairs across the whole batch, even
// when the data repair is on another object and comes first in match order:
// a flush must not run against a service whose restore has not happened.
func TestDeterministicStageRestoresSourceBeforeDataRepairs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBatchStage(t, time.Now, time.Sleep, []string{"a", "b"}, []string{"a"}, "", func(w *batchWorld) []program.Diagnoser {
			return []program.Diagnoser{targetFlush{id: "a-flush", target: "a", world: w}, targetRestart{id: "z-restore", target: "b"}}
		})
		report, _ := b.run(t)
		var order []string
		for _, attempt := range report.GetMitigations() {
			order = append(order, attempt.GetPlaybookId())
		}
		if !slices.Equal(order, []string{"a-flush", "z-restore"}) {
			t.Fatalf("match order = %v, want the data repair first so the test shows reordering", order)
		}
		if got := b.world.writeOrder(); !slices.Equal(got, []string{"restart b", "flush a-pod"}) {
			t.Fatalf("writes = %v, want the restore before the data repair", got)
		}
		for _, id := range order {
			if !attemptFor(report, id).GetApplyReceipt().GetApplied() {
				t.Fatalf("%s must apply: %v", id, attemptFor(report, id))
			}
		}
	})
}

// Two applications of one stage batch both verified: each earns its credited
// pair. Before batching only a single application could be attributed.
func TestProgramCreditIndexCreditsEachVerifiedStageApplication(t *testing.T) {
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(time.Unix(1_000_000+int64(seconds), 0))
	}
	component := func(name string) []*contracts.Binding {
		return []*contracts.Binding{{Name: ComponentRole, Object: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}}}
	}
	application := func(id, playbook string, completed int, passed bool) *contracts.PlanApplication {
		app := &contracts.PlanApplication{ApplicationId: id, ProgramIds: []string{playbook}, Applied: true, StartedAt: at(completed - 1), CompletedAt: at(completed),
			VerificationCompleted: true, VerificationPassed: passed, FinalVerificationBasis: "recovery-settled",
			ObjectEffects: []*contracts.PlanObjectEffect{{State: "applied"}}}
		if !passed {
			app.SettlementRejectionReason = contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_DIAGNOSIS_NOT_CLEAR
		}
		return app
	}
	outcome := func(passed ...bool) *contracts.OutcomeRecord {
		apps := []*contracts.PlanApplication{application("app-1", "fix-a", 10, passed[0]), application("app-2", "fix-b", 11, passed[1]), application("app-3", "fix-c", 12, passed[2])}
		report := &contracts.StageReport{Stage: "deterministic", ApplyCompletedAt: at(12), RecoveryWait: &contracts.RecoveryWait{ExitReason: contracts.RecoveryWait_EXIT_REASON_CLEARED}}
		for i, name := range []string{"a", "b", "c"} {
			report.Mitigations = append(report.Mitigations, &contracts.StageMitigation{PlaybookId: "fix-" + name, Bindings: component(name), ApplyReceipt: apps[i], Result: "applied_cleared"})
		}
		return &contracts.OutcomeRecord{Classification: "success", PlanApplications: apps, PriorStages: []*contracts.StageReport{report},
			RecoveryVerification: &contracts.RecoveryVerification{OpeningKeys: []string{"workload-health/request-stall"}, WritesSettled: true},
			Timestamps:           &contracts.OutcomeTimestamps{VerifiedAt: at(40)}}
	}
	credits := ProgramCreditIndex([]*contracts.OutcomeRecord{outcome(true, false, true)})
	if !credits.Has("fix-a") || !credits.Has("fix-c") || credits.Has("fix-b") {
		t.Fatalf("credits = %v, want fix-a and fix-c (each verified on its own) and not fix-b", credits)
	}
	// The last application failing does not decide the others' credit.
	credits = ProgramCreditIndex([]*contracts.OutcomeRecord{outcome(true, true, false)})
	if !credits.Has("fix-a") || !credits.Has("fix-b") || credits.Has("fix-c") {
		t.Fatalf("credits = %v, want fix-a and fix-b although the last application failed", credits)
	}
}

// A data repair on a Pod whose Deployment the same turn restarts waits for
// the next turn, even when it comes first in match order: the restart goes
// first, and the repair is diagnosed and planned again on the Pods the
// restart left, never run against the ones it replaces.
func TestDeterministicStageDefersADataRepairOnAWorkloadTheTurnRestores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBatchStage(t, time.Now, time.Sleep, []string{"a"}, []string{"a"}, "", func(w *batchWorld) []program.Diagnoser {
			return []program.Diagnoser{targetFlush{id: "a-flush", target: "a", world: w}, targetRestart{id: "z-restore", target: "a"}}
		})
		report, _ := b.run(t)
		if got := b.world.writeOrder(); !slices.Equal(got, []string{"restart a", "flush a-pod"}) {
			t.Fatalf("writes = %v, want the restart, then the repair in the next turn", got)
		}
		if got := b.rounds.Load(); got != 2 {
			t.Fatalf("detector rounds = %d, want one verify per turn", got)
		}
		for _, id := range []string{"a-flush", "z-restore"} {
			if attempt := attemptFor(report, id); !attempt.GetApplyReceipt().GetApplied() || attempt.GetSkipReason() != "" {
				t.Fatalf("%s must apply: %v", id, attempt)
			}
		}
	})
}

// A restart that lands after the batch's first write but before the others
// redoes the planned ones it had not applied, and never repeats the write
// that began.
func TestDeterministicStageRestartMidBatchRedoesThePlansNotApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBatchStage(t, time.Now, time.Sleep, []string{"a", "b", "c"}, nil, "", restarts("a", "b", "c"))
		var saved [][]byte
		b.loop.saveToolState = func(_ context.Context, payload []byte) error {
			saved = append(saved, slices.Clone(payload))
			return nil
		}
		b.run(t)
		// The journal as a controller that died during the first apply's rollout left it.
		var checkpoint []byte
		for _, payload := range saved {
			var tool responderToolState
			if err := json.Unmarshal(payload, &tool); err != nil {
				t.Fatal(err)
			}
			if len(tool.ApplicationEvidence) != 1 {
				continue
			}
			for _, progress := range tool.StageProgress {
				planned := 0
				for _, attempt := range progress.Report.GetMitigations() {
					if attempt.GetSkipReason() == stagePlannedInTurn {
						planned++
					}
				}
				if planned == 2 && checkpoint == nil {
					checkpoint = payload
				}
			}
		}
		if checkpoint == nil {
			t.Fatalf("no journal entry with one write and two planned bindings (%d saves)", len(saved))
		}
		resumed := newBatchStage(t, time.Now, time.Sleep, []string{"a", "b", "c"}, nil, "", restarts("a", "b", "c"))
		if err := resumed.loop.restoreToolSnapshot(checkpoint); err != nil {
			t.Fatal(err)
		}
		report, _ := resumed.run(t)
		if got := resumed.world.writeOrder(); !slices.Equal(got, []string{"restart b", "restart c"}) {
			t.Fatalf("resumed writes = %v, want the two planned restarts and no repeat of the first", got)
		}
		for _, id := range []string{"fix-b", "fix-c"} {
			if !attemptFor(report, id).GetApplyReceipt().GetApplied() {
				t.Fatalf("%s must apply after the restart: %v", id, attemptFor(report, id))
			}
		}
	})
}
