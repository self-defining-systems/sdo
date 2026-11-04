package runtime

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// sameWriteWorld is one Deployment whose traffic stays unhealthy until the
// test says the restart's effect has reached it: a windowed health detector
// keeps firing after the rollout completes.
type sameWriteWorld struct {
	client   *fake.Clientset
	healthy  atomic.Bool
	observed atomic.Int32
	restarts atomic.Int32
	// stuck keeps rollouts from ever completing.
	stuck atomic.Bool
}

func (w *sameWriteWorld) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
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

// Observe plays the deployment controller: the first rollout sample is still
// in progress, every later one has completed.
func (w *sameWriteWorld) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if w.observed.Add(1) == 1 {
		return &RolloutPendingError{Pending: &contracts.RolloutPending{Reason: "synthetic rollout in progress"}}
	}
	for _, ref := range refs {
		if ref.Kind != "Deployment" || w.stuck.Load() {
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

func (w *sameWriteWorld) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return w.Observe(ctx, refs)
}

func (w *sameWriteWorld) AcceptedManifest(ctx context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return TypedPlanClient{Client: w.client}.Get(ctx, ref)
}

func (w *sameWriteWorld) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, nil
}

type sameWriteDetector struct{ world *sameWriteWorld }

func (sameWriteDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "traffic", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"aware-restart", "blind-restart"}, OriginatingCommit: "seed"}
}

func (d sameWriteDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.world.healthy.Load() {
		return []sdk.Finding{}, nil
	}
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		findings = append(findings, sdk.Finding{DetectorID: "traffic", RuleID: "request-stall", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "requests stall", Evidence: "synthetic stall", Playbooks: []string{"aware-restart", "blind-restart"}})
	}
	return findings, nil
}

// restartProgram restarts its bound Deployment. A blind program cannot see
// its restart (as one reading a field SDO normalizes away), so it keeps matching.
type restartProgram struct {
	id    string
	blind bool
}

func (p restartProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (p restartProgram) Diagnose(_ context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == b.Objects["deployment"].Name && (p.blind || deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "") {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p restartProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: b.Objects["deployment"], Reason: p.id + " restarts the stalled service"}}}, nil
}

func sameWriteLoop(t *testing.T) (*sameWriteWorld, *ResponderLoop, *IncidentState) {
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
	detector := sameWriteDetector{world: world}
	registry, err := program.NewRegistry(
		[]program.Diagnoser{restartProgram{id: "aware-restart"}, restartProgram{id: "blind-restart", blind: true}},
		[]program.Mitigator{restartProgram{id: "aware-restart"}, restartProgram{id: "blind-restart", blind: true}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := world.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "synthetic-same-write", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: world}}
	loop := NewResponderLoop(request, registry, world, []sdk.Detector{detector}, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	return world, loop, &IncidentState{Request: request, Findings: findings}
}

// Two programs bound to one Deployment plan the same restart. The second one
// cannot observe the first restart, so it still matches while that restart's
// effect is in progress. Repeating the write would supersede the first,
// correct application; the stage skips it, and the recovery that follows the
// first restart is credited to that restart.
func TestDeterministicStageSkipsSameWriteInProgress(t *testing.T) {
	ctx := context.Background()
	world, loop, state := sameWriteLoop(t)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		recoveryWait: func(context.Context, time.Duration) error { return nil }}
	report, err := stage.Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	attempts := map[string]*contracts.StageMitigation{}
	for _, attempt := range report.Mitigations {
		attempts[attempt.PlaybookId] = attempt
	}
	first, second := attempts["aware-restart"], attempts["blind-restart"]
	if first == nil || second == nil {
		t.Fatalf("both programs must be attempted: %v", report)
	}
	if !first.GetApplyReceipt().GetApplied() {
		t.Fatalf("the first restart must apply: %v", first)
	}
	if second.Result != "skipped" || second.SkipReason != stageAlreadyAttempted || second.GetApplyReceipt() != nil {
		t.Fatalf("the repeated restart must be skipped as %s: %v", stageAlreadyAttempted, second)
	}
	if got := world.restarts.Load(); got != 1 {
		t.Fatalf("the Deployment was written %d times, want one restart", got)
	}

	// The windowed detector re-observes later; closure settles what the stage applied.
	world.healthy.Store(true)
	_, settled := loop.SettleRecoveryApplications(ctx, IncidentResult{}, []*contracts.StageReport{report})
	for _, attempt := range settled[0].Mitigations {
		if attempt.PlaybookId != "aware-restart" {
			continue
		}
		receipt := attempt.GetApplyReceipt()
		if attempt.Result != "applied_cleared" || receipt.GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED || !receipt.GetVerificationPassed() {
			t.Fatalf("recovery after the first restart must be credited to it: result=%s rejection=%s receipt=%v", attempt.Result, receipt.GetSettlementRejectionReason(), receipt)
		}
		return
	}
	t.Fatalf("settlement lost the first restart: %v", settled)
}

// The skip survives a controller restart: the first restart's application is
// restored from the tool state journal (no bound inputs, only the archive), and
// the second program's identical restart is still skipped.
func TestDeterministicStageSkipsSameWriteAfterRestore(t *testing.T) {
	ctx := context.Background()
	_, first, state := sameWriteLoop(t)
	var saved [][]byte
	first.saveToolState = func(_ context.Context, payload []byte) error {
		saved = append(saved, append([]byte(nil), payload...))
		return nil
	}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return first }, KeepLoop: true,
		recoveryWait: func(context.Context, time.Duration) error { return nil }}
	if _, err := stage.Run(ctx, state); err != nil {
		t.Fatal(err)
	}
	// The journal as the controller left it when it died after the first apply.
	var checkpoint []byte
	for _, payload := range saved {
		var tool responderToolState
		if err := json.Unmarshal(payload, &tool); err != nil {
			t.Fatal(err)
		}
		if len(tool.ApplicationEvidence) == 1 {
			for _, progress := range tool.StageProgress {
				if mitigations := progress.Report.GetMitigations(); len(mitigations) == 1 && mitigations[0].GetResult() == "rollout_pending_at_handoff" {
					checkpoint = payload
				}
			}
		}
	}
	if checkpoint == nil {
		t.Fatalf("no journal entry between the two attempts (%d saves)", len(saved))
	}
	world, second, restored := sameWriteLoop(t)
	if err := second.restoreToolSnapshot(checkpoint); err != nil {
		t.Fatal(err)
	}
	resumed := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return second }, KeepLoop: true,
		recoveryWait: func(context.Context, time.Duration) error { return nil }}
	report, err := resumed.Run(ctx, restored)
	if err != nil {
		t.Fatal(err)
	}
	var skipped bool
	for _, attempt := range report.Mitigations {
		if attempt.PlaybookId == "blind-restart" {
			skipped = attempt.Result == "skipped" && attempt.SkipReason == stageAlreadyAttempted
		}
	}
	if !skipped || world.restarts.Load() != 0 {
		t.Fatalf("restored stage repeated the restart (writes=%d, skipped=%v): %v", world.restarts.Load(), skipped, report)
	}
}

// "In progress" is read from the cluster when the decision is made, not from
// the label recorded at apply. Past the recovery window, a Deployment whose
// rollout finished is not in progress, so a fault that came back is written
// again; one still rolling out stays in progress however old the write is.
func TestSameWriteInProgressIsReadLive(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rolledOut  bool
		later      time.Duration
		wantWrites int32
	}{
		{"window open", true, 0, 1},
		{"window closed, rollout finished", true, 2 * DefaultMitigationRolloutTimeout, 2},
		{"window closed, still rolling out", false, 2 * DefaultMitigationRolloutTimeout, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world, loop, state := sameWriteLoop(t)
			if !tc.rolledOut {
				deployment, err := world.client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				world.stuck.Store(true)
				deployment.Status.AvailableReplicas = 0
				if _, err := world.client.AppsV1().Deployments("app").UpdateStatus(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
			loop.Now = func() time.Time { return start }
			clock := func() time.Time { return start.Add(tc.later) }
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
				recoveryWait: func(context.Context, time.Duration) error { return nil }, recoveryNow: clock}
			if _, err := stage.Run(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			if got := world.restarts.Load(); got != tc.wantWrites {
				t.Fatalf("Deployment written %d times, want %d", got, tc.wantWrites)
			}
		})
	}
}

// A write's identity is its action kind, object and resolved arguments, for
// every action kind; the program's narration of why is not part of it.
func TestSameWriteComparesWhatIsWritten(t *testing.T) {
	deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	other := deployment
	other.Name = "worker"
	spec := program.ProgramSpec{ID: "synthetic", Params: []program.ParamSpec{{Name: "replicas", ValueKind: program.IntValue, Default: 1}}}
	param := func(replicas int) program.Action {
		plan := program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: deployment, Replicas: program.FromParam("replicas")}}}
		return program.BindPlanValues(plan, program.ValueInputs{Spec: spec, Bindings: program.Bindings{Values: map[string]any{"replicas": replicas}}}).Actions[0]
	}
	observed := func(image string) program.Action {
		plan := program.Plan{Actions: []program.Action{program.SetField{Ref: deployment, Path: "spec.template.spec.containers[0].image", Value: program.FromObserved(other, "spec.template.spec.containers[0].image"), Reason: "copy"}}}
		read := func(sdk.ObjectRef) (map[string]any, error) {
			return map[string]any{"metadata": map[string]any{"resourceVersion": "synthetic"}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": image}}}}}}, nil
		}
		return program.BindPlanValues(plan, program.ValueInputs{Observed: read}).Actions[0]
	}
	source := func(path string) program.Action {
		return program.SetField{Ref: deployment, Path: path, Value: program.FromSource(deployment, path), Reason: "restore " + path}
	}
	for _, tc := range []struct {
		name string
		a, b program.Action
		same bool
	}{
		{"restart with another reason", program.RestartRollout{Ref: deployment, Reason: "one"}, &program.RestartRollout{Ref: deployment, Reason: "two"}, true},
		{"restart of another object", program.RestartRollout{Ref: deployment, Reason: "one"}, program.RestartRollout{Ref: other, Reason: "one"}, false},
		{"another action on the object", program.RestartRollout{Ref: deployment, Reason: "one"}, program.ReconcileFromSource{Ref: deployment}, false},
		{"source field", source("spec.replicas"), source("spec.replicas"), true},
		{"another source field", source("spec.replicas"), source("spec.paused"), false},
		{"parameter resolving alike", param(2), param(2), true},
		{"parameter resolving differently", param(2), param(3), false},
		{"observed value resolving alike", observed("api:v2"), observed("api:v2"), true},
		{"observed value resolving differently", observed("api:v2"), observed("api:v3"), false},
		{"unbound parameter", program.ScaleWorkload{Ref: deployment, Replicas: program.FromParam("replicas")}, program.ScaleWorkload{Ref: deployment, Replicas: program.FromParam("replicas")}, false},
	} {
		if got := sameWrite(tc.a, tc.b); got != tc.same {
			t.Errorf("%s: sameWrite = %v, want %v", tc.name, got, tc.same)
		}
	}
}
