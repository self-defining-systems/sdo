package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var (
	rollWeb    = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	rollOldRS  = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "web-old"}
	rollNewPod = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-new-0"}
)

// rollCluster models a rollout that deadlocks after a correct template patch:
// the new pod cannot schedule while the old-template ReplicaSet keeps its pod.
type rollCluster struct{ planFakeClient }

func (c *rollCluster) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if err := c.planFakeClient.Patch(ctx, ref, kind, data); err != nil {
		return err
	}
	if ref == rollWeb {
		// The Deployment controller starts a new revision for the new template.
		c.objects[rollWeb].SetAnnotations(map[string]string{rollRevision: "2"})
	}
	if ref == rollWeb && c.objects[rollNewPod] == nil {
		c.objects[rollNewPod] = &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": rollNewPod.Name, "namespace": "app", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-new", "uid": "rs-new", "controller": true}}}, "status": map[string]any{"phase": "Pending", "conditions": []any{map[string]any{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}}}}}
	}
	return nil
}

func (c *rollCluster) blocked() bool {
	replicas, _, _ := unstructured.NestedFieldNoCopy(c.objects[rollOldRS].Object, "spec", "replicas")
	return replicas != int64(0) && replicas != float64(0)
}

func (c *rollCluster) Snapshot(context.Context) (sdk.DetectionContext, error) {
	var deployment appsv1.Deployment
	var set appsv1.ReplicaSet
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for ref, target := range map[sdk.ObjectRef]any{rollWeb: &deployment, rollOldRS: &set} {
		data, _ := json.Marshal(c.objects[ref].Object)
		if err := json.Unmarshal(data, target); err != nil {
			return nil, err
		}
	}
	snapshot.DeploymentList = []appsv1.Deployment{deployment}
	snapshot.ReplicaSetList = []appsv1.ReplicaSet{set}
	if object := c.objects[rollNewPod]; object != nil {
		var pod corev1.Pod
		data, _ := json.Marshal(object.Object)
		if err := json.Unmarshal(data, &pod); err != nil {
			return nil, err
		}
		snapshot.PodList = []corev1.Pod{pod}
	}
	return snapshot, nil
}

type rollRollouts struct{ cluster *rollCluster }

func (r rollRollouts) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Observe(ctx, refs)
}
func (r rollRollouts) Observe(context.Context, []sdk.ObjectRef) error {
	if r.cluster.blocked() {
		return &rolloutBlockedError{blockers: []*contracts.RolloutBlocker{{Ref: planRefProto(rollWeb), Reason: "new pod Unschedulable beside old-template pod"}}, cause: context.DeadlineExceeded}
	}
	return nil
}

const rollRevision = "deployment.kubernetes.io/revision"

// rollProgram restores the Deployment and, once it observes the deadlock, retires
// the old ReplicaSet. Its first plan cannot know the rollout will deadlock.
// oneShot retires the bound Deployment's ready ReplicaSet in its first plan;
// flip alternates the Deployment between two sizes; fail errors after the deadlock.
type rollProgram struct{ cleanup, oneShot, flip, fail bool }

func (rollProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "rollout-cleanup", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}, {Name: "retired", ValueKind: program.IntValue, Default: 0}, {Name: "replicas", ValueKind: program.IntValue, Default: 1}, {Name: "wide", ValueKind: program.IntValue, Default: 2}}}
}
func (rollProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if *view.Deployments()[0].Spec.Replicas != 1 || rollDeadlocked(view) {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p rollProgram) Mitigate(_ context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	if p.fail && rollDeadlocked(view) {
		return program.Plan{}, errors.New("cannot read the rollout")
	}
	size := "replicas"
	if p.flip && *view.Deployments()[0].Spec.Replicas == 1 {
		size = "wide"
	}
	plan := program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: b.Objects["deployment"], Replicas: program.FromParam(size)}}}
	if set, ok := rollOwnedReadySet(view, b.Objects["deployment"], !p.oneShot); ok && (p.oneShot || (p.cleanup && rollDeadlocked(view))) {
		plan.Actions = append(plan.Actions, program.ScaleWorkload{Ref: set, Replicas: program.FromParam("retired")})
	}
	return plan, nil
}

// rollOwnedReadySet selects a ready ReplicaSet by its owner relation and,
// when old is set, only an older revision than the Deployment's current one.
func rollOwnedReadySet(view program.ReadView, deployment sdk.ObjectRef, old bool) (sdk.ObjectRef, bool) {
	current := ""
	for _, candidate := range view.Deployments() {
		if candidate.Name == deployment.Name {
			current = candidate.Annotations[rollRevision]
		}
	}
	for _, set := range view.ReplicaSets() {
		owner := metav1.GetControllerOf(&set)
		if owner == nil || owner.Kind != "Deployment" || owner.Name != deployment.Name || set.Spec.Replicas == nil || *set.Spec.Replicas == 0 || set.Status.ReadyReplicas == 0 {
			continue
		}
		if !old || set.Annotations[rollRevision] != current {
			return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: set.Namespace, Name: set.Name}, true
		}
	}
	return sdk.ObjectRef{}, false
}

func rollDeadlocked(view program.ReadView) bool {
	unschedulable := false
	for _, pod := range view.Pods() {
		for _, condition := range pod.Status.Conditions {
			unschedulable = unschedulable || (condition.Type == corev1.PodScheduled && condition.Reason == "Unschedulable")
		}
	}
	_, blocking := rollOwnedReadySet(view, rollWeb, true)
	return unschedulable && blocking
}

func rollLoop(t *testing.T, p rollProgram) (*ResponderLoop, *rollCluster, string) {
	t.Helper()
	deployment := planObject(rollWeb, map[string]any{"replicas": int64(2)})
	deployment.SetUID("web-uid")
	deployment.SetAnnotations(map[string]string{rollRevision: "1"})
	set := planObject(rollOldRS, map[string]any{"replicas": int64(1)})
	set.SetUID("rs-old")
	set.SetAnnotations(map[string]string{rollRevision: "1"})
	set.Object["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "web-uid", "controller": true}}
	set.Object["status"] = map[string]any{"readyReplicas": int64(1)}
	cluster := &rollCluster{planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{rollWeb: deployment, rollOldRS: set}}}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: rollWeb, ParameterBindings: map[string]sdk.ObjectRef{"deployment": rollWeb}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "roll", Namespace: "app", SourceCommit: "source", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(10 * time.Minute), Findings: []sdk.Finding{finding}}, registry, cluster, nil, PlanExecutor{Client: cluster, Rollouts: rollRollouts{cluster}, Verifier: planFakeVerifier{true}}, 5)
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
	}
	loop.MenuConfig = &ResponderMenuConfig{}
	loop.ProgramFollowUpSettle = time.Millisecond
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	for _, option := range menu.Menu.GetOptions() {
		if option.GetProgramId() == "rollout-cleanup" {
			return loop, cluster, option.GetOptionId()
		}
	}
	t.Fatalf("program option missing: %+v", menu)
	return nil, nil, ""
}

func TestProgramFollowUpRetiresOldReplicaSetAfterDeadlockedRollout(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{cleanup: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if response.Report.GetStatus() != contracts.MitigationReport_STATUS_SUCCESS || response.RolloutPending != nil {
		t.Fatalf("program did not finish its repair after the rollout deadlocked: %+v %+v", response.Report, response.MenuExecution)
	}
	if cluster.blocked() {
		t.Fatal("old-template ReplicaSet was not retired")
	}
	retired := false
	for _, receipt := range loop.MenuRepairActions() {
		retired = retired || receipt.Target == "ReplicaSet/web-old"
	}
	if !retired || loop.LoopsUsed() != 1 || len(loop.pendingWrites) != 0 {
		t.Fatalf("follow-up lost its receipt, consumed a batch or kept a lock: receipts=%+v loops=%d pending=%d", loop.MenuRepairActions(), loop.LoopsUsed(), len(loop.pendingWrites))
	}
}

func TestStalledProgramApplicationReleasesLockToResponder(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Millisecond
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a deadlocked rollout: %+v", first.Report)
	}
	time.Sleep(5 * time.Millisecond)
	scale := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "Deployment/app/web", "replicas": "1"}})
	if scale.Report.GetReasonCode() == "pending_write_conflict" {
		t.Fatalf("responder waited on a stalled program lock: %s", scale.Error)
	}
	report := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if report.Report.GetReasonCode() != "rollout_stalled" || report.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("stalled application lacks its typed reason: %+v", report.Report)
	}
	// The responder wrote the released target, so a later completion is not the program's.
	rollRetireOld(cluster)
	late := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if late.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("program received credit for a rollout the responder's later write may have finished: %+v", late.Report)
	}
}

func TestSlowHealthyRolloutAfterStallReleaseKeepsProgramSuccess(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Millisecond
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a slow rollout: %+v", first.Report)
	}
	time.Sleep(5 * time.Millisecond)
	stalled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if stalled.Report.GetReasonCode() != "rollout_stalled" {
		t.Fatalf("setup: expected the lock release: %+v", stalled.Report)
	}
	_, release, ok := acquireClosureGuard(loop)
	release()
	if !ok {
		t.Fatal("released application still blocks the responder's closure check")
	}
	rollRetireOld(cluster) // the old pod finally terminates; nobody else wrote the target
	settled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if settled.Report.GetStatus() != contracts.MitigationReport_STATUS_SUCCESS || settled.Report.GetReasonCode() != "" {
		t.Fatalf("slow healthy rollout lost the program's success: %+v", settled.Report)
	}
	for _, application := range loop.applicationsLog {
		if application.ID == first.ApplicationID && (application.Error != "" || application.Pending || !application.Result.VerificationCompleted || !application.Result.VerificationPassed) {
			t.Fatalf("learning receipt still records a stall: %+v", application)
		}
	}
	if len(loop.pendingWrites) != 0 {
		t.Fatal("settled reservation remained")
	}
}

func rollRetireOld(cluster *rollCluster) {
	cluster.objects[rollOldRS].Object["spec"].(map[string]any)["replicas"] = int64(0)
}

// A dup-PVC repeat: after the release the responder retires the old ReplicaSet,
// a different object than the program's Deployment. Its write finished the
// rollout, so the program must not earn credit for it.
func TestResponderChildWriteAfterStallReleaseDeniesProgramCredit(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Millisecond
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a deadlocked rollout: %+v", first.Report)
	}
	time.Sleep(5 * time.Millisecond)
	if stalled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}}); stalled.Report.GetReasonCode() != "rollout_stalled" {
		t.Fatalf("setup: expected the lock release: %+v", stalled.Report)
	}
	scale := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "ReplicaSet/app/web-old", "replicas": "0"}})
	if scale.Error != "" || cluster.blocked() {
		t.Fatalf("setup: the responder's ReplicaSet scale did not apply: %s %+v", scale.Error, scale.Report)
	}
	late := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if late.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("program received credit for a rollout the responder's child write finished: %+v", late.Report)
	}
	for _, application := range loop.applicationsLog {
		if application.ID == first.ApplicationID && application.Result.VerificationPassed {
			t.Fatalf("learning receipt credits the program: %+v", application)
		}
	}
}

// A first plan that retires the ReplicaSet still serving the Deployment would
// cause an outage; the runtime refuses it before any write.
func TestProgramScaleOfCurrentReplicaSetIsRefused(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{oneShot: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if response.Report.GetReasonCode() != "replicaset_scale_refused" {
		t.Fatalf("scaling the current revision must be a typed refusal: %+v %+v", response.Report, response.MenuExecution)
	}
	if !cluster.blocked() || cluster.objects[rollNewPod] != nil {
		t.Fatal("a refused plan wrote to the cluster")
	}
}

func TestProgramFollowUpStopsOnRepeatedPlanCycle(t *testing.T) {
	loop, _, option := rollLoop(t, rollProgram{flip: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	applied := 0
	for _, round := range response.MenuExecution {
		if round.ApplicationID != "" {
			applied++
		}
	}
	if applied != 2 {
		t.Fatalf("an A/B/A plan cycle must stop at its first repeat, applied %d rounds: %+v", applied, response.MenuExecution)
	}
}

func TestProgramFollowUpFailureIsSignaledAndReported(t *testing.T) {
	loop, _, option := rollLoop(t, rollProgram{fail: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	signaled := false
	for _, signal := range loop.failureSignals {
		signaled = signaled || signal == "mitigator_failure:rollout-cleanup"
	}
	reported := false
	for _, round := range response.MenuExecution {
		reported = reported || strings.Contains(round.Error, "cannot read the rollout")
	}
	if !signaled || !reported {
		t.Fatalf("follow-up mitigator failure lost: signals=%v rounds=%+v", loop.failureSignals, response.MenuExecution)
	}
}

// A follow-up writes only through the ordinary reservation check: another
// application's held write on the same object refuses it.
func TestProgramFollowUpRespectsOtherHeldReservations(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{cleanup: true})
	loop.pendingWrites = map[string]pendingMitigation{"other": {Refs: []sdk.ObjectRef{rollOldRS}, AttemptID: "other", ReservedAt: time.Now().UTC()}}
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if !cluster.blocked() {
		t.Fatal("follow-up wrote an object another application holds")
	}
	if !strings.Contains(fmt.Sprint(response.Report, response.MenuExecution), "pending_write_conflict") {
		t.Fatalf("follow-up conflict lacks its typed reason: %+v %+v", response.Report, response.MenuExecution)
	}
}
