package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

// rollProgram restores the Deployment and, once it observes the deadlock, retires
// the old ReplicaSet. Its first plan cannot know the rollout will deadlock.
type rollProgram struct{ cleanup bool }

func (rollProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "rollout-cleanup", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}, {Name: "retired", ValueKind: program.IntValue, Default: 0}, {Name: "replicas", ValueKind: program.IntValue, Default: 1}}}
}
func (rollProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if *view.Deployments()[0].Spec.Replicas != 1 || rollDeadlocked(view) {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p rollProgram) Mitigate(_ context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	plan := program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: b.Objects["deployment"], Replicas: program.FromParam("replicas")}}}
	if p.cleanup && rollDeadlocked(view) {
		plan.Actions = append(plan.Actions, program.ScaleWorkload{Ref: rollOldRS, Replicas: program.FromParam("retired")})
	}
	return plan, nil
}

func rollDeadlocked(view program.ReadView) bool {
	unschedulable := false
	for _, pod := range view.Pods() {
		for _, condition := range pod.Status.Conditions {
			unschedulable = unschedulable || (condition.Type == corev1.PodScheduled && condition.Reason == "Unschedulable")
		}
	}
	for _, set := range view.ReplicaSets() {
		if set.Name == rollOldRS.Name && set.Spec.Replicas != nil && *set.Spec.Replicas > 0 && set.Status.ReadyReplicas > 0 {
			return unschedulable
		}
	}
	return false
}

func rollLoop(t *testing.T, p rollProgram) (*ResponderLoop, *rollCluster, string) {
	t.Helper()
	deployment := planObject(rollWeb, map[string]any{"replicas": int64(2)})
	deployment.SetUID("web-uid")
	set := planObject(rollOldRS, map[string]any{"replicas": int64(1)})
	set.SetUID("rs-old")
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
	loop, _, option := rollLoop(t, rollProgram{})
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
}
