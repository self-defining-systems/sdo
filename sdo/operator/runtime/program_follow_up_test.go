package runtime

import (
	"context"
	"encoding/json"
	"errors"

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

func rollRetireOld(cluster *rollCluster) {
	cluster.objects[rollOldRS].Object["spec"].(map[string]any)["replicas"] = int64(0)
}

// wrongPlanProgram is a p024-shaped learned program: its diagnosis matches a
// fault its plan does not touch, so after its write the rollout stays blocked
// and its own diagnosis still matches.
type wrongPlanProgram struct{ rollProgram }

func (wrongPlanProgram) Spec() program.ProgramSpec {
	spec := rollProgram{}.Spec()
	spec.ID = "wrong-plan"
	return spec
}

func (wrongPlanProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}

