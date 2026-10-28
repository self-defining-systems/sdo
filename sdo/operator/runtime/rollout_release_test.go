package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A stalled source write hands off the one standard release as a typed action
// that the real executor can apply, and names it before the scheduler text so
// a clipped line still carries it. SDO itself never applies it.
func TestStalledRolloutHandoffCarriesTheStandardRelease(t *testing.T) {
	deployment, current, stale := reconcileRolloutObjects(t)
	deployment.Annotations = map[string]string{"deployment.kubernetes.io/revision": "2"}
	deployment.Status.UpdatedReplicas, deployment.Status.AvailableReplicas = 1, 1
	controller := true
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: stale.Namespace, UID: "old-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	newPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "new-pod", Namespace: current.Namespace, UID: "new-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: current.Name, UID: current.UID, Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			Message: "0/2 nodes are available: 1 node(s) didn't match pod anti-affinity rules, 1 node(s) had untolerated taint {node-role.kubernetes.io/control-plane: }. preemption: 0/2 nodes are available."}}}}
	kube := fake.NewSimpleClientset(deployment, current, stale, oldPod, newPod)
	source := addedFieldsSource()
	live := source.DeepCopy()
	live.SetUID(deployment.UID)
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	ref := addedFieldsRef()
	executor := PlanExecutor{Client: contextAwareSourceClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: kube}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: 6 * time.Second}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(6 * time.Second)}, reconcilePlan(ref), false)
	var pending *RolloutPendingError
	if !errors.As(err, &pending) || result.RolloutPending == nil || len(result.RolloutPending.Blockers) != 1 {
		t.Fatalf("stall was not handed off: %v", err)
	}
	blocker := result.RolloutPending.Blockers[0]
	if len(blocker.Release) != 1 || blocker.Release[0].GetRetireOldRevision().GetRef().GetName() != stale.Name {
		t.Fatalf("handoff lacks the typed release of the old revision: %+v", blocker.Release)
	}
	// The responder brief clips stage lines at 160 characters.
	clipped := result.RolloutPending.Reason[:min(160, len(result.RolloutPending.Reason))]
	for _, want := range []string{"Pod/old-pod", "ReplicaSet/" + stale.Name} {
		if !strings.Contains(clipped, want) {
			t.Fatalf("clipped handoff %q does not name %q", clipped, want)
		}
	}
	for _, action := range kube.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("SDO applied the release on its own: %v", action)
		}
	}

	// Consumer: the decoded action runs through the ordinary executor.
	release := planActionState(blocker.Release[0])
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for _, object := range []kruntime.Object{deployment, current, stale} {
		fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{Object: fields}
		u.SetAPIVersion("apps/v1")
		kind := "ReplicaSet"
		if u.GetName() == deployment.Name {
			kind = "Deployment"
		}
		u.SetKind(kind)
		objects[sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: u.GetNamespace(), Name: u.GetName()}] = u
	}
	client := &planFakeClient{objects: objects}
	apply := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	if _, err := apply.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{release}}, false); err != nil {
		t.Fatalf("standard release failed: %v", err)
	}
	staleRef := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: stale.Namespace, Name: stale.Name}
	if replicas, _, _ := unstructured.NestedFloat64(client.objects[staleRef].Object, "spec", "replicas"); replicas != 0 {
		if r, _, _ := unstructured.NestedInt64(client.objects[staleRef].Object, "spec", "replicas"); r != 0 {
			t.Fatalf("old revision still keeps %v replicas", client.objects[staleRef].Object["spec"])
		}
	}
	currentRef := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: current.Namespace, Name: current.Name}
	if _, err := apply.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.RetireOldRevision{Ref: currentRef}}}, false); !errors.Is(err, program.ErrReplicaSetScaleRefused) {
		t.Fatalf("retiring the current revision must be refused at write time, got %v", err)
	}
}
