package program_test

import (
	"errors"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func releaseFixture(currentPhase string) sdktest.Snapshot {
	controller := true
	one, zero := int32(1), int32(0)
	web := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "web", UID: "web-uid", Annotations: map[string]string{"deployment.kubernetes.io/revision": "3"}}}
	set := func(name, uid, revision string, replicas *int32) appsv1.ReplicaSet {
		s := revisionSet(name, "web", revision)
		s.UID = types.UID(uid)
		s.Spec.Replicas = replicas
		return s
	}
	pod := func(name, owner, uid string) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: name, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: owner, UID: types.UID(uid), Controller: &controller}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	}
	holder := pod("web-2-a", "web-2", "rs-2")
	replacement := pod("web-3-a", "web-3", "rs-3")
	replacement.Status.Phase = corev1.PodPending
	switch currentPhase {
	case "unschedulable":
		replacement.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/1 nodes are available: 1 node(s) didn't match pod anti-affinity rules."}}
	case "crashing":
		replacement.Status.Phase = corev1.PodRunning
		replacement.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "web", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
	}
	return sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{web}, ReplicaSetList: []appsv1.ReplicaSet{
		set("web-1", "rs-1", "1", &zero), set("web-2", "rs-2", "2", &one), set("web-3", "rs-3", "3", &one), revisionSet("other-1", "other", "1"),
	}, PodList: []corev1.Pod{holder, replacement}}
}

// A rollout whose replacement cannot be placed is released by retiring every
// old revision that still keeps a replica, and only then: a crashing or merely
// slow replacement must never lose the old revision that still serves.
func TestStalledRolloutReleaseRetiresOnlyOldRevisionsOfAnUnplaceableRollout(t *testing.T) {
	web := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}
	got := program.StalledRolloutRelease(releaseFixture("unschedulable"), web)
	want := []program.Action{program.RetireOldRevision{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "demo", Name: "web-2"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("release = %#v, want %#v", got, want)
	}
	if err := program.CheckReplicaSetScales(program.Plan{Actions: got}, releaseFixture("unschedulable")); err != nil {
		t.Fatalf("the standard release must pass the old-revision rule: %v", err)
	}
	for _, phase := range []string{"crashing", "pending"} {
		if got := program.StalledRolloutRelease(releaseFixture(phase), web); len(got) != 0 {
			t.Fatalf("%s replacement must keep the old revision, got %#v", phase, got)
		}
	}
	if got := program.StalledRolloutRelease(releaseFixture("unschedulable"), sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "missing"}); len(got) != 0 {
		t.Fatalf("an unobserved Deployment has no release, got %#v", got)
	}
}

// RetireOldRevision is bounded by the same rule as a ReplicaSet scale: never
// the current revision, never a ReplicaSet without an observed owner.
func TestRetireOldRevisionRefusesCurrentAndUnownedReplicaSets(t *testing.T) {
	view := releaseFixture("unschedulable")
	for _, name := range []string{"web-3", "other-1", "missing"} {
		plan := program.Plan{Actions: []program.Action{program.RetireOldRevision{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "demo", Name: name}}}}
		if err := program.CheckReplicaSetScales(plan, view); !errors.Is(err, program.ErrReplicaSetScaleRefused) {
			t.Fatalf("retiring %s must be a typed refusal, got %v", name, err)
		}
	}
	if err := (program.Plan{Actions: []program.Action{program.RetireOldRevision{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}}}}).Validate(); err == nil {
		t.Fatal("RetireOldRevision must name a ReplicaSet")
	}
}
