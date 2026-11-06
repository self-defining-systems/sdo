package runtime

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestRecoveryWaitsForScaledDownRolloutChildren(t *testing.T) {
	yes := true
	zero := int32(0)
	d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "app", UID: "deployment"}}
	rs := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "app", UID: "old-set", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Name, UID: d.UID, Controller: &yes}}}, Spec: appsv1.ReplicaSetSpec{Replicas: &zero}}
	p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: "app", UID: "old-pod", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: &yes}}}}
	raw, _ := kruntime.DefaultUnstructuredConverter.ToUnstructured(&p)
	object := &unstructured.Unstructured{Object: raw}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: p.Namespace, Name: p.Name}
	f := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, PrimaryResource: ref, Metadata: map[string]any{"source_inventory": true, "drift_event": sdk.DriftEvent{Ref: ref, Source: "absent from accepted source", Fields: inventoryFields(object)}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{d}, ReplicaSetList: []appsv1.ReplicaSet{rs}, PodList: []corev1.Pod{p}}
	effects := lifecycleEffects(snapshot, []sdk.Finding{f}, map[types.UID]bool{d.UID: true}, true)
	if len(effects) != 1 {
		t.Fatal("scaled-down old Pod has no waiting authority before deletion starts")
	}
	if len(effects.observe(snapshot, []sdk.Finding{f})) != 1 {
		t.Fatal("live Pod was retired")
	}
	// The replacing controller may not have scaled the old set down yet.
	// Its old template grants waiting only; the live Pod still blocks.
	one := int32(1)
	snapshot.ReplicaSetList[0].Spec.Replicas = &one
	snapshot.ReplicaSetList[0].Spec.Template.Spec.NodeSelector = map[string]string{"synthetic": "old"}
	if len(lifecycleEffects(snapshot, []sdk.Finding{f}, map[types.UID]bool{d.UID: true}, true)) != 1 {
		t.Fatal("old template's Pod has no waiting authority before scale-down")
	}
	snapshot.PodList = nil
	if len(effects.observe(snapshot, []sdk.Finding{f})) != 0 {
		t.Fatal("absent old Pod was retained")
	}
	snapshot.PodList = []corev1.Pod{p}
	snapshot.PodList[0].UID = "replacement"
	if len(effects.observe(snapshot, []sdk.Finding{f})) != 1 {
		t.Fatal("replacement borrowed disappearance")
	}
}
