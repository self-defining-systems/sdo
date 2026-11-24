package generichealth

import (
	"context"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/workloadhealth"
)

func TestReplicaDeficitRespectsReplacementProgress(t *testing.T) {
	at := observedAt
	one, yes := int32(1), true
	hold := time.Minute
	workload := meta.ObjectMeta{Name: "store", Namespace: "app", CreationTimestamp: before(time.Hour)}
	for _, tc := range []struct {
		kind     string
		rule     string
		detector sdk.Detector
	}{
		{"Deployment", workloadhealth.RuleDeploymentUnavailable, workloadhealth.NewDetector(sdk.DetectorSpec{ID: "workload-health"})},
		{"StatefulSet", "statefulset-unready", NewWorkloads(sdk.DetectorSpec{ID: "workload-controllers"})},
		{"DaemonSet", "daemonset-unready", NewWorkloads(sdk.DetectorSpec{ID: "workload-controllers"})},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at, SelfHealHoldTime: &hold}
			owner := meta.OwnerReference{Kind: tc.kind, Name: workload.Name, Controller: &yes}
			switch tc.kind {
			case "Deployment":
				snapshot.DeploymentList = []apps.Deployment{{ObjectMeta: workload, Spec: apps.DeploymentSpec{Replicas: &one},
					Status: apps.DeploymentStatus{Conditions: []apps.DeploymentCondition{{Type: apps.DeploymentAvailable,
						Status: core.ConditionFalse, LastTransitionTime: before(time.Hour)}}}}}
				snapshot.ReplicaSetList = []apps.ReplicaSet{{ObjectMeta: meta.ObjectMeta{Name: "store-rs", Namespace: "app", OwnerReferences: []meta.OwnerReference{owner}}}}
				owner.Kind, owner.Name = "ReplicaSet", "store-rs"
			case "StatefulSet":
				snapshot.StatefulSetList = []apps.StatefulSet{{ObjectMeta: workload, Spec: apps.StatefulSetSpec{Replicas: &one}}}
			case "DaemonSet":
				snapshot.DaemonSetList = []apps.DaemonSet{{ObjectMeta: workload, Status: apps.DaemonSetStatus{DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1}}}
			}
			deletion := before(5 * time.Second)
			old := core.Pod{ObjectMeta: meta.ObjectMeta{Name: "store-old", Namespace: "app",
				CreationTimestamp: workload.CreationTimestamp, DeletionTimestamp: &deletion, OwnerReferences: []meta.OwnerReference{owner}}}
			replacement := core.Pod{ObjectMeta: meta.ObjectMeta{Name: "store-new", Namespace: "app", OwnerReferences: old.OwnerReferences},
				Status: core.PodStatus{Phase: core.PodRunning}}
			check := func(label string, want bool, pods ...core.Pod) {
				t.Helper()
				snapshot.PodList = pods
				got, err := tc.detector.Detect(context.Background(), snapshot)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, finding := range got {
					found = found || finding.RuleID == tc.rule
				}
				if found != want {
					t.Errorf("%s: %s present = %v, want %v (findings %v)", label, tc.rule, found, want, got)
				}
			}
			check("deleting Pod and no replacement yet", false, old)
			replacement.CreationTimestamp = before(10 * time.Second)
			replacement.Status.Conditions = []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue, LastTransitionTime: before(2 * time.Second)}}
			check("replacement Ready before controller status catches up", false, replacement)
			replacement.CreationTimestamp = before(2 * time.Minute)
			replacement.Status.Conditions = []core.PodCondition{{Type: core.PodReady, Status: core.ConditionFalse, LastTransitionTime: replacement.CreationTimestamp}}
			check("replacement stuck past hold", true, replacement)
		})
	}
}
