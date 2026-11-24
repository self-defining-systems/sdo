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

func TestUnfilledReplicaDeficitOutlivesSelfHealing(t *testing.T) {
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		for _, scenario := range []struct {
			name    string
			ready   int32
			blocked string
		}{
			{name: "zero replicas"},
			{name: "one old ready replica", ready: 1},
			{name: "quota blocks creation", blocked: "exceeded quota"},
			{name: "admission blocks creation", blocked: "admission webhook denied the request"},
		} {
			t.Run(kind+"/"+scenario.name, func(t *testing.T) {
				at, hold := observedAt, time.Minute
				desired, yes := int32(3), true
				object := meta.ObjectMeta{Name: "store", Namespace: "app", CreationTimestamp: before(time.Hour)}
				owner := meta.OwnerReference{Kind: kind, Name: object.Name, Controller: &yes}
				view := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at, SelfHealHoldTime: &hold}
				detector := NewWorkloads(sdk.DetectorSpec{ID: "workload-controllers"})
				rule := "statefulset-unready"
				setReady := func(ready int32) {}
				switch kind {
				case "Deployment":
					detector, rule = workloadhealth.NewDetector(sdk.DetectorSpec{ID: "workload-health"}), workloadhealth.RuleDeploymentUnavailable
					view.DeploymentList = []apps.Deployment{{ObjectMeta: object, Spec: apps.DeploymentSpec{Replicas: &desired},
						Status: apps.DeploymentStatus{Conditions: []apps.DeploymentCondition{{Type: apps.DeploymentAvailable, Status: core.ConditionFalse}}}}}
					view.ReplicaSetList = []apps.ReplicaSet{{ObjectMeta: meta.ObjectMeta{Name: "store-rs", Namespace: "app", OwnerReferences: []meta.OwnerReference{owner}}}}
					owner.Kind, owner.Name = "ReplicaSet", "store-rs"
					setReady = func(ready int32) { view.DeploymentList[0].Status.AvailableReplicas = ready }
					if scenario.blocked != "" {
						rule = workloadhealth.RuleReplicaFailure
						view.DeploymentList[0].Status.Conditions = append(view.DeploymentList[0].Status.Conditions, apps.DeploymentCondition{
							Type: apps.DeploymentReplicaFailure, Status: core.ConditionTrue, Reason: "FailedCreate",
							Message: scenario.blocked, LastTransitionTime: meta.NewTime(at),
						})
					}
				case "StatefulSet":
					view.StatefulSetList = []apps.StatefulSet{{ObjectMeta: object, Spec: apps.StatefulSetSpec{Replicas: &desired}}}
					setReady = func(ready int32) { view.StatefulSetList[0].Status.ReadyReplicas = ready }
				case "DaemonSet":
					rule = "daemonset-unready"
					view.DaemonSetList = []apps.DaemonSet{{ObjectMeta: object, Status: apps.DaemonSetStatus{DesiredNumberScheduled: desired, UpdatedNumberScheduled: desired}}}
					setReady = func(ready int32) { view.DaemonSetList[0].Status.NumberReady = ready }
				}
				setReady(scenario.ready)
				if scenario.ready > 0 {
					view.PodList = []core.Pod{{ObjectMeta: meta.ObjectMeta{Name: "store-0", Namespace: "app", CreationTimestamp: before(time.Hour),
						OwnerReferences: []meta.OwnerReference{owner}}, Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{
						{Type: core.PodReady, Status: core.ConditionTrue, LastTransitionTime: before(time.Hour)},
					}}}}
				}
				if scenario.blocked != "" {
					view.EventList = []core.Event{{InvolvedObject: core.ObjectReference{Kind: owner.Kind, Name: owner.Name, Namespace: "app"},
						Type: core.EventTypeWarning, Reason: "FailedCreate", Message: scenario.blocked, FirstTimestamp: meta.NewTime(at), Count: 1}}
				}
				check := func(want bool) {
					t.Helper()
					got, err := detector.Detect(context.Background(), view)
					if err != nil {
						t.Fatal(err)
					}
					for _, finding := range got {
						if finding.RuleID == rule && want {
							return
						}
					}
					if want || len(got) != 0 {
						t.Errorf("at %s, want %s present=%v, got %v", at, rule, want, got)
					}
				}
				check(false)
				at = observedAt.Add(hold - time.Second)
				check(false)
				at = observedAt.Add(hold)
				check(true)
				if scenario.blocked == "" {
					setReady(desired)
					check(false)
					at = at.Add(time.Second)
					setReady(scenario.ready)
					check(false)
					at = at.Add(hold)
					check(true)
				}
			})
		}
	}
}
