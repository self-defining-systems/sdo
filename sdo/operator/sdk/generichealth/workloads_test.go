package generichealth

import (
	"context"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

var observedAt = time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)

func before(d time.Duration) meta.Time { return meta.NewTime(observedAt.Add(-d)) }

func heldRules(t *testing.T, d sdk.Detector, s sdktest.Snapshot) []string {
	t.Helper()
	at := observedAt
	s.NamespaceName, s.ObservedAtTime = "app", &at
	findings, err := d.Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range findings {
		out = append(out, f.RuleID)
	}
	return out
}

// dbPod is a StatefulSet Pod that turned unready `since` before observation.
func dbPod(since time.Duration) core.Pod {
	return core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "db-0", Namespace: "app", CreationTimestamp: before(time.Hour), OwnerReferences: []meta.OwnerReference{{Kind: "StatefulSet", Name: "db"}}},
		Status:     core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionFalse, LastTransitionTime: before(since)}}},
	}
}

// An unready StatefulSet is held from its newest unready Pod's Ready
// transition, not gated on the age of the objects.
func TestStatefulSetUnreadyIsHeldFromPodReadyTransition(t *testing.T) {
	one := int32(1)
	db := apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{Name: "db", Namespace: "app", CreationTimestamp: before(48 * time.Hour)},
		Spec:       apps.StatefulSetSpec{Replicas: &one},
	}
	workloads := NewWorkloads(sdk.DetectorSpec{ID: "workload-controllers"})
	if got := heldRules(t, workloads, sdktest.Snapshot{StatefulSetList: []apps.StatefulSet{db}, PodList: []core.Pod{dbPod(time.Minute)}}); len(got) != 0 {
		t.Fatalf("a one-minute unready StatefulSet was reported: %v", got)
	}
	if got := heldRules(t, workloads, sdktest.Snapshot{StatefulSetList: []apps.StatefulSet{db}, PodList: []core.Pod{dbPod(6 * time.Minute)}}); len(got) != 1 || got[0] != "statefulset-unready" {
		t.Fatalf("findings = %v, want statefulset-unready after the hold", got)
	}
}

// A CronJob's failed Job never reconciles, so it is not reported; a
// standalone failed Job is Kubernetes' give-up signal and is.
func TestJobFailedExcludesCronJobOwnedJobs(t *testing.T) {
	failed := batch.JobStatus{Failed: 1, Conditions: []batch.JobCondition{{Type: batch.JobFailed, Status: core.ConditionTrue, Reason: "BackoffLimitExceeded"}}}
	scheduled := batch.Job{ObjectMeta: meta.ObjectMeta{Name: "jobs-29000", Namespace: "app", OwnerReferences: []meta.OwnerReference{{Kind: "CronJob", Name: "jobs"}}}, Status: failed}
	standalone := batch.Job{ObjectMeta: meta.ObjectMeta{Name: "migrate", Namespace: "app"}, Status: failed}
	got := heldRules(t, NewWorkloads(sdk.DetectorSpec{ID: "workload-controllers"}), sdktest.Snapshot{JobList: []batch.Job{scheduled, standalone}})
	if len(got) != 1 || got[0] != "job-failed" {
		t.Fatalf("findings = %v, want one job-failed for the standalone Job", got)
	}
}

// Node pressure heals through eviction and is not reported; NotReady is held.
func TestNodeHealthReportsOnlyHeldNotReady(t *testing.T) {
	pod := core.Pod{ObjectMeta: meta.ObjectMeta{Name: "api-1", Namespace: "app"}, Spec: core.PodSpec{NodeName: "n1"}, Status: core.PodStatus{Phase: core.PodRunning}}
	node := func(conditions ...core.NodeCondition) core.Node {
		return core.Node{ObjectMeta: meta.ObjectMeta{Name: "n1"}, Status: core.NodeStatus{Conditions: conditions}}
	}
	nodes := NewNodes(sdk.DetectorSpec{ID: "node-health"})
	pressure := core.NodeCondition{Type: core.NodeMemoryPressure, Status: core.ConditionTrue, LastTransitionTime: before(time.Hour)}
	if got := heldRules(t, nodes, sdktest.Snapshot{PodList: []core.Pod{pod}, NodeList: []core.Node{node(pressure)}}); len(got) != 0 {
		t.Fatalf("node pressure was reported: %v", got)
	}
	notReady := core.NodeCondition{Type: core.NodeReady, Status: core.ConditionFalse, LastTransitionTime: before(time.Minute)}
	if got := heldRules(t, nodes, sdktest.Snapshot{PodList: []core.Pod{pod}, NodeList: []core.Node{node(notReady)}}); len(got) != 0 {
		t.Fatalf("a one-minute NotReady node was reported: %v", got)
	}
	notReady.LastTransitionTime = before(5 * time.Minute)
	if got := heldRules(t, nodes, sdktest.Snapshot{PodList: []core.Pod{pod}, NodeList: []core.Node{node(notReady)}}); len(got) != 1 || got[0] != "node-Ready" {
		t.Fatalf("findings = %v, want node-Ready after the hold", got)
	}
}
