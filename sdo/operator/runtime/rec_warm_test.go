package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

func TestRECPostRestartWaitsForBoundStandbyAndContinuation(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.currentIncidentRequest.MaxResponseLoops = 3
	for i := 2; i <= 17; i++ {
		step(i)
	}
	state := exportState(t, c)
	id := state.GetIncidentRequest().GetIncidentId()
	root := t.TempDir()
	warmName := "sdo-standby-bound-before-continuation"
	coldName := IncidentJobName(id + "-continue-1")
	binding := worker_registry.WarmBinding{JobName: warmName, IncidentID: id, Directory: filepath.Join(root, "standby-slot"), Digest: "frozen-request", ClaimedAt: time.Unix(1, 0)}
	payload, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, ".sdo-runtime", "standby", "bindings")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "frozen-request.json"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: warmName, Namespace: "control", UID: types.UID("bound-job")}, Status: batchv1.JobStatus{Active: 1}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: coldName, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: warmName + "-binding", Namespace: "control", Annotations: map[string]string{"sdo.dev/incident-id": id, "sdo.dev/task-id": id, "sdo.dev/job-name": warmName, "sdo.dev/job-uid": "bound-job"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "warm-orphan", Namespace: "control", Labels: map[string]string{"job-name": warmName}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	)
	client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	restored, err := NewController(testControllerConfig(), c.detectorList(), c.provider, KubernetesJobDispatcher{Client: client, Namespace: "control", RepositoryMountPath: root}, time.Unix(17, 0))
	if err != nil {
		t.Fatal(err)
	}
	restored.broker = &recordingIncidentBroker{}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	restored.finishEarlyReport(context.Background(), time.Unix(47, 0))
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("reflection released with a bound standby/continuation still active")
	}
	deleted := map[string]bool{}
	for _, action := range client.Actions() {
		if action.Matches("delete", "jobs") {
			deleted[action.(ktesting.DeleteAction).GetName()] = true
		}
	}
	if !deleted[warmName] || !deleted[coldName] {
		t.Fatalf("not every execution identity was stopped: %v", deleted)
	}
	for _, name := range []string{warmName, coldName} {
		if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "control", name); err != nil {
			t.Fatal(err)
		}
	}
	restored.finishEarlyReport(context.Background(), time.Unix(48, 0))
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("reflection released while bound standby orphan Pod still runs")
	}
	if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "control", "warm-orphan"); err != nil {
		t.Fatal(err)
	}
	restored.finishEarlyReport(context.Background(), time.Unix(49, 0))
	if _, ok := restored.PendingClosureEffect(); !ok {
		t.Fatal("quiescent bound standby still blocks closure")
	}
}
