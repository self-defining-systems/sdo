package runtime

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUsageSampleCarriesOOMKilledTermination(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	killed := now.Add(-time.Minute)
	pod := corev1.Pod{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "p", UID: "u"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "a"}, {Name: "b"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "a", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", FinishedAt: meta.NewTime(killed)}}},
			{Name: "b", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", FinishedAt: meta.NewTime(killed)}}},
		}},
	}
	data := []byte(`{"pods":[{"podRef":{"name":"p","namespace":"ns","uid":"u"},"containers":[
	 {"name":"a","memory":{"workingSetBytes":1,"rssBytes":1}},{"name":"b","memory":{"workingSetBytes":1,"rssBytes":1}}]}]}`)
	r, err := parseUsageSummary(data, "ns", []corev1.Pod{pod}, now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]time.Time{}
	for _, w := range r.Containers {
		got[w.Identity.Container] = w.Samples[0].LastOOMKilledAt
	}
	if !got["a"].Equal(killed) {
		t.Fatalf("OOMKilled termination not carried: %v", got["a"])
	}
	if !got["b"].IsZero() {
		t.Fatalf("non-OOM termination carried: %v", got["b"])
	}
}
