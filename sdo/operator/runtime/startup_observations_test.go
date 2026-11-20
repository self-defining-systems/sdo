package runtime

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var observationEpoch = time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)

// readyPod is a Running pod whose container started `boot` before it turned Ready at `readyAt`.
func readyPod(name string, readyAt time.Time, boot time.Duration, restarts int32) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt)}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "web", Ready: true, RestartCount: restarts,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(readyAt.Add(-boot))}},
			}},
		},
	}
}

func sameWorkload(corev1.Pod) string { return "Deployment/web" }

func TestObservedStartupIsAPercentileWithAMargin(t *testing.T) {
	store := NewStartupObservations()
	store.Observe([]corev1.Pod{readyPod("a", observationEpoch, 100*time.Second, 0)}, sameWorkload)
	got := store.Allowances()["Deployment/web"]
	if want := 135 * time.Second; got != want {
		t.Fatalf("allowance = %s, want %s (100 s x 1.25 + 10 s)", got, want)
	}
}

func TestObservedStartupCountsAReadyTransitionOnce(t *testing.T) {
	store := NewStartupObservations()
	pod := readyPod("a", observationEpoch, 100*time.Second, 0)
	store.Observe([]corev1.Pod{pod}, sameWorkload)
	slow := readyPod("a", observationEpoch, 400*time.Second, 0)
	store.Observe([]corev1.Pod{pod, slow}, sameWorkload)
	if got := store.Allowances()["Deployment/web"]; got != 135*time.Second {
		t.Fatalf("allowance = %s; a pod already sampled was counted again", got)
	}
}

// A restarted container is a crash, not a healthy start.
func TestObservedStartupIgnoresRestartedContainers(t *testing.T) {
	store := NewStartupObservations()
	store.Observe([]corev1.Pod{readyPod("a", observationEpoch, 100*time.Second, 2)}, sameWorkload)
	if got := store.Allowances(); len(got) != 0 {
		t.Fatalf("allowances = %v, want none from a restarted container", got)
	}
}

func TestObservedStartupIsCapped(t *testing.T) {
	store := NewStartupObservations()
	store.Observe([]corev1.Pod{readyPod("a", observationEpoch, 3*time.Hour, 0)}, sameWorkload)
	if got := store.Allowances()["Deployment/web"]; got != startupMaxAllowance {
		t.Fatalf("allowance = %s, want the %s cap", got, startupMaxAllowance)
	}
}

// What the install learned survives a controller restart through the state.
func TestObservedStartupSurvivesAControllerRestart(t *testing.T) {
	first := startupController(t, 0, 0, probeSamples(1, func(int) bool { return false }))
	first.config.StartupObservations = NewStartupObservations()
	first.config.StartupObservations.Observe([]corev1.Pod{readyPod("a", observationEpoch, 100*time.Second, 0)}, sameWorkload)
	state, err := first.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	second := startupController(t, 0, 500, probeSamples(1, func(int) bool { return false }))
	second.config.StartupObservations = NewStartupObservations()
	if err := second.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if got := second.config.StartupObservations.Allowances()["Deployment/web"]; got != 135*time.Second {
		t.Fatalf("restored allowance = %s, want 135s", got)
	}
	// The restored Ready transition is not sampled twice.
	second.config.StartupObservations.Observe([]corev1.Pod{readyPod("a", observationEpoch, 400*time.Second, 0)}, sameWorkload)
	if got := second.config.StartupObservations.Allowances()["Deployment/web"]; got != 135*time.Second {
		t.Fatalf("allowance after re-observing = %s, want 135s", got)
	}
}
