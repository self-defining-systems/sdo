package runtime

import (
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var observationEpoch = time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)

// startingPod is a pod whose only container started at `started` and that has
// not been Ready: the kubelet reported Ready=False before the container started.
func startingPod(name string, started time.Time) corev1.Pod {
	pod := readyPod(name, started, 0)
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	pod.Status.Conditions[0].LastTransitionTime = metav1.NewTime(started.Add(-2 * time.Second))
	pod.Status.ContainerStatuses[0].Ready = false
	return pod
}

// readyPod is a Running pod whose container started `boot` before its Ready
// condition last turned true at `readyAt`.
func readyPod(name string, readyAt time.Time, boot time.Duration) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("uid-" + name)},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt)}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "web", Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(readyAt.Add(-boot))}},
			}},
		},
	}
}

// createdPod has no started container yet.
func createdPod(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("uid-" + name)},
		Status: corev1.PodStatus{
			Phase:             corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "web", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}},
		},
	}
}

// unreadyPod is a pod that was Ready and lost it at `at`.
func unreadyPod(name string, started, at time.Time) corev1.Pod {
	pod := startingPod(name, started)
	pod.Status.Conditions[0].LastTransitionTime = metav1.NewTime(at)
	return pod
}

func restarted(pod corev1.Pod) corev1.Pod {
	pod.Status.ContainerStatuses[0].RestartCount = 1
	return pod
}

func sameWorkload(corev1.Pod) string { return "Deployment/web" }

type observedAt struct {
	after time.Duration // since observationEpoch, when the container started
	pod   corev1.Pod
}

func observeAll(store *StartupObservations, passes ...observedAt) {
	for _, pass := range passes {
		store.Observe(observationEpoch.Add(pass.after), []corev1.Pod{pass.pod}, sameWorkload)
	}
}

// liveStart is a pod seen starting, then Ready `boot` after its container started.
func liveStart(name string, boot time.Duration) []observedAt {
	return []observedAt{
		{boot - 5*time.Second, startingPod(name, observationEpoch)},
		{boot + time.Second, readyPod(name, observationEpoch.Add(boot), boot)},
	}
}

func TestObservedStartupIsAPercentileWithAMargin(t *testing.T) {
	store := NewStartupObservations()
	observeAll(store, liveStart("a", 100*time.Second)...)
	got := store.Allowances()["Deployment/web"]
	if want := 135 * time.Second; got != want {
		t.Fatalf("allowance = %s, want %s (100 s x 1.25 + 10 s)", got, want)
	}
}

// Only the first Ready of containers seen before they were Ready is a start
// time. The Ready condition keeps only its last transition, so anything else
// may be a return to Ready after a flap.
func TestObservedStartupIsTheFirstReadyOnly(t *testing.T) {
	const s = time.Second
	start := observationEpoch
	at := func(d time.Duration) time.Time { return start.Add(d) }
	for name, test := range map[string]struct {
		passes []observedAt
		want   []time.Duration
	}{
		"first Ready seen live":             {liveStart("a", 50*s), []time.Duration{50 * s}},
		"first Ready seen from creation":    {[]observedAt{{-s, createdPod("a")}, {6 * s, readyPod("a", at(5*s), 5*s)}}, []time.Duration{5 * s}},
		"already Ready when first seen":     {[]observedAt{{60 * s, readyPod("a", at(50*s), 50*s)}, {70 * s, readyPod("a", at(50*s), 50*s)}}, nil},
		"first seen Ready after a flap":     {[]observedAt{{2000 * s, readyPod("a", at(1051*s), 1051*s)}}, nil},
		"first seen unready, was Ready":     {[]observedAt{{1030 * s, unreadyPod("a", start, at(1000*s))}, {1052 * s, readyPod("a", at(1051*s), 1051*s)}}, nil},
		"flap after the first Ready":        {append(liveStart("a", 50*s), observedAt{1030 * s, unreadyPod("a", start, at(1000*s))}, observedAt{1052 * s, readyPod("a", at(1051*s), 1051*s)}), []time.Duration{50 * s}},
		"unseen flap after the first Ready": {append(liveStart("a", 50*s), observedAt{1052 * s, readyPod("a", at(1051*s), 1051*s)}), []time.Duration{50 * s}},
		"unobserved long before Ready":      {[]observedAt{{10 * s, startingPod("a", start)}, {1100 * s, readyPod("a", at(1051*s), 1051*s)}}, nil},
		"container restarted while starting": {[]observedAt{
			{10 * s, startingPod("a", start)},
			{60 * s, restarted(startingPod("a", at(55*s)))},
			{66 * s, restarted(readyPod("a", at(65*s), 10*s))},
		}, nil},
		"container restarted after Ready": {append(liveStart("a", 50*s),
			observedAt{500 * s, restarted(unreadyPod("a", at(499*s), at(490*s)))},
			observedAt{541 * s, restarted(readyPod("a", at(540*s), 41*s))}), []time.Duration{50 * s}},
		"never Ready":                         {[]observedAt{{-s, createdPod("a")}, {10 * s, startingPod("a", start)}, {600 * s, startingPod("a", start)}}, nil},
		"a replacement pod of the same name":  {[]observedAt{{45 * s, startingPod("a", start)}, {51 * s, replaced(readyPod("a", at(50*s), 50*s))}}, nil},
		"no Ready condition after containers": {[]observedAt{{45 * s, unconditioned(startingPod("a", start))}, {51 * s, readyPod("a", at(50*s), 50*s)}}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			store := NewStartupObservations()
			observeAll(store, test.passes...)
			var got []time.Duration
			for _, item := range store.contract() {
				for _, d := range item.GetReadyAfter() {
					got = append(got, d.AsDuration())
				}
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("observed starts = %v, want %v", got, test.want)
			}
		})
	}
}

func replaced(pod corev1.Pod) corev1.Pod {
	pod.UID = "uid-replacement"
	return pod
}

func unconditioned(pod corev1.Pod) corev1.Pod {
	pod.Status.Conditions = nil
	return pod
}

func TestObservedStartupIsCapped(t *testing.T) {
	store := NewStartupObservations()
	observeAll(store, liveStart("a", 3*time.Hour)...)
	if got := store.Allowances()["Deployment/web"]; got != startupMaxAllowance {
		t.Fatalf("allowance = %s, want the %s cap", got, startupMaxAllowance)
	}
}

// What the install learned survives a controller restart through the state.
func TestObservedStartupSurvivesAControllerRestart(t *testing.T) {
	first := startupController(t, 0, 0, probeSamples(1, func(int) bool { return false }))
	first.config.StartupObservations = NewStartupObservations()
	observeAll(first.config.StartupObservations, liveStart("a", 100*time.Second)...)
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
	// The new process did not see this pod start, so a later Ready is not a sample.
	observeAll(second.config.StartupObservations, observedAt{2000 * time.Second, readyPod("a", observationEpoch.Add(1051*time.Second), 1051*time.Second)})
	if got := second.config.StartupObservations.Allowances()["Deployment/web"]; got != 135*time.Second {
		t.Fatalf("allowance after re-observing = %s, want 135s", got)
	}
}
