package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// effectWorld is detectApp with the create test failing throughout, a
// responder write to Deployment/api at writeAt, and a rollout that completes
// on read rolledAt (0: never). Each read appends one scheduled create sample
// at the read's time, so time and samples advance together.
type effectWorld struct {
	mu        sync.Mutex
	clock     *clocktesting.FakeClock
	writeAt   time.Time
	rolledAt  int
	crashLoop bool
	reads     int
	samples   []traffic.Sample
}

func (w *effectWorld) Snapshot(context.Context) (sdk.DetectionContext, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reads++
	now := w.clock.Now()
	view := detectApp("create")
	w.samples = append(w.samples, traffic.Sample{At: now, Outcome: traffic.OutcomeError, Status: 500, Step: "write-row"})
	window := view.Traffic["health"]
	for i := range window.Scenarios {
		if window.Scenarios[i].Scenario.ID == "create" {
			window.Scenarios[i].Samples = append(window.Scenarios[i].Samples, w.samples...)
		}
	}
	view.Traffic["health"] = window
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}
	for i := range view.DeploymentList {
		d := &view.DeploymentList[i]
		d.Spec.Selector = selector
		d.Generation, d.Status.ObservedGeneration = 1, 1
		d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.AvailableReplicas = 1, 1, 1
		if d.Name != "api" || w.writeAt.IsZero() {
			continue
		}
		d.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: sdk.ResponderFieldManager, Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: w.writeAt}}}
		if w.rolledAt == 0 || w.reads < w.rolledAt {
			d.Generation = 2
			d.Status.UpdatedReplicas, d.Status.Replicas = 0, 2
		}
	}
	if w.crashLoop {
		view.PodList = append(view.PodList, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "api-new", Namespace: "app", Labels: map[string]string{"app": "api"}, CreationTimestamp: metav1.NewTime(w.writeAt)},
			Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}},
		})
	}
	return view, nil
}

func effectLoop(world *effectWorld) (*ResponderLoop, autoClock) {
	clock := autoClock{clocktesting.NewFakeClock(detectNow())}
	world.clock = clock.FakeClock
	loop, _ := detectWorldLoop(&detectWorld{})
	loop.Provider = world
	loop.Traffic = nil
	loop.Now = clock.Now
	loop.WaitClock = clock
	return loop, clock
}

func waitLine(text string) string { return strings.SplitN(text, "\n", 2)[0] }

func TestDetectWaitReturnsWhenTheChangeRolledOutAndAFreshSampleRan(t *testing.T) {
	world := &effectWorld{writeAt: detectNow().Add(-3 * time.Second), rolledAt: 3}
	loop, clock := effectLoop(world)
	started := clock.Now()
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 120})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	line := waitLine(response.Text)
	// Reads at 0s and 2s see the rollout pending; the 4s read sees it done
	// (first seen then) and the same read's sample at 4s is the fresh one.
	if got := clock.Since(started); got != 4*time.Second {
		t.Fatalf("waited %s, want 4s (rollout done, then its sample), not the bound:\n%s", got, line)
	}
	for _, want := range []string{"wait: after 4s, result of your change at 07:28:40Z (Deployment/api) is in:", "Deployment/api rolled out", "user test create FAIL"} {
		if !strings.Contains(line, want) {
			t.Fatalf("first line lacks %q: %q", want, line)
		}
	}
	// The same change is not reported again: the next wait runs to its bound
	// and says what it waited for.
	again := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 6})
	if line := waitLine(again.Text); !strings.HasPrefix(line, "wait: no change in 6s; waited for an alert change (no write of yours since 07:28:40Z)") {
		t.Fatalf("second wait = %q", line)
	}
}

func TestDetectWaitNamesThePendingRolloutAtTheBound(t *testing.T) {
	world := &effectWorld{writeAt: detectNow().Add(-3 * time.Second)}
	loop, _ := effectLoop(world)
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 6})
	want := "wait: no change in 6s; waited for your change at 07:28:40Z (Deployment/api): Deployment/api rollout (0/1 updated, 1 available)"
	if line := waitLine(response.Text); line != want {
		t.Fatalf("first line = %q, want %q", line, want)
	}
}

func TestDetectWaitReturnsOnAFailedRollout(t *testing.T) {
	world := &effectWorld{writeAt: detectNow().Add(-3 * time.Second), crashLoop: true}
	loop, clock := effectLoop(world)
	started := clock.Now()
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 120})
	if got := clock.Since(started); got != 0 {
		t.Fatalf("waited %s for a rollout that cannot finish", got)
	}
	if line := waitLine(response.Text); !strings.Contains(line, "is in: Deployment/api rollout failed: Pod api-new CrashLoopBackOff") {
		t.Fatalf("first line = %q", line)
	}
}

func TestDetectWaitIgnoresWritesBeforeTheIncident(t *testing.T) {
	world := &effectWorld{writeAt: detectRequest().DetectedAt.Add(-time.Minute), rolledAt: 1}
	loop, _ := effectLoop(world)
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 4})
	if line := waitLine(response.Text); !strings.HasPrefix(line, "wait: no change in 4s; waited for an alert change") {
		t.Fatalf("first line = %q", line)
	}
}

func TestEffectWatchNeedsASampleAfterTheRolloutWasSeenDone(t *testing.T) {
	at := detectNow()
	view := detectApp("create")
	view.DeploymentList = []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Status: appsv1.DeploymentStatus{Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1}}}
	change := knownChange{At: at.Add(-30 * time.Second), Workloads: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}, Writes: []string{"Deployment/api"}}
	var watch effectWatch
	// Every sample of detectApp is older than the read that first sees the
	// rollout done, so none of them is the result.
	if result := watch.evaluate(view, change, []string{"create"}, at.Add(time.Minute)); result.in {
		t.Fatalf("a sample older than the observed rollout counted: %q", result.line)
	}
	if result := watch.evaluate(view, change, nil, at.Add(time.Minute)); !result.in {
		t.Fatalf("with no failing user test the rollout alone is the result: %q", result.line)
	}
}
