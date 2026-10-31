package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/runtime/internal/testprotocol"
	"sdo.dev/operator/sdk"
)

// World owns one RNG and a serial event schedule. Run constructs it inside a
// synctest bubble; standard Go timers and injected Now/Sleep then share time.
type World struct {
	// Endpoints maps a cluster service address (host:port) to its link protocol.
	Endpoints map[string]string
	Logs      map[LogKey]string
	Kubelet   map[string]KubeletSample
	Seed      uint64
	RNG       *rand.Rand
	Kube      *Kube
	Cache     *Cache
	Faults    *Injector
	events    []event
	hooks     []func(Event) error
}
type event struct {
	at    time.Time
	apply func()
}

// Event is an invariant observation emitted by a scenario or edge adapter.
type Event struct {
	Kind     string
	Resource sdk.ObjectRef
	At       time.Time
}

// Run isolates all timers and goroutines in a virtual-time bubble.
func Run(t *testing.T, seed uint64, body func(*testing.T, *World)) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		rng := rand.New(rand.NewPCG(seed, seed^0xd1b54a32d192ed03))
		kube := NewKube(QuietFaults())
		w := &World{Endpoints: map[string]string{}, Logs: map[LogKey]string{}, Kubelet: map[string]KubeletSample{}, Seed: seed, RNG: rng, Kube: kube, Cache: NewCache(kube, QuietFaults())}
		w.Faults = &Injector{rng: rng, Rules: map[string][]Fault{}}
		kube.Clock = w.Now
		kube.Before = func(ctx context.Context, edge string) (Fault, error) {
			f := w.Faults.Next(edge)
			if err := w.Sleep(ctx, f.Delay); err != nil {
				return f, err
			}
			return f, f.Err
		}
		body(t, w)
	})
}
func (w *World) Now() time.Time { return time.Now() }

// Sleep advances virtual timers while preserving context cancellation.
func (w *World) wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (w *World) Sleep(ctx context.Context, d time.Duration) error {
	end := w.Now().Add(d)
	for {
		next := -1
		for i, e := range w.events {
			if !e.at.After(end) && (next < 0 || e.at.Before(w.events[next].at)) {
				next = i
			}
		}
		if next < 0 {
			break
		}
		e := w.events[next]
		if err := w.wait(ctx, e.at.Sub(w.Now())); err != nil {
			return err
		}
		w.events = append(w.events[:next], w.events[next+1:]...)
		e.apply()
	}
	return w.wait(ctx, end.Sub(w.Now()))
}

// After schedules a cluster transition. Equal-time events retain insertion order.
func (w *World) After(d time.Duration, f func()) {
	w.events = append(w.events, event{w.Now().Add(d), f})
}

// Advance executes scheduled events chronologically, including events created by events.
func (w *World) Advance(d time.Duration) {
	if d < 0 {
		panic("negative simulation duration")
	}
	_ = w.Sleep(context.Background(), d)
}

// Invariant registers an assertion checked at every emitted boundary.
func (w *World) Invariant(h func(Event) error) { w.hooks = append(w.hooks, h) }
func (w *World) Emit(kind string, ref sdk.ObjectRef) error {
	for _, h := range w.hooks {
		if err := h(Event{kind, ref, w.Now()}); err != nil {
			return fmt.Errorf("seed=%d %s: %w", w.Seed, kind, err)
		}
	}
	return nil
}

// PodAfter models kubelet crash/backoff and readiness transitions. A repeated
// backoff event is refreshed after a minute, just as Kubernetes aggregates Events.
func (w *World) PodAfter(ref sdk.ObjectRef, d time.Duration, ready bool) {
	w.After(d, func() {
		obj, err := w.Kube.Get(context.Background(), ref)
		if err != nil {
			panic(err)
		}
		var pod corev1.Pod
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &pod); err != nil {
			panic(err)
		}
		now := metav1.NewTime(w.Now())
		pod.Status.Phase = corev1.PodRunning
		state := corev1.ConditionFalse
		if ready {
			state = corev1.ConditionTrue
		}
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: state, LastTransitionTime: now}}
		oldStatuses := pod.Status.ContainerStatuses
		pod.Status.ContainerStatuses = nil
		for _, c := range pod.Spec.Containers {
			status := corev1.ContainerStatus{Name: c.Name, Ready: ready}
			for _, old := range oldStatuses {
				if old.Name == c.Name {
					status.RestartCount = old.RestartCount
					status.LastTerminationState = old.LastTerminationState
				}
			}
			if !ready {
				status.RestartCount++
			}
			if ready {
				status.State.Running = &corev1.ContainerStateRunning{StartedAt: now}
			} else {
				status.State.Waiting = &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}
				status.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ExitCode: 1, FinishedAt: now}
			}
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, status)
		}
		data, _ := kruntime.DefaultUnstructuredConverter.ToUnstructured(&pod)
		w.Kube.Put(&unstructured.Unstructured{Object: data})
		if !ready {
			first := now
			emit := func(count int32) {
				ev := corev1.Event{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Event"}, ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name + "-backoff"}, InvolvedObject: corev1.ObjectReference{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}, Type: "Warning", Reason: "BackOff", Count: count, FirstTimestamp: first, LastTimestamp: metav1.NewTime(w.Now())}
				data, _ := kruntime.DefaultUnstructuredConverter.ToUnstructured(&ev)
				w.Kube.Put(&unstructured.Unstructured{Object: data})
			}
			emit(1)
			w.After(time.Minute, func() {
				current, err := w.Kube.Get(context.Background(), ref)
				if err != nil || current.GetUID() != pod.UID {
					return
				}
				var latest corev1.Pod
				if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(current.Object, &latest); err != nil {
					panic(err)
				}
				for _, status := range latest.Status.ContainerStatuses {
					for _, crashed := range pod.Status.ContainerStatuses {
						if status.Name == crashed.Name && status.RestartCount == crashed.RestartCount && status.State.Waiting != nil && status.State.Waiting.Reason == "CrashLoopBackOff" {
							emit(2)
							return
						}
					}
				}
			})
		}
	})
}

// Fault is one scheduled external-call result. CommitThenTimeout is consumed by
// API adapters; DropWatch freezes a cache until an explicit relist.
type Fault struct {
	Err                          error
	Delay                        time.Duration
	CommitThenTimeout, DropWatch bool
	ReplyMode                    string
}

// Injector consumes one seeded schedule in call order. Explicit rules override
// random transient errors; partitions last until the scenario clears them.
type Injector struct {
	rng            *rand.Rand
	Rules          map[string][]Fault
	Partition      map[string]bool
	RandomFailures bool
}

func (i *Injector) Next(edge string) Fault {
	if i.Partition[edge] {
		return Fault{Err: fmt.Errorf("partition: %s", edge)}
	}
	if queue := i.Rules[edge]; len(queue) > 0 {
		i.Rules[edge] = queue[1:]
		return queue[0]
	}
	if i.RandomFailures && i.rng.IntN(8) == 0 {
		return Fault{Err: fmt.Errorf("seeded failure: %s", edge)}
	}
	return Fault{}
}
func (w *World) Read(ctx context.Context, edge string) error {
	f := w.Faults.Next(edge)
	if err := w.Sleep(ctx, f.Delay); err != nil {
		return err
	}
	return f.Err
}

// Dial supplies real wire handshakes over net.Pipe, without host sockets.
func (w *World) Dial(ctx context.Context, protocol string) (net.Conn, error) {
	f := w.Faults.Next("probe/" + protocol)
	if err := w.Sleep(ctx, f.Delay); err != nil {
		return nil, err
	}
	if f.Err != nil {
		return nil, f.Err
	}
	mode := f.ReplyMode
	if mode == "" {
		mode = "healthy"
	}
	return testprotocol.Dial(ctx, protocol, mode)
}

// Sweep caps PR campaigns at 45 seconds of host time. Each seed gets its own
// bubble so virtual durations do not consume the campaign's host-time budget.
func Sweep(t *testing.T, body func(*testing.T, *World)) {
	t.Helper()
	runs := 256
	var replay *uint64
	if raw := os.Getenv("SDO_SIM_SEED"); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		replay = &n
		runs = 1
	} else if raw := os.Getenv("SDO_SIM_RUNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			t.Fatalf("invalid SDO_SIM_RUNS %q", raw)
		}
		runs = n
	}
	deadline := time.Now().Add(45 * time.Second)
	for n := 0; n < runs; n++ {
		if n > 0 && time.Now().After(deadline) {
			t.Logf("wall budget: completed %d/%d seeds", n, runs)
			break
		}
		seed := uint64(n + 1)
		if replay != nil {
			seed = *replay
		}
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) { Run(t, seed, body) })
	}
}

// RolloutAfter models readiness lag for a deployment. Pods remain explicit so
// tests can independently fault individual replicas during rollout.
func (w *World) RolloutAfter(ref sdk.ObjectRef, d time.Duration) {
	obj, err := w.Kube.Get(context.Background(), ref)
	if err != nil {
		panic(err)
	}
	_ = unstructured.SetNestedField(obj.Object, int64(0), "status", "availableReplicas")
	w.Kube.Put(obj)
	w.After(d, func() {
		obj, err := w.Kube.Get(context.Background(), ref)
		if err != nil {
			panic(err)
		}
		replicas, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
		if !ok {
			replicas = 1
		}
		for _, field := range []string{"replicas", "updatedReplicas", "readyReplicas", "availableReplicas"} {
			_ = unstructured.SetNestedField(obj.Object, replicas, "status", field)
		}
		_ = unstructured.SetNestedField(obj.Object, obj.GetGeneration(), "status", "observedGeneration")
		w.Kube.Put(obj)
	})
}

// DialContext plugs into prober.Config and routes service addresses without DNS.
func (w *World) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if err := w.Read(ctx, "network/"+address); err != nil {
		return nil, err
	}
	protocol, ok := w.Endpoints[address]
	if !ok {
		return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
	}
	return w.Dial(ctx, protocol)
}
