package sim_test

import (
	"context"
	"encoding/json"
	"errors"
	"k8s.io/apimachinery/pkg/types"
	"sort"
	"testing"
	"time"

	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/core"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/generichealth"
)

func TestCrashLoopRecovery(t *testing.T) {
	sim.Run(t, 17, func(t *testing.T, w *sim.World) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "api"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api"}}}}
		obj, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(pod)
		if err != nil {
			t.Fatal(err)
		}
		obj["apiVersion"] = "v1"
		obj["kind"] = "Pod"
		w.Kube.Put(&unstructured.Unstructured{Object: obj})
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
		w.PodAfter(ref, time.Second, false)
		w.PodAfter(ref, 2*time.Minute, true)
		detector := generichealth.NewWarnings(sdk.DetectorSpec{ID: "warnings"})
		w.Advance(time.Minute + time.Second)
		client, err := w.Client()
		if err != nil {
			t.Fatal(err)
		}
		provider := core.KubernetesSnapshotProvider{Client: client, Namespace: "app", Clock: w.Now}
		view, err := provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		findings, err := detector.Detect(context.Background(), view)
		if err != nil || len(findings) != 1 {
			t.Fatalf("crash: %v %v", findings, err)
		}
		w.Advance(time.Minute)
		view, err = provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		findings, err = detector.Detect(context.Background(), view)
		if err != nil || len(findings) != 0 {
			t.Fatalf("recovery: %v %v", findings, err)
		}
	})
}

func TestSeededInvariants(t *testing.T) {
	sim.Sweep(t, func(t *testing.T, w *sim.World) {
		diagnosed, healthy := false, false
		w.Invariant(func(e sim.Event) error {
			if e.Kind == "mutation" && !diagnosed {
				return fmt.Errorf("mutation without diagnosis")
			}
			if e.Kind == "closure" && !healthy {
				return fmt.Errorf("closure without health")
			}
			return nil
		})
		// Assert the hooks reject invalid ordering, rather than only testing good traces.
		if w.Emit("mutation", sdk.ObjectRef{}) == nil || w.Emit("closure", sdk.ObjectRef{}) == nil {
			t.Fatal("invariants did not reject invalid events")
		}
		for n := 0; n < 20; n++ {
			switch w.RNG.IntN(4) {
			case 0:
				diagnosed = true
			case 1:
				if diagnosed {
					if err := w.Emit("mutation", sdk.ObjectRef{}); err != nil {
						t.Fatal(err)
					}
					healthy = false
				}
			case 2:
				healthy = true
			case 3:
				if healthy {
					if err := w.Emit("closure", sdk.ObjectRef{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			w.Advance(time.Second)
		}
	})
}

func TestFaultSchedule(t *testing.T) {
	sim.Run(t, 3, func(t *testing.T, w *sim.World) {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
		w.Kube.Put(&unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"type": "ClusterIP"}}})
		obj, _ := w.Kube.Get(context.Background(), ref)
		patch := []byte(fmt.Sprintf(`{"metadata":{"uid":%q,"resourceVersion":%q},"spec":{"type":"NodePort"}}`, obj.GetUID(), obj.GetResourceVersion()))
		w.Faults.Rules["patch"] = []sim.Fault{{CommitThenTimeout: true}}
		if err := w.Kube.Patch(context.Background(), ref, types.MergePatchType, patch); !errors.Is(err, sim.ErrTimeout) {
			t.Fatalf("timeout: %v", err)
		}
		obj, _ = w.Kube.Get(context.Background(), ref)
		value, _, _ := unstructured.NestedString(obj.Object, "spec", "type")
		if value != "NodePort" {
			t.Fatal("write did not commit")
		}
		w.Faults.Rules["snapshot"] = []sim.Fault{{DropWatch: true}}
		_, _ = w.Cache.Snapshot(context.Background())
		_ = unstructured.SetNestedField(obj.Object, "LoadBalancer", "spec", "type")
		w.Kube.Put(obj)
		view, _ := w.Cache.Snapshot(context.Background())
		if view.Services()[0].Spec.Type != corev1.ServiceTypeNodePort {
			t.Fatal("dropped watch delivered update")
		}
		w.Cache.CatchUp()
		view, _ = w.Cache.Snapshot(context.Background())
		if view.Services()[0].Spec.Type != corev1.ServiceTypeLoadBalancer {
			t.Fatal("relist did not catch up")
		}
		start := w.Now()
		w.Faults.Rules["logs"] = []sim.Fault{{Delay: time.Hour, Err: context.DeadlineExceeded}}
		if err := w.Read(context.Background(), "logs"); err == nil || w.Now().Sub(start) != time.Hour {
			t.Fatal("slow read fault")
		}
		w.Faults.Partition = map[string]bool{"probe/http": true}
		if _, err := w.Dial(context.Background(), "http"); err == nil {
			t.Fatal("partition allowed probe")
		}
	})
}

func TestSnapshotKindsAndRollout(t *testing.T) {
	sim.Run(t, 8, func(t *testing.T, w *sim.World) {
		kinds := map[string]string{"ConfigMap": "v1", "Service": "v1", "Pod": "v1", "Deployment": "apps/v1", "StatefulSet": "apps/v1", "ReplicaSet": "apps/v1", "HorizontalPodAutoscaler": "autoscaling/v2", "PodDisruptionBudget": "policy/v1", "Endpoints": "v1", "EndpointSlice": "discovery.k8s.io/v1", "NetworkPolicy": "networking.k8s.io/v1", "Event": "v1", "DaemonSet": "apps/v1", "Job": "batch/v1", "CronJob": "batch/v1", "PersistentVolumeClaim": "v1", "ServiceAccount": "v1", "Ingress": "networking.k8s.io/v1", "Node": "v1", "Secret": "v1"}
		names := make([]string, 0, len(kinds))
		for kind := range kinds {
			names = append(names, kind)
		}
		sort.Strings(names)
		for _, kind := range names {
			ns := "app"
			if kind == "Node" {
				ns = ""
			}
			w.Kube.Put(&unstructured.Unstructured{Object: map[string]any{"apiVersion": kinds[kind], "kind": kind, "metadata": map[string]any{"namespace": ns, "name": "example"}}})
		}
		view, err := w.Cache.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(view)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err = json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		lists := 0
		for _, value := range fields {
			if items, ok := value.([]any); ok && len(items) == 1 {
				lists++
			}
		}
		if lists != len(kinds) {
			t.Fatalf("modeled %d/%d kinds: %s", lists, len(kinds), data)
		}
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "example"}
		w.RolloutAfter(ref, time.Minute)
		obj, _ := w.Kube.Get(context.Background(), ref)
		n, _, _ := unstructured.NestedInt64(obj.Object, "status", "availableReplicas")
		if n != 0 {
			t.Fatal("rollout completed early")
		}
		// A controller's injected sleep must drive cluster events as well as timers.
		if err = w.Sleep(context.Background(), time.Minute); err != nil {
			t.Fatal(err)
		}
		obj, _ = w.Kube.Get(context.Background(), ref)
		n, _, _ = unstructured.NestedInt64(obj.Object, "status", "availableReplicas")
		if n != 1 {
			t.Fatal("rollout did not progress")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := w.Now()
		if w.Sleep(ctx, time.Hour) == nil || w.Now() != start {
			t.Fatal("cancelled sleep advanced clock")
		}
	})
}

func TestRecoveryStopsBackoffEvents(t *testing.T) {
	sim.Run(t, 9, func(t *testing.T, w *sim.World) {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
		w.Kube.Put(&unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "api"}}}}})
		w.PodAfter(ref, time.Second, false)
		w.PodAfter(ref, 10*time.Second, true)
		w.Advance(2 * time.Minute)
		view, err := w.Cache.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if events := view.Events(); len(events) != 1 || events[0].Count != 1 {
			t.Fatalf("recovered pod continued backoff: %+v", events)
		}
	})
}
