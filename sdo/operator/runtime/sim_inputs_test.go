package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/core"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/generichealth"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/usage"
)

// Exercise the production snapshot, log parsing, termination retention, kubelet
// summary parsing and generic detectors against the same world's raw inputs.
func TestSimRealDetectionInputs(t *testing.T) {
	sim.Run(t, 42, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		pod := corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "api", UID: "api-uid"}, Spec: corev1.PodSpec{NodeName: "node", Containers: []corev1.Container{{Name: "api"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "api", RestartCount: 1, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, FinishedAt: metav1.NewTime(w.Now())}}}}}}
		data, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(&pod)
		if err != nil {
			t.Fatal(err)
		}
		w.Kube.Put(&unstructured.Unstructured{Object: data})
		key := sim.LogKey{Namespace: "app", Pod: "api", Container: "api", Previous: true}
		w.Logs[key] = w.Now().Format(time.RFC3339Nano) + " panic: synthetic crash\n"
		client, err := w.Client()
		if err != nil {
			t.Fatal(err)
		}
		crash := generichealth.NewCrashes(sdk.DetectorSpec{ID: "crashes"})
		observer, err := NewLogsObserver(client, []sdk.Detector{crash}, LogsObserverOptions{Clock: w.Now})
		if err != nil {
			t.Fatal(err)
		}
		base := core.KubernetesSnapshotProvider{Client: client, Namespace: "app", Clock: w.Now}
		provider := LogsSnapshotProvider{Base: base, Observer: observer}
		view, err := provider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		findings, err := crash.Detect(ctx, view)
		if err != nil || len(findings) != 1 {
			t.Fatalf("crash findings: %v %v", findings, err)
		}
		result, err := logs.Require(view, "generic-crashes")
		if err != nil || len(result.Windows[0].Terminations) != 1 {
			t.Fatalf("tail: %+v %v", result, err)
		}
		// The captured tail survives recovery even if kubelet has already discarded it.
		delete(w.Logs, key)
		w.PodAfter(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, time.Second, true)
		w.Advance(time.Second)
		w.Logs[sim.LogKey{Namespace: "app", Pod: "api", Container: "api"}] = w.Now().Format(time.RFC3339Nano) + " ready\n"
		view, err = provider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err = logs.Require(view, "generic-crashes")
		if err != nil || len(result.Windows[0].Terminations) != 1 {
			t.Fatalf("retained tail: %+v %v", result, err)
		}
		summary := map[string]any{"pods": []any{map[string]any{"podRef": map[string]any{"name": "api", "namespace": "app", "uid": "api-uid"}, "containers": []any{map[string]any{"name": "api", "startTime": w.Now().Format(time.RFC3339), "memory": map[string]any{"workingSetBytes": 95}}}}}}
		raw, _ := json.Marshal(summary)
		w.Kubelet["node"] = sim.KubeletSample{Summary: raw}
		saturation := generichealth.NewSaturation(sdk.DetectorSpec{ID: "usage"}, false)
		usageProvider := withUsage(base, []sdk.Detector{saturation}, client, w.Now)
		view, err = usageProvider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		u, err := usage.From(view)
		if err != nil || len(u.Containers) != 1 || u.Containers[0].Samples[0].WorkingSetBytes == nil {
			t.Fatalf("usage: %+v %v", u, err)
		}
		w.Faults.Rules["kubelet"] = []sim.Fault{{Err: context.DeadlineExceeded}}
		w.Advance(16 * time.Second)
		view, err = usageProvider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = usage.From(view); err == nil {
			t.Fatal("failed kubelet read reported healthy")
		}
	})
}
