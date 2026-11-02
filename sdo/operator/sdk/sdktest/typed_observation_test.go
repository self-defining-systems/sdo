package sdktest

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
)

func TestTypedObservationsPreserveIdentityWhenCorpusRenames(t *testing.T) {
	c := renameFixture(t)
	ref := sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "front-1"}
	timestamp := metav1.NewTime(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC))
	sample := metricsv1beta1.PodMetrics{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}, Containers: []metricsv1beta1.ContainerMetrics{{Name: "app", Usage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("125m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}}, Timestamp: timestamp, Window: metav1.Duration{Duration: 30 * time.Second}}
	options := corev1.PodLogOptions{Container: "app", Previous: true, SinceTime: &timestamp}
	add := func(method string, args, result any) {
		a, _ := json.Marshal(args)
		r, _ := json.Marshal(result)
		c.Recordings = append(c.Recordings, PrimitiveCall{Method: method, Arguments: a, Result: r})
	}
	add("LogsWithOptions", []any{ref, options}, "front failed")
	add("PodUsage", []any{ref}, sample)
	node := sdk.ObjectRef{Kind: "Node", Name: "worker"}
	nodeSample := metricsv1beta1.NodeMetrics{ObjectMeta: metav1.ObjectMeta{Name: node.Name}, Usage: sample.Containers[0].Usage, Timestamp: timestamp, Window: sample.Window}
	add("NodeUsage", []any{node}, nodeSample)
	renamed, err := renameCorpusCase(c)
	if err != nil {
		t.Fatal(err)
	}
	replay := &ReplayReadView{Calls: renamed.Recordings}
	renamedRef := ref
	renamedRef.Name = "front-1-rx7"
	options.Container = "app-rx7"
	logs, err := replay.LogsWithOptions(renamedRef, options)
	if err != nil || logs != "front-rx7 failed" {
		t.Fatalf("renamed logs=%q %v", logs, err)
	}
	got, err := replay.PodUsage(renamedRef)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != renamedRef.Name || got.Containers[0].Name != "app-rx7" || !got.Timestamp.Equal(&timestamp) || got.Containers[0].Usage.Cpu().Cmp(*sample.Containers[0].Usage.Cpu()) != 0 || got.Window != sample.Window {
		t.Fatalf("renamed sample changed observation: %+v", got)
	}
	n, err := replay.NodeUsage(node)
	if err != nil || n.Name != node.Name {
		t.Fatalf("cluster identity changed: %+v %v", n, err)
	}
}

func TestUsageReplayRejectsMissingOrMismatchedSamples(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "front-1"}
	for _, tc := range []struct {
		name   string
		result any
	}{
		{"null", nil}, {"empty", metricsv1beta1.PodMetrics{}}, {"missing metadata", metricsv1beta1.PodMetrics{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, _ := json.Marshal([]any{ref})
			result, _ := json.Marshal(tc.result)
			replay := &ReplayReadView{Calls: []PrimitiveCall{{Method: "PodUsage", Arguments: args, Result: result}}}
			if _, err := replay.PodUsage(ref); err == nil {
				t.Fatalf("missing usage became a successful read: %s", result)
			}
		})
	}
}
