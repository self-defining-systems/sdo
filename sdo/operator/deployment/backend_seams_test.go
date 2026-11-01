package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestObserveRetriesWithInjectedWaitAndCommands(t *testing.T) {
	image := "registry.example/app@sha256:" + strings.Repeat("a", 64)
	workload := map[string]any{
		"kind":     "Deployment",
		"metadata": map[string]any{"namespace": "default", "name": "app", "uid": "uid", "generation": 1, "annotations": map[string]any{"sdo.dev/source-tree": "tree"}},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"sdo.dev/source-tree": "tree"}}, "spec": map[string]any{"containers": []any{map[string]any{"image": image}}}}},
		"status":   map[string]any{"observedGeneration": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1},
	}
	reads, waits := 0, 0
	b := CommandBackend{KubectlBinary: "fake-kubectl", KubeContext: "test",
		Run: func(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
			reads++
			if name != "fake-kubectl" || strings.Join(args, " ") != "--context test get Deployment app --namespace default --output=json" || input != nil {
				t.Fatalf("unexpected observation command: %s %v %q", name, args, input)
			}
			if reads == 1 {
				return nil, errors.New("transient read")
			}
			return json.Marshal(workload)
		},
		Wait: func(ctx context.Context, d time.Duration) error {
			waits++
			if d != 2*time.Second {
				t.Fatalf("poll interval %v", d)
			}
			return nil
		},
	}
	observed, err := b.Observe(context.Background(), "Deployment/default/app")
	if err != nil || !observed.Ready || observed.Revision != "tree" || reads != 2 || waits != 1 {
		t.Fatalf("observe: %+v, %v; reads=%d waits=%d", observed, err, reads, waits)
	}
}

func TestObservePreservesLastErrorOnInjectedCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := CommandBackend{Run: func(context.Context, string, []string, []byte) ([]byte, error) {
		return nil, errors.New("read unavailable")
	}, Wait: func(context.Context, time.Duration) error { cancel(); return ctx.Err() }}
	_, err := b.Observe(ctx, "Deployment/default/app")
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "read unavailable") {
		t.Fatalf("lost observation context: %v", err)
	}
}
