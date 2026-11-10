package runtime

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
)

// argsPodExec answers by the container read and records each command.
type argsPodExec struct {
	mu      sync.Mutex
	answers map[string]string
	calls   map[string][][]string
}

func (e *argsPodExec) Stream(_ context.Context, _ sdk.ObjectRef, container string, args []string, _ string, stdout io.Writer) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls[container] = append(e.calls[container], args)
	_, err := io.WriteString(stdout, e.answers[container])
	return err
}

func lookupQueries() []podread.Query {
	return []podread.Query{
		{ID: "source", Target: podread.Target{Image: "synthetic-store"}, Args: []string{"store-cli", "scan"}, Interval: time.Minute},
		{ID: "lookup", Target: podread.Target{Image: "proxy"}, Args: []string{"index-cli", "get " + podread.KeysPlaceholder}, Interval: time.Minute,
			Keys: &podread.Keys{From: "source", Prefix: "K\t", Max: 8}},
	}
}

func TestPodReadLookupRunsWithTheKeysItsSourcePrintedInTheSameCycle(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	exec := &argsPodExec{answers: map[string]string{"store": "R\t1\nK\t7\nK\t9\n", "sidecar": "found 7\n"}, calls: map[string][][]string{}}
	observer := podReadObserver(t, &scriptedPodExec{}, func() time.Time { return now })
	observer.exec, observer.queries = exec, lookupQueries()
	pod := runningStorePod("a")
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: "sidecar", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	snapshot := storeSnapshot(pod)

	results := observer.collect(context.Background(), snapshot)
	got := results["lookup"]
	if got.Error != "" || len(got.Readings) != 1 || got.Readings[0].Stdout != "found 7\n" {
		t.Fatalf("lookup result = %+v", got)
	}
	if args := exec.calls["sidecar"][0]; strings.Join(args, "|") != "index-cli|get 7 9" {
		t.Fatalf("lookup ran with %q, want the source's keys", args)
	}
	if got.Query.Args[1] != "get 7 9" {
		t.Fatalf("the result records the command that ran: %v", got.Query.Args)
	}

	// Within the interval both keep their answers; when the source runs
	// again, the lookup follows it with the new keys.
	now = now.Add(30 * time.Second)
	observer.collect(context.Background(), snapshot)
	if len(exec.calls["store"]) != 1 || len(exec.calls["sidecar"]) != 1 {
		t.Fatalf("calls within the interval = %v", exec.calls)
	}
	exec.answers["store"] = "K\t3\n"
	now = now.Add(30 * time.Second)
	observer.collect(context.Background(), snapshot)
	if calls := exec.calls["sidecar"]; len(calls) != 2 || calls[1][1] != "get 3" {
		t.Fatalf("lookup after the source reran = %v", calls)
	}

	// A source without a complete answer leaves the lookup unavailable and
	// never runs it with partial keys.
	exec.answers["store"] = strings.Repeat("K\t1\n", podread.LimitBytes)
	now = now.Add(time.Minute)
	results = observer.collect(context.Background(), snapshot)
	if got := results["lookup"]; got.Error == "" || len(got.Readings) != 0 || len(exec.calls["sidecar"]) != 2 {
		t.Fatalf("lookup of a truncated source = %+v, calls %d", got, len(exec.calls["sidecar"]))
	}
}
