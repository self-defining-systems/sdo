package runtime

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
)

// versionedExec answers every read with the current version, so a test can
// tell a fresh answer from a kept one.
type versionedExec struct{ version atomic.Int32 }

func (e *versionedExec) Stream(_ context.Context, _ sdk.ObjectRef, _ string, _ []string, _ string, stdout io.Writer) error {
	_, err := fmt.Fprintf(stdout, "key v%d\n", e.version.Load())
	return err
}

func freshObserver(t *testing.T, queries []podread.Query, exec podReadExecStream, now *time.Time) *PodReadObserver {
	t.Helper()
	if err := podread.ValidateQueries(queries); err != nil {
		t.Fatal(err)
	}
	observer := newPodReadObserver(fake.NewSimpleClientset(), exec, "", queries, func() time.Time { return *now })
	observer.get = func(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetUID(types.UID("uid-" + ref.Name))
		return object, nil
	}
	return observer
}

type podReadExecStream interface {
	Stream(context.Context, sdk.ObjectRef, string, []string, string, io.Writer) error
}

// A fresh read ignores an answer kept from within the query's interval: the
// incident-open read sees the store as it is now, not as it was before the
// fault. A cycle without the request still keeps the answer.
func TestFreshPodReadIgnoresKeptAnswer(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	exec := &versionedExec{}
	exec.version.Store(1)
	queries := rotationQueries(2, 1, 2*podread.MinInterval)
	observer := freshObserver(t, queries, exec, &now)
	snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{runningStorePod("a")}}
	observer.collect(context.Background(), snapshot)
	exec.version.Store(2)
	now = now.Add(podread.MinInterval / 2)

	kept := observer.collect(context.Background(), snapshot)
	if got := kept["q-00"].Readings[0].Stdout; !strings.Contains(got, "v1") {
		t.Fatalf("a scheduled cycle within the interval must keep its answer, got %q", got)
	}
	request := &freshPodReads{want: func(q podread.Query) bool { return q.ID == "q-00-lookup-0" }}
	results := observer.collect(withFreshPodReads(context.Background(), request), snapshot)
	for _, id := range []string{"q-00", "q-00-lookup-0"} {
		if got := results[id].Readings[0]; got.Error != "" || !strings.Contains(got.Stdout, "v2") || !got.ObservedAt.Equal(now) {
			t.Fatalf("%s must be read now (a lookup with its source), got %+v", id, got)
		}
		if request.unread(id) {
			t.Fatalf("%s answered, yet it is listed as unread", id)
		}
	}
	if got := results["q-01"].Readings[0].Stdout; !strings.Contains(got, "v1") {
		t.Fatalf("a query the request does not select keeps its answer, got %q", got)
	}
}

// Forced reads count against podread.MaxQueries and start first. One the
// budget leaves unread is an explicit unavailable answer, never the kept one,
// and is listed as unread so the caller asks again.
func TestFreshPodReadsCountAgainstTheLoadBudget(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	exec := &versionedExec{}
	exec.version.Store(1)
	const n = podread.MaxQueries + 4
	queries := rotationQueries(n, 0, 2*podread.MinInterval)
	observer := freshObserver(t, queries, exec, &now)
	snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{runningStorePod("a")}}
	for range 3 {
		observer.collect(context.Background(), snapshot)
	}
	exec.version.Store(2)
	now = now.Add(time.Second)
	counting := &countingExec{inner: exec}
	observer.exec = counting
	request := &freshPodReads{want: func(podread.Query) bool { return true }}
	results := observer.collect(withFreshPodReads(context.Background(), request), snapshot)
	if started := counting.calls.Load(); started > podread.MaxQueries {
		t.Fatalf("the fresh read started %d reads, over the per-cycle budget %d", started, podread.MaxQueries)
	}
	fresh, unread := 0, 0
	for _, q := range queries {
		result := results[q.ID]
		switch {
		case request.unread(q.ID):
			unread++
			if result.Error == "" || len(result.Readings) != 0 {
				t.Fatalf("unread %s must say it has no current answer, got %+v", q.ID, result)
			}
		case len(result.Readings) == 1 && strings.Contains(result.Readings[0].Stdout, "v2"):
			fresh++
		default:
			t.Fatalf("%s returned a kept answer as current: %+v", q.ID, result)
		}
	}
	if fresh != podread.MaxQueries || unread != n-podread.MaxQueries {
		t.Fatalf("fresh %d, unread %d; want the budget's %d fresh and the rest unread", fresh, unread, podread.MaxQueries)
	}
	again := &freshPodReads{want: func(podread.Query) bool { return true }}
	now = now.Add(time.Second)
	results = observer.collect(withFreshPodReads(context.Background(), again), snapshot)
	for _, q := range queries {
		if request.unread(q.ID) && again.unread(q.ID) {
			t.Fatalf("%s was unread twice: the next fresh read must start the ones left over first", q.ID)
		}
	}
	_ = results
}

type countingExec struct {
	inner podReadExecStream
	calls atomic.Int32
}

func (e *countingExec) Stream(ctx context.Context, pod sdk.ObjectRef, container string, args []string, shell string, stdout io.Writer) error {
	e.calls.Add(1)
	return e.inner.Stream(ctx, pod, container, args, shell, stdout)
}
