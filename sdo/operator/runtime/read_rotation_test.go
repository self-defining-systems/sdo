package runtime

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/vantage"
)

// rotationQueries declares n plain queries of one Pod role on interval, and
// lookups lookups of the first one.
func rotationQueries(n, lookups int, interval time.Duration) []podread.Query {
	target := podread.Target{Image: "synthetic-store"}
	qs := make([]podread.Query, 0, n+lookups)
	for i := range n {
		qs = append(qs, podread.Query{ID: fmt.Sprintf("q-%02d", i), Target: target, Args: []string{"cli", fmt.Sprint(i)}, Interval: interval})
	}
	for i := range lookups {
		qs = append(qs, podread.Query{ID: fmt.Sprintf("q-00-lookup-%d", i), Target: target, Args: []string{"cli", podread.KeysPlaceholder}, Interval: interval, Keys: &podread.Keys{From: "q-00", Prefix: "key ", Max: 4}})
	}
	return qs
}

// recordingExec answers every read and records which queries ran, by their
// first distinct argument.
type recordingExec struct {
	mu  sync.Mutex
	ran []string
}

func (e *recordingExec) Stream(_ context.Context, _ sdk.ObjectRef, _ string, args []string, _ string, stdout io.Writer) error {
	e.mu.Lock()
	e.ran = append(e.ran, fmt.Sprint(args))
	e.mu.Unlock()
	_, err := io.WriteString(stdout, "key a\n")
	return err
}

func (e *recordingExec) take() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := len(e.ran)
	e.ran = nil
	return n
}

// More queries than one cycle may start rotate: no cycle starts more than
// podread.MaxQueries (lookups count with their source), a due query beyond it
// keeps its last answer, and every query is read within its interval plus the
// cycles the rotation needs.
func TestPodReadsRotateUnderThePerCycleBudget(t *testing.T) {
	const n = 30
	now := time.Unix(1000, 0).UTC()
	exec := &recordingExec{}
	queries := rotationQueries(n, 2, 2*podread.MinInterval)
	if err := podread.ValidateQueries(queries); err != nil {
		t.Fatal(err)
	}
	observer := newPodReadObserver(fake.NewSimpleClientset(), exec, "", queries, func() time.Time { return now })
	observer.get = func(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetUID(types.UID("uid-" + ref.Name))
		return object, nil
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{runningStorePod("a")}}
	lastRead := map[string]time.Time{}
	for cycle := range 12 {
		results := observer.collect(context.Background(), snapshot)
		if started := exec.take(); started > podread.MaxQueries {
			t.Fatalf("cycle %d started %d reads, over the per-cycle budget %d", cycle, started, podread.MaxQueries)
		}
		for _, q := range queries {
			result := results[q.ID]
			if len(result.Readings) != 1 {
				if cycle == 0 && result.Error != "" {
					continue // not read yet: no answer to keep
				}
				t.Fatalf("cycle %d query %s = %+v", cycle, q.ID, result)
			}
			reading := result.Readings[0]
			if reading.Error != "" {
				t.Fatalf("cycle %d query %s has no answer: %+v", cycle, q.ID, reading)
			}
			lastRead[q.ID] = reading.ObservedAt
		}
		if cycle >= 1 {
			for _, q := range queries {
				if age := now.Sub(lastRead[q.ID]); age > q.Interval+podread.MinInterval {
					t.Fatalf("cycle %d: query %s answer is %s old", cycle, q.ID, age)
				}
			}
		}
		now = now.Add(podread.MinInterval)
	}
}

// rotationProbes declares n probes of one Pod role on interval.
type rotationProbeDetector struct{ probes []vantage.Probe }

func (d rotationProbeDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "client-links", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}
}
func (d rotationProbeDetector) VantageProbes() []vantage.Probe { return d.probes }
func (rotationProbeDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// Vantage probes rotate under vantage.MaxProbes per cycle as pod reads do.
func TestVantageProbesRotateUnderThePerCycleBudget(t *testing.T) {
	const n = 24
	now := time.Unix(1000, 0).UTC()
	var probes []vantage.Probe
	for i := range n {
		probes = append(probes, vantage.Probe{ID: fmt.Sprintf("p-%02d", i), Kind: vantage.TCPConnect, From: podread.Target{Image: "synthetic-store"}, To: &vantage.Peer{Host: fmt.Sprintf("peer-%d", i), Port: 80}, Interval: 2 * vantage.MinInterval})
	}
	pod := vantagePod(corev1.ContainerStatus{Name: "sdo-vantage-store", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, _ string, stdout io.Writer) error {
		_, err := io.WriteString(stdout, "dial 0 999 0 999 2000 \n")
		return err
	}}
	observer, err := NewVantageObserver(fake.NewSimpleClientset(pod.DeepCopy()), exec, "", "", []sdk.Detector{rotationProbeDetector{probes}}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observer.get = func(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetUID(types.UID("uid-" + ref.Name))
		return object, nil
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}
	lastRead := map[string]time.Time{}
	for cycle := range 10 {
		before := exec.calls.Load()
		results := observer.collect(context.Background(), snapshot)
		if started := exec.calls.Load() - before; started > vantage.MaxProbes {
			t.Fatalf("cycle %d started %d probes, over the per-cycle budget %d", cycle, started, vantage.MaxProbes)
		}
		for _, probe := range probes {
			result := results[probe.ID]
			if len(result.Readings) == 1 && result.Readings[0].Error == "" {
				lastRead[probe.ID] = result.Readings[0].ObservedAt
				continue
			}
			if cycle > 0 {
				t.Fatalf("cycle %d probe %s has no answer: %+v", cycle, probe.ID, result)
			}
		}
		if cycle >= 1 {
			for _, probe := range probes {
				if age := now.Sub(lastRead[probe.ID]); age > probe.Interval+vantage.MinInterval {
					t.Fatalf("cycle %d: probe %s answer is %s old", cycle, probe.ID, age)
				}
			}
		}
		now = now.Add(vantage.MinInterval)
	}
}
