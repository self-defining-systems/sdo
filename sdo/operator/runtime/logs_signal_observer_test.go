package runtime

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
)

// signalObserver reads one running container of a Deployment's Pod whose log
// is lines(now) at each collection, with the clock advanced by the caller.
func signalObserver(now *time.Time, lines func(time.Time) []string) (*LogsObserver, sdktest.Snapshot) {
	q := logs.Query{ID: "q", Patterns: []logs.Pattern{{ID: "denied", RE2: "denied"}, {ID: "done", RE2: "job done"}}}
	o := &LogsObserver{queries: []logs.Query{q}, patterns: map[string]*regexp.Regexp{}, clock: func() time.Time { return *now }, states: map[string]*logState{}}
	for _, p := range q.Patterns {
		o.patterns[p.RE2] = regexp.MustCompile(p.RE2)
	}
	o.read = func(_ context.Context, _, _ string, _ *corev1.PodLogOptions) (io.ReadCloser, error) {
		var b strings.Builder
		for _, line := range lines(*now) {
			fmt.Fprintf(&b, "%s %s\n", now.Add(-time.Millisecond).UTC().Format(time.RFC3339Nano), line)
		}
		return io.NopCloser(strings.NewReader(b.String())), nil
	}
	controller := true
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-5d8f-abcde", Namespace: "app", UID: "u1", Labels: map[string]string{"pod-template-hash": "5d8f"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-5d8f", Controller: &controller}}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "main",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-time.Hour))}}}}},
	}
	return o, sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}
}

func TestLogWindowsNameTheirWorkloadAndRedactExcerpts(t *testing.T) {
	now := time.Unix(10_000, 0)
	o, base := signalObserver(&now, func(time.Time) []string {
		return []string{"request denied: password=synthetic-secret-value"}
	})
	r := o.collect(context.Background(), base)["q"]
	if len(r.Windows) != 1 || r.Windows[0].Workload != "Deployment/web" {
		t.Fatalf("windows = %+v, want one window of Deployment/web", r.Windows)
	}
	m := r.Windows[0].Cycles[0].Matches["denied"]
	if m.Count != 1 || len(m.Excerpts) != 1 {
		t.Fatalf("match = %+v", m)
	}
	if strings.Contains(m.Excerpts[0], "synthetic-secret-value") {
		t.Fatalf("excerpt keeps a secret-looking value: %q", m.Excerpts[0])
	}
}

func TestLogWindowsCoverRetainSpanWhenSnapshotsAreFrequent(t *testing.T) {
	now := time.Unix(10_000, 0)
	o, base := signalObserver(&now, func(time.Time) []string { return []string{"job done"} })
	var r logs.Result
	for i := 0; i < 3*logs.WindowCycles; i++ {
		now = now.Add(time.Second)
		r = o.collect(context.Background(), base)["q"]
	}
	if got := len(r.Windows[0].Cycles); got != 3*logs.WindowCycles {
		t.Fatalf("cycles = %d, want every cycle younger than %s", got, logs.RetainSpan)
	}
	now = now.Add(logs.RetainSpan)
	r = o.collect(context.Background(), base)["q"]
	if got := len(r.Windows[0].Cycles); got != logs.WindowCycles {
		t.Fatalf("cycles after %s = %d, want the last %d", logs.RetainSpan, got, logs.WindowCycles)
	}
}

// A healthy stretch that lasts the settle period becomes a history sample cut
// to the stretch; an unhealthy evaluation discards the stretch.
func TestLogsBaselineKeepsSettledHealthyStretches(t *testing.T) {
	now := time.Unix(10_000, 0)
	o, base := signalObserver(&now, func(time.Time) []string { return []string{"job done"} })
	baseline := NewLogsBaseline(time.Minute)
	provider := LogsSnapshotProvider{Base: staticSnapshot{base}, Observer: o, Baseline: baseline}
	step := func(healthy bool) logs.Result {
		now = now.Add(10 * time.Second)
		snapshot, err := provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		baseline.observe(now, healthy, snapshot)
		r, _ := snapshot.(logs.Source).LogResult("q")
		return r
	}
	for i := 0; i < 3; i++ {
		step(false)
	}
	for i := 0; i < 5; i++ {
		if r := step(true); len(r.History) != 0 {
			t.Fatalf("history before a settled stretch: %+v", r.History)
		}
	}
	step(true)
	step(true) // the stretch has lasted the settle period: promoted
	r := step(true)
	if len(r.History) != 1 {
		t.Fatalf("history = %d samples, want 1", len(r.History))
	}
	sample := r.History[0]
	if len(sample.Windows) != 1 || sample.Windows[0].Workload != "Deployment/web" {
		t.Fatalf("sample windows = %+v", sample.Windows)
	}
	for _, c := range sample.Windows[0].Cycles {
		if c.Start.Before(sample.End.Add(-2*time.Minute)) || c.End.After(sample.End) {
			t.Fatalf("sample cycle %v-%v outside the stretch ending %v", c.Start, c.End, sample.End)
		}
		for _, m := range c.Matches {
			if len(m.Excerpts) != 0 {
				t.Fatal("history samples keep counts only")
			}
		}
	}
	// An unhealthy evaluation in the next stretch discards it.
	for i := 0; i < 3; i++ {
		step(true)
	}
	step(false)
	for i := 0; i < 3; i++ {
		step(true)
	}
	if r := step(true); len(r.History) != 1 {
		t.Fatalf("history = %d samples after an interrupted stretch, want 1", len(r.History))
	}
}

type staticSnapshot struct{ snapshot sdktest.Snapshot }

func (s staticSnapshot) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return inputs.New(s.snapshot), nil
}
