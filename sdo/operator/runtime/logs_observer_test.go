package runtime

import (
	"context"
	"fmt"
	"io"
	"sdo.dev/operator/sdk/generichealth"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
)

type logDetector struct {
	sdk.Detector
	queries []logs.Query
}

func (d logDetector) LogQueries() []logs.Query { return d.queries }
func TestLogsObserverBoundedFrozenAndIdle(t *testing.T) {
	client := fake.NewSimpleClientset()
	o, err := NewLogsObserver(client, nil, LogsObserverOptions{})
	if err != nil || o != nil || len(client.Actions()) != 0 {
		t.Fatal("unused logs perform work")
	}
	at := time.Unix(100, 0)
	d := logDetector{queries: []logs.Query{{ID: "q", Patterns: []logs.Pattern{{ID: "bad", RE2: "error"}}, Previous: true}}}
	o, err = NewLogsObserver(client, []sdk.Detector{d}, LogsObserverOptions{Clock: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	calls, previous := 0, 0
	o.read = func(_ context.Context, ns, pod string, opt *corev1.PodLogOptions) (io.ReadCloser, error) {
		calls++
		if opt.LimitBytes == nil || *opt.LimitBytes > logs.LimitBytes || !opt.Timestamps {
			t.Fatalf("unbounded options %+v", opt)
		}
		if opt.Previous {
			previous++
			return io.NopCloser(strings.NewReader("1970-01-01T00:01:39Z error died\n")), nil
		}
		if opt.SinceTime == nil {
			t.Fatal("missing sinceTime")
		}
		return io.NopCloser(strings.NewReader("1970-01-01T00:01:40Z error one\n1970-01-01T00:01:40Z ok\n")), nil
	}
	base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app", UID: "uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c", RestartCount: 1, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}}}
	provider := LogsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: o}
	ctx, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := logs.Require(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	c := r.Windows[0].Cycles[0]
	if c.Lines != 2 || c.Matches["bad"].Count != 1 || c.Previous.Lines != 1 {
		t.Fatalf("%+v", c)
	}
	logs.ResultFrom(ctx, "q")
	if calls != 2 {
		t.Fatalf("not frozen: %d", calls)
	}
	// Mutation of returned evidence cannot modify the frozen snapshot.
	r.Windows[0].Cycles[0].Matches["bad"] = logs.Match{Count: 99}
	again, _ := logs.ResultFrom(ctx, "q")
	if again.Windows[0].Cycles[0].Matches["bad"].Count != 1 {
		t.Fatal("snapshot aliases")
	}
	for i := 0; i < logs.WindowCycles+2; i++ {
		at = at.Add(time.Second)
		ctx, err = provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	r, err = logs.Require(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if previous != 1 || len(r.Windows[0].Cycles) != logs.WindowCycles || r.Windows[0].Cycles[logs.WindowCycles-1].Lines != 0 {
		t.Fatalf("previous=%d %+v", previous, r)
	}
	o.read = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(strings.Repeat("x", logs.LimitBytes+999))), nil
	}
	at = at.Add(time.Second)
	ctx, _ = provider.Snapshot(context.Background())
	r, _ = logs.ResultFrom(ctx, "q")
	if !r.Windows[0].Cycles[logs.WindowCycles-1].Truncated {
		t.Fatal("cap not flagged")
	}
}

func TestLogsSourceFailureAndBounds(t *testing.T) {
	d := logDetector{queries: []logs.Query{{ID: "q", Patterns: []logs.Pattern{{ID: "bad", RE2: "x"}}}}}
	o, err := NewLogsObserver(nil, []sdk.Detector{d}, LogsObserverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app", UID: "u"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}}}
	p := LogsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: o}
	ctx, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := logs.Require(ctx, "q"); err == nil {
		t.Fatal("source error became clear")
	}
	text := "1970-01-01T00:01:40Z " + strings.Repeat("x", 1000) + "\n"
	o.read = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(strings.Repeat(text, 10))), nil
	}
	o.clock = func() time.Time { return time.Unix(100, 0) }
	o.states = map[string]*logState{}
	ctx, _ = p.Snapshot(context.Background())
	r, err := logs.Require(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	m := r.Windows[0].Cycles[0].Matches["bad"]
	if m.Count != 10 || len(m.Excerpts) != logs.MaxExcerpts || len(m.Excerpts[0]) != logs.MaxExcerptBytes {
		t.Fatalf("%+v", m)
	}
	base.PodList[0].Spec.Containers = make([]corev1.Container, logs.MaxContainers+1)
	p.Base = staticProvider{snapshot: base}
	ctx, _ = p.Snapshot(context.Background())
	if _, err := logs.Require(ctx, "q"); err == nil {
		t.Fatal("unbounded containers accepted")
	}
}
func TestLogsWatchCreatesOnlyPodInformer(t *testing.T) {
	_, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset()}, []sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{ID: "logs", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: logs.Watch.APIVersion, Kind: logs.Watch.Kind, Namespace: "app"}}}}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLogsFakeClientUsesBoundedLogSubresource(t *testing.T) {
	client := fake.NewSimpleClientset()
	d := logDetector{queries: []logs.Query{{ID: "q"}}}
	o, err := NewLogsObserver(client, []sdk.Detector{d}, LogsObserverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("constructor performed I/O")
	}
	base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app", UID: "u"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}}}
	p := LogsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: o}
	ctx, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// client-go's fake returns an un-timestamped synthetic string. That must
	// become an unavailable source, never a successful empty window.
	if _, err := logs.Require(ctx, "q"); err == nil {
		t.Fatal("malformed source became healthy")
	}
	actions := client.Actions()
	if len(actions) != 1 || actions[0].GetSubresource() != "log" || actions[0].GetNamespace() != "app" {
		t.Fatalf("%+v", actions)
	}
	action, ok := actions[0].(interface{ GetValue() interface{} })
	if !ok {
		t.Fatal("log options not recorded")
	}
	opt := action.GetValue().(*corev1.PodLogOptions)
	if opt.LimitBytes == nil || *opt.LimitBytes != logs.LimitBytes || opt.SinceTime == nil || !opt.Timestamps {
		t.Fatalf("%+v", opt)
	}
}

// Both exits used to lose the decisive line at the log-window boundary.
func TestCrashTerminationLifecycle(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint("restart=", restart), func(t *testing.T) {
			at := time.Unix(100, 0)
			d := generichealth.NewCrashes(sdk.DetectorSpec{})
			o, err := NewLogsObserver(nil, []sdk.Detector{d}, LogsObserverOptions{Clock: func() time.Time { return at }})
			if err != nil {
				t.Fatal(err)
			}
			status := corev1.ContainerStatus{Name: "c", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
			base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app", UID: "uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}}}
			calls := 0
			o.read = func(_ context.Context, _, _ string, opt *corev1.PodLogOptions) (io.ReadCloser, error) {
				calls++
				if at.Unix() == 100 {
					return io.NopCloser(strings.NewReader("1970-01-01T00:01:40Z healthy\n")), nil
				}
				if !opt.Previous && opt.SinceTime != nil {
					return io.NopCloser(strings.NewReader("")), nil
				}
				if opt.SinceTime != nil || opt.TailLines == nil {
					t.Errorf("termination must read bounded tail: %+v", opt)
				}
				return io.NopCloser(strings.NewReader("1970-01-01T00:01:41Z panic: synthetic\n")), nil
			}
			check := func(want int) []sdk.Finding {
				base.PodList[0].Status.ContainerStatuses = []corev1.ContainerStatus{status}
				snapshot, err := (LogsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: o}).Snapshot(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				findings, err := d.Detect(context.Background(), snapshot)
				if err != nil || len(findings) != want {
					t.Fatalf("findings=%+v err=%v want=%d", findings, err, want)
				}
				return findings
			}
			check(0)
			at = at.Add(2 * time.Second)
			term := &corev1.ContainerStateTerminated{ExitCode: 2, FinishedAt: metav1.NewTime(at.Add(-time.Second))}
			status.State = corev1.ContainerState{Terminated: term}
			if restart {
				status.RestartCount = 1
				status.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
				status.LastTerminationState.Terminated = term
			}
			first := check(1)
			before := calls
			at = at.Add(time.Second)
			again := check(1)
			if !restart && calls != before {
				t.Fatal("terminal tail reread")
			}
			if first[0].Fingerprint != again[0].Fingerprint {
				t.Fatal("crash identity changed")
			}
			if !strings.Contains(first[0].Fingerprint, "uid/c/0") {
				t.Fatalf("missing crash identity: %s", first[0].Fingerprint)
			}
			if !restart {
				// Moving a judged terminal state into lastState must not re-read/re-key it.
				status.RestartCount = 1
				status.LastTerminationState.Terminated = term
				status.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
				at = at.Add(time.Second)
				moved := check(1)
				if moved[0].Fingerprint != first[0].Fingerprint {
					t.Fatal("same crash re-keyed after restart")
				}
				term2 := *term
				// Kubernetes timestamps can give two rapid exits the same finish time.
				term2.FinishedAt = term.FinishedAt
				status.State = corev1.ContainerState{Terminated: &term2}
				at = at.Add(time.Second)
				second := check(2)
				if second[0].Fingerprint == second[1].Fingerprint {
					t.Fatal("distinct crashes merged")
				}
			}

		})
	}
}

func TestLogsNeverStartedIsAbsent(t *testing.T) {
	d := generichealth.NewCrashes(sdk.DetectorSpec{})
	o, _ := NewLogsObserver(nil, []sdk.Detector{d}, LogsObserverOptions{})
	o.read = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		t.Fatal("read never-started container")
		return nil, nil
	}
	base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}}}}}
	snapshot, _ := (LogsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: o}).Snapshot(context.Background())
	if fs, err := d.Detect(context.Background(), snapshot); err != nil || len(fs) != 0 {
		t.Fatalf("%+v %v", fs, err)
	}
}

func TestTerminationTailRetryAndFrozen(t *testing.T) {
	d := generichealth.NewCrashes(sdk.DetectorSpec{})
	o, _ := NewLogsObserver(nil, []sdk.Detector{d}, LogsObserverOptions{})
	calls := 0
	o.read = func(_ context.Context, _, _ string, opt *corev1.PodLogOptions) (io.ReadCloser, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("synthetic kubelet outage")
		}
		if !opt.Previous || opt.SinceTime != nil {
			t.Fatalf("wrong termination source: %+v", opt)
		}
		return io.NopCloser(strings.NewReader("1970-01-01T00:00:01Z panic: old crash\n")), nil
	}
	base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app", UID: "uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c", RestartCount: 1, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, FinishedAt: metav1.NewTime(time.Unix(1, 0))}}}}}}}}
	provider := LogsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: o}
	snapshot, _ := provider.Snapshot(context.Background())
	if _, err := d.Detect(context.Background(), snapshot); err == nil {
		t.Fatal("source failure became healthy")
	}
	snapshot, _ = provider.Snapshot(context.Background())
	fs, err := d.Detect(context.Background(), snapshot)
	if err != nil || len(fs) != 1 {
		t.Fatalf("retry lost crash: %+v %v", fs, err)
	}
	result, _ := logs.ResultFrom(snapshot, "generic-crashes")
	result.Windows[0].Terminations[0].Tail.Matches["go-panic"] = logs.Match{}
	if fs, err := d.Detect(context.Background(), snapshot); err != nil || len(fs) != 1 {
		t.Fatal("termination evidence aliases snapshot")
	}
	snapshot, _ = provider.Snapshot(context.Background())
	if _, err := d.Detect(context.Background(), snapshot); err != nil || calls != 2 {
		t.Fatalf("successful tail reread: calls=%d err=%v", calls, err)
	}
}
