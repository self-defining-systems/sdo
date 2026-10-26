package runtime

import (
	"errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPreviousLogsOptionsRecordReplay(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if r.URL.Path != "/api/v1/namespaces/app/pods/api/log" || q.Get("previous") != "true" || q.Get("container") != "main" || q.Get("tailLines") != "5" || q.Get("timestamps") != "true" || q.Get("limitBytes") != "1024" || q.Get("sinceTime") != "2026-10-26T00:00:00Z" {
			t.Errorf("unexpected logs request: %s", r.URL)
		}
		_, _ = w.Write([]byte("prior instance failed"))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	view := NewCycleReadView(LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client})
	pod := sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api"}
	tail := int64(5)
	options := corev1.PodLogOptions{Container: "main", Previous: true, SinceTime: &metav1.Time{Time: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}, TailLines: &tail, Timestamps: true, LimitBytes: int64ptr(1024)}
	for i := 0; i < 2; i++ {
		got, err := program.ReadFrom(view).LogsWithOptions(pod, options)
		if err != nil || got != "prior instance failed" {
			t.Fatalf("logs=%q err=%v", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("cache made %d calls", calls)
	}
	replay := &sdktest.ReplayReadView{Calls: view.RecordedReads()}
	got, err := replay.LogsWithOptions(pod, options)
	if err != nil || got != "prior instance failed" {
		t.Fatalf("replay=%q err=%v", got, err)
	}
	if _, err := sdktest.RecordPrimitiveCalls(replay, view.RecordedReads()[:1]); err != nil {
		t.Fatal(err)
	}
}

func TestUsageSamplesAndUnavailableReplay(t *testing.T) {
	var available atomic.Bool
	available.Store(true)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requests.Add(1)
		if !available.Load() {
			http.Error(w, `{"kind":"Status","status":"Failure","code":404}`, 404)
			return
		}
		switch r.URL.Path {
		case "/apis/metrics.k8s.io/v1beta1/namespaces/app/pods/api":
			_, _ = w.Write([]byte(`{"metadata":{"name":"api","namespace":"app"},"timestamp":"2026-10-26T00:00:00Z","window":"30s","containers":[{"name":"main","usage":{"cpu":"125m","memory":"64Mi"}}]}`))
		case "/apis/metrics.k8s.io/v1beta1/nodes/worker":
			_, _ = w.Write([]byte(`{"metadata":{"name":"worker"},"timestamp":"2026-10-26T00:00:00Z","window":"30s","usage":{"cpu":"1","memory":"1Gi"}}`))
		default:
			t.Errorf("unexpected usage request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	live := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}
	view := NewCycleReadView(live)
	pod := sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api"}
	samples, err := view.PodUsage(pod)
	if err != nil || len(samples.Containers) != 1 {
		t.Fatalf("pod usage=%+v, %v", samples, err)
	}
	sample := samples.Containers[0]
	if samples.Name != pod.Name || samples.Namespace != pod.Namespace || sample.Name != "main" || sample.Usage.Cpu().String() != "125m" || sample.Usage.Memory().String() != "64Mi" || samples.Window.Duration != 30*time.Second || samples.Timestamp.IsZero() {
		t.Fatalf("incomplete usage: %+v", sample)
	}
	node := sdk.ObjectRef{Kind: "Node", Name: "worker"}
	n, err := view.NodeUsage(node)
	if err != nil || n.Name != node.Name || n.Namespace != "" || n.Usage.Cpu().String() != "1" {
		t.Fatalf("node=%+v %v", n, err)
	}
	replay := &sdktest.ReplayReadView{Calls: view.RecordedReads()}
	p, err := replay.PodUsage(pod)
	if err != nil || p.Containers[0].Usage.Cpu().Cmp(*sample.Usage.Cpu()) != 0 {
		t.Fatalf("usage replay=%+v %v", p, err)
	}
	before := requests.Load()
	again, err := view.PodUsage(pod)
	if err != nil || requests.Load() != before || again == samples {
		t.Fatalf("usage cache failed: %+v %v", again, err)
	}
	again.Containers[0].Usage[corev1.ResourceCPU] = resource.MustParse("9")
	unchanged, err := view.PodUsage(pod)
	if err != nil || unchanged.Containers[0].Usage.Cpu().String() != "125m" {
		t.Fatalf("cached quantities aliased caller: %+v %v", unchanged, err)
	}
	if _, err := sdktest.RecordPrimitiveCalls(&sdktest.ReplayReadView{Calls: view.RecordedReads()}, view.RecordedReads()); err != nil {
		t.Fatal(err)
	}
	available.Store(false)
	missing := NewCycleReadView(live)
	_, err = missing.PodUsage(pod)
	var unavailable *metrics.UsageUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("missing API returned %T %v", err, err)
	}
	_, err = (&sdktest.ReplayReadView{Calls: missing.RecordedReads()}).PodUsage(pod)
	if !errors.As(err, &unavailable) {
		t.Fatalf("replay lost unavailable type: %T %v", err, err)
	}
}

func TestUsageMalformedSamplesNeverBecomeZero(t *testing.T) {
	valid := `{"metadata":{"name":"api","namespace":"app"},"timestamp":"2026-10-26T00:00:00Z","window":"30s","containers":[{"name":"main","usage":{"cpu":"0","memory":"0"}}]}`
	var body atomic.Value
	body.Store(valid)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body.Load().(string)))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	live := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}
	pod := sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api"}
	for _, tc := range []struct{ name, old, replacement string }{
		{"wrong identity", `"name":"api"`, `"name":"other"`},
		{"missing timestamp", `"timestamp":"2026-10-26T00:00:00Z",`, ``},
		{"missing window", `"window":"30s",`, ``},
		{"no containers", `[{"name":"main","usage":{"cpu":"0","memory":"0"}}]`, `[]`},
		{"missing container", `"name":"main",`, ``},
		{"missing CPU", `"cpu":"0",`, ``},
		{"missing memory", `,"memory":"0"`, ``},
		{"negative CPU", `"cpu":"0"`, `"cpu":"-1"`},
		{"invalid quantity", `"cpu":"0"`, `"cpu":"invalid"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body.Store(strings.Replace(valid, tc.old, tc.replacement, 1))
			_, err := live.PodUsage(pod)
			var unavailable *metrics.UsageUnavailableError
			if !errors.As(err, &unavailable) {
				t.Fatalf("bad sample became usage: %T %v", err, err)
			}
		})
	}
	body.Store(valid)
	got, err := live.PodUsage(pod)
	if err != nil || len(got.Containers) != 1 || !got.Containers[0].Usage.Cpu().IsZero() || !got.Containers[0].Usage.Memory().IsZero() {
		t.Fatalf("explicit measured zero rejected: %+v %v", got, err)
	}
	for _, ref := range []sdk.ObjectRef{{Kind: "Pod", Namespace: "other", Name: "api"}, {Kind: "Pod", Namespace: "app", Name: "../api"}, {Kind: "Node", Namespace: "app", Name: "worker"}} {
		if _, err := live.PodUsage(ref); err == nil {
			t.Fatalf("invalid usage identity accepted: %+v", ref)
		}
	}
}

func TestTypedLogsEnforceByteCapAndDuration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("sinceSeconds") != "1" {
			t.Errorf("subsecond duration must round up: %s", r.URL)
		}
		_, _ = w.Write([]byte("12345"))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	live := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}
	pod := sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api"}
	if got, err := live.LogsWithOptions(pod, corev1.PodLogOptions{Container: "main", SinceSeconds: int64ptr(1), LimitBytes: int64ptr(4)}); err == nil || got != "" {
		t.Fatalf("unbounded logs returned: %q %v", got, err)
	}
	if got, err := live.Logs(pod, "main", time.Nanosecond); err != nil || got != "12345" {
		t.Fatalf("legacy duration wrapper: %q %v", got, err)
	}
	if _, err := live.LogsWithOptions(pod, corev1.PodLogOptions{}); err == nil {
		t.Fatal("new logs accepted implicit container")
	}
}
