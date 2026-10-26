package runtime

import (
	"errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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
	options := program.LogOptions{Previous: true, SinceTime: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), TailLines: &tail, Timestamps: true, LimitBytes: 1024}
	for i := 0; i < 2; i++ {
		got, err := program.ReadFrom(view).LogsWithOptions(pod, "main", options)
		if err != nil || got != "prior instance failed" {
			t.Fatalf("logs=%q err=%v", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("cache made %d calls", calls)
	}
	replay := &sdktest.ReplayReadView{Calls: view.RecordedReads()}
	got, err := replay.LogsWithOptions(pod, "main", options)
	if err != nil || got != "prior instance failed" {
		t.Fatalf("replay=%q err=%v", got, err)
	}
	if _, err := sdktest.RecordPrimitiveCalls(replay, view.RecordedReads()[:1]); err != nil {
		t.Fatal(err)
	}
}

func TestUsageSamplesAndUnavailableReplay(t *testing.T) {
	available := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !available {
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
	if err != nil || len(samples) != 1 {
		t.Fatalf("pod usage=%+v, %v", samples, err)
	}
	sample := samples[0]
	if sample.Resource != pod || sample.Container != "main" || sample.CPU.String() != "125m" || sample.Memory.String() != "64Mi" || sample.Window != 30*time.Second || sample.Timestamp.IsZero() {
		t.Fatalf("incomplete usage: %+v", sample)
	}
	node := sdk.ObjectRef{Kind: "Node", Name: "worker"}
	n, err := view.NodeUsage(node)
	if err != nil || n.Resource != node || n.CPU.String() != "1" {
		t.Fatalf("node=%+v %v", n, err)
	}
	replay := &sdktest.ReplayReadView{Calls: view.RecordedReads()}
	p, err := replay.PodUsage(pod)
	if err != nil || p[0].CPU.Cmp(sample.CPU) != 0 {
		t.Fatalf("usage replay=%+v %v", p, err)
	}
	available = false
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
