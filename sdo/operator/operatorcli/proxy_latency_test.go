//go:build !race

package operatorcli_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
	"sort"
	"strings"
	"testing"
	"time"
)

// A fixed set of windows separates a sustained regression from a transient
// runner stall. Within each window retain p95 (including genuine tail delays).
// The 10ms budget applies on fast runners; when the direct transfer itself is
// slow, allow three direct-transfer times for the extra hop and capture copies.
// This compares work on the current machine instead of assuming dedicated CPUs.
type forwardingWindow struct {
	directP95 time.Duration
	addedP95  time.Duration
}

func (w forwardingWindow) budgetRatio() float64 {
	budget := max(10*time.Millisecond, 3*w.directP95)
	return float64(w.addedP95) / float64(budget)
}

func forwardingBudgetRatio(windows [3]forwardingWindow) float64 {
	var ratios [3]float64
	for i, window := range windows {
		ratios[i] = window.budgetRatio()
	}
	sort.Float64s(ratios[:])
	return ratios[len(ratios)/2]
}

func TestForwardingWindows(t *testing.T) {
	quiet := forwardingWindow{time.Millisecond, 2 * time.Millisecond}
	stall := forwardingWindow{time.Millisecond, 200 * time.Millisecond}
	for _, tc := range []struct {
		name    string
		windows [3]forwardingWindow
		pass    bool
	}{
		{"one_runner_stall", [3]forwardingWindow{quiet, stall, quiet}, true},
		{"sustained_tail_regression", [3]forwardingWindow{stall, quiet, stall}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ratio := forwardingBudgetRatio(tc.windows)
			if (ratio <= 1) != tc.pass {
				t.Fatalf("median ratio=%g, want pass=%v", ratio, tc.pass)
			}
		})
	}
}

func TestForwardingWindowBudget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		direct, added time.Duration
		pass          bool
	}{
		{"quiet", time.Millisecond, 2 * time.Millisecond, true},
		{"absolute_boundary", time.Millisecond, 10 * time.Millisecond, true},
		{"over_absolute_budget", time.Millisecond, 11 * time.Millisecond, false},
		{"cpu_contention", 10 * time.Millisecond, 25 * time.Millisecond, true},
		{"relative_boundary", 10 * time.Millisecond, 30 * time.Millisecond, true},
		{"over_relative_budget", 10 * time.Millisecond, 31 * time.Millisecond, false},
		{"tenfold_proxy_regression", time.Millisecond, 20 * time.Millisecond, false},
		{"tenfold_proxy_regression_under_contention", 10 * time.Millisecond, 250 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ratio := (forwardingWindow{tc.direct, tc.added}).budgetRatio()
			if (ratio <= 1) != tc.pass {
				t.Fatalf("budget ratio=%g, want pass=%v", ratio, tc.pass)
			}
		})
	}
}

func TestLargeResponseWarmAddedP95(t *testing.T) {
	item := `{"metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"c","env":[{"name":"MODE","value":"` + strings.Repeat("x", 3072) + `"}]}]}}`
	// Bound parallelism without overriding a caller's single-CPU limit.
	oldProcs := runtime.GOMAXPROCS(min(2, runtime.GOMAXPROCS(0)))
	defer runtime.GOMAXPROCS(oldProcs)
	payload := []byte(`{"apiVersion":"v1","kind":"PodList","items":[` + strings.TrimSuffix(strings.Repeat(item+",", 950), ",") + `]}`)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	}))
	defer up.Close()
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	u, _ := url.Parse(up.URL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	proxy := httptest.NewServer(kubeproxy.New(u, transport, transport, store))
	defer proxy.Close()
	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	defer clientTransport.CloseIdleConnections()
	client := &http.Client{Transport: clientTransport, Timeout: 5 * time.Second}
	get := func(endpoint string) time.Duration {
		start := time.Now()
		res, err := client.Get(endpoint + "/api/v1/namespaces/app/pods")
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if err != nil || n != int64(len(payload)) {
			t.Fatalf("read %d %v", n, err)
		}
		return time.Since(start)
	}
	measure := func() forwardingWindow {
		added := make([]time.Duration, 100)
		directs := make([]time.Duration, 100)
		for i := range added {
			var direct, proxied time.Duration
			if i%2 == 0 {
				direct = get(up.URL)
				proxied = get(proxy.URL)
			} else {
				proxied = get(proxy.URL)
				direct = get(up.URL)
			}
			directs[i] = direct
			added[i] = proxied - direct
			time.Sleep(time.Millisecond)
		}
		sort.Slice(added, func(i, j int) bool { return added[i] < added[j] })
		sort.Slice(directs, func(i, j int) bool { return directs[i] < directs[j] })
		return forwardingWindow{directs[94], added[94]}
	}
	// Fixed warmup, then three fixed windows, regardless of their results.
	// No retry-until-pass: a sustained slowdown fails the median window, while
	// one scheduling/GC/TCP stall cannot decide the entire test.
	t.Logf("discarded startup window: %+v", measure())
	var windows [3]forwardingWindow
	for i := range windows {
		windows[i] = measure()
		t.Logf("window %d: %d bytes, 100 balanced warm pairs: direct p95=%s added p95=%s budget ratio=%.3f",
			i, len(payload), windows[i].directP95, windows[i].addedP95, windows[i].budgetRatio())
	}
	if ratio := forwardingBudgetRatio(windows); ratio > 1 {
		t.Fatalf("median window budget ratio %.3f exceeds 1 (10ms added or 3x direct p95)", ratio)
	}
}

func TestProjectionTailLatency(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(min(2, runtime.GOMAXPROCS(0)))
	defer runtime.GOMAXPROCS(oldProcs)
	// 3 MiB, below the supported 4 MiB cap. No actual credentials.
	item := `{"metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"c","env":[{"name":"MODE","value":"` + strings.Repeat("x", 3072) + `"}]}]}}`
	payload := []byte(`{"apiVersion":"v1","kind":"PodList","items":[` + strings.TrimSuffix(strings.Repeat(item+",", 950), ",") + `]}`)
	dir := t.TempDir()
	store, _ := apihistory.Open(dir, "synthetic", apihistory.Limits{Queue: 1})
	defer store.Close()
	for range 50 {
		store.Record(apihistory.Event{Phase: "end", Verb: "list"}, payload)
	}
	samples := make([]time.Duration, 200)
	for i := range samples {
		start := time.Now()
		store.Record(apihistory.Event{Phase: "end", Verb: "list"}, payload)
		samples[i] = time.Since(start)
		time.Sleep(time.Millisecond)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	t.Logf("200 paced admissions for %d-byte PodList: p95=%s", len(payload), samples[189])
	if samples[189] > 10*time.Millisecond {
		t.Fatal("large response projection blocked admission")
	}
}
