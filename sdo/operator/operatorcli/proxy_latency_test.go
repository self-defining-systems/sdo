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

// Normal-build latency acceptance: bounded CPU parallelism, warm connections,
// balanced request order, and pacing outside each measured pair.
func TestLargeResponseWarmAddedP95(t *testing.T) {
	item := `{"metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"c","env":[{"name":"MODE","value":"` + strings.Repeat("x", 3072) + `"}]}]}}`
	oldProcs := runtime.GOMAXPROCS(2)
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
	measure := func() time.Duration {
		added := make([]time.Duration, 200)
		for i := range added {
			var direct, proxied time.Duration
			if i%2 == 0 {
				direct = get(up.URL)
				proxied = get(proxy.URL)
			} else {
				proxied = get(proxy.URL)
				direct = get(up.URL)
			}
			added[i] = proxied - direct
			time.Sleep(time.Millisecond)
		}
		sort.Slice(added, func(i, j int) bool { return added[i] < added[j] })
		return added[189]
	}
	// Connection establishment alone does not warm a multi-MiB transfer. Discard
	// one complete window with the same pacing and ordering as the measured
	// window, so cold TCP/runtime setup cannot enter the warm acceptance gate.
	// This is fixed warmup, not a retry of a failed measurement.
	t.Logf("discarded startup window: added p95=%s", measure())
	p95 := measure()
	t.Logf("%d-byte supported response, 200 balanced warm pairs: added p95=%s", len(payload), p95)
	if p95 > 10*time.Millisecond {
		t.Error("supported response exceeds 10ms forwarding target")
	}
}

func TestProjectionTailLatency(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
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
