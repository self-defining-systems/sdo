//go:build !race

package kubeproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sdo.dev/operator/apihistory"
	"sort"
	"testing"
	"time"
)

// Normal-build latency acceptance: bounded CPU parallelism, warm connections,
// balanced request order, and pacing outside each measured pair.
func TestProxyWarmAddedP95(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)
	payload := `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"app","name":"settings","resourceVersion":"17"},"data":{"mode":"live"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, payload) }))
	defer upstream.Close()
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "latency", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	u, _ := url.Parse(upstream.URL)
	proxy := httptest.NewServer(New(u, http.DefaultTransport, http.DefaultTransport, store))
	defer proxy.Close()
	get := func(endpoint string) time.Duration {
		start := time.Now()
		res, err := http.Get(endpoint + "/api/v1/namespaces/app/configmaps/settings")
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		return time.Since(start)
	}
	for range 50 {
		get(upstream.URL)
		get(proxy.URL)
	}
	direct := make([]time.Duration, 200)
	added := make([]time.Duration, 200)
	for i := range direct {
		var proxied time.Duration
		if i%2 == 0 {
			direct[i] = get(upstream.URL)
			proxied = get(proxy.URL)
		} else {
			proxied = get(proxy.URL)
			direct[i] = get(upstream.URL)
		}
		added[i] = proxied - direct[i]
		time.Sleep(time.Millisecond)
	}
	sort.Slice(direct, func(i, j int) bool { return direct[i] < direct[j] })
	sort.Slice(added, func(i, j int) bool { return added[i] < added[j] })
	store.Close()
	history, _ := apihistory.Read(dir)
	t.Logf("history envelopes=%d dropped=%d with paced loopback requests", len(history.Events), history.Manifest.Dropped)
	p95 := added[189]
	t.Logf("200 balanced warm GETs: direct p95=%s added p95=%s target=10ms", direct[189], p95)
	if p95 > 10*time.Millisecond {
		t.Fatalf("added warm p95 %s exceeds 10ms", p95)
	}
}
