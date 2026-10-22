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
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
	defer up.Close()
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	u, _ := url.Parse(up.URL)
	proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
	defer proxy.Close()
	get := func(endpoint string) time.Duration {
		start := time.Now()
		res, err := http.Get(endpoint + "/api/v1/namespaces/app/pods")
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
	for range 50 {
		get(up.URL)
		get(proxy.URL)
	}
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
	t.Logf("%d-byte supported response, 200 balanced warm pairs: added p95=%s", len(payload), added[189])
	if added[189] > 10*time.Millisecond {
		t.Error("supported response exceeds 10ms forwarding target")
	}
}
