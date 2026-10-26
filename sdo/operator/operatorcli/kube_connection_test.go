package operatorcli

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"sdo.dev/operator/kubeproxy"
)

// This exercises the actual per-incident proxy rather than a client mock.
func TestIncidentProxyConnectionBudget(t *testing.T) {
	var dials atomic.Int32
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"api"}}`)
	}))
	upstream.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			dials.Add(1)
		}
	}
	upstream.StartTLS()
	defer upstream.Close()
	reads, writes, err := apiProxyTransports(&rest.Config{Host: upstream.URL, TLSClientConfig: rest.TLSClientConfig{Insecure: true}})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(upstream.URL)
	proxy := httptest.NewServer(kubeproxy.New(u, reads, writes, nil))
	defer proxy.Close()
	started := time.Now()
	for i := 0; i < 12; i++ {
		method := "GET"
		if i%2 == 1 {
			method = "PATCH"
		}
		req, _ := http.NewRequest(method, proxy.URL+"/api/v1/namespaces/app/pods/api", nil)
		res, err := proxy.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal(res.Status)
		}
	}
	t.Logf("12 alternating reads/writes: dials=%d elapsed=%s mean=%s", dials.Load(), time.Since(started), time.Since(started)/12)
	if dials.Load() != 1 {
		t.Fatalf("incident opened %d TLS connections, want one", dials.Load())
	}
}
