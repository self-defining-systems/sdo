package operatorcli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/client-go/rest"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

func TestHermeticAPIProxyPodIdentityFreshWritesAndOfflineReflection(t *testing.T) {
	var calls atomic.Int32
	var readConnection atomic.Value
	readConnection.Store("")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-pod-token" {
			t.Error("proxy did not use Pod identity")
		}
		if r.Method == "POST" {
			calls.Add(1)
			if r.RemoteAddr == readConnection.Load().(string) {
				t.Error("write reused a connection and can be retried")
			}
			c, _, _ := w.(http.Hijacker).Hijack()
			c.Close()
			return
		}
		readConnection.Store(r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"kind":"ConfigMap","apiVersion":"v1","metadata":{"namespace":"app","name":"settings","resourceVersion":"opaque"},"data":{"mode":"broken"}}`)
	}))
	defer up.Close()
	reads, writes, err := apiProxyTransports(&rest.Config{Host: up.URL, BearerToken: "synthetic-pod-token"})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "incident", apihistory.Limits{Queue: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	u, _ := url.Parse(up.URL)
	proxy := httptest.NewServer(kubeproxy.New(u, reads, writes, store))
	defer proxy.Close()
	req, _ := http.NewRequest("GET", proxy.URL+"/api/v1/namespaces/app/configmaps/settings", nil)
	req.Header.Set("Authorization", "Bearer synthetic-caller-token")
	res, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	req, _ = http.NewRequest("POST", proxy.URL+"/api/v1/namespaces/app/configmaps", nil)
	req.Header.Set("Idempotency-Key", "synthetic")
	res, err = proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 502 || calls.Load() != 1 {
		t.Fatal("failed write was retried or changed status")
	}
	proxy.Close()
	store.Close()
	var out strings.Builder
	err = RunEvidence([]string{"state", "ConfigMap/app/settings", "--at", "before-first-mutation"}, func(string) string { return dir }, &out)
	if err != nil || !strings.Contains(out.String(), `"last_observed"`) ||
		!strings.Contains(out.String(), `"mode":""`) || strings.Contains(out.String(), "broken") ||
		!strings.Contains(out.String(), `"redacted":true`) || !strings.Contains(out.String(), `"unknown":true`) {
		t.Fatalf("reflection view: %s %v", out.String(), err)
	}
}
