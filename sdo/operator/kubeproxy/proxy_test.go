package kubeproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

func proxyServer(t *testing.T, upstream http.Handler) (*httptest.Server, string, *apihistory.Store) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "incident", apihistory.Limits{Queue: 100})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(up.URL)
	server := httptest.NewServer(New(u, http.DefaultTransport, http.DefaultTransport, store))
	t.Cleanup(func() { server.Close(); store.Close() })
	return server, dir, store
}

func TestProxyPreservesErrorsWritesAndRedacts(t *testing.T) {
	var calls atomic.Int32
	p, dir, store := proxyServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != "PATCH" || string(body) != "patch" || r.URL.RawQuery != "fieldManager=test" {
			t.Errorf("request changed: %v %s", r, string(body))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "kept")
		w.WriteHeader(403)
		io.WriteString(w, `{"kind":"Secret","apiVersion":"v1","metadata":{"name":"db"},"data":{"token":"cHJpdmF0ZQ=="}}`)
	}))
	req, _ := http.NewRequest("PATCH", p.URL+"/api/v1/namespaces/app/secrets/db?fieldManager=test", strings.NewReader("patch"))
	req.Header.Set("Authorization", "Bearer private")
	res, err := p.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 403 || res.Header.Get("X-Upstream") != "kept" || !strings.Contains(string(b), "cHJpdmF0ZQ==") || calls.Load() != 1 {
		t.Fatalf("passthrough: %s %d", b, calls.Load())
	}
	p.Close()
	store.Close()
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Events) != 2 || h.Events[1].Status != 403 || strings.Contains(string(h.Body(h.Events[1])), "cHJpdmF0ZQ==") {
		t.Fatalf("history %+v", h)
	}
}

func TestProxyUpgradePassThrough(t *testing.T) {
	for _, protocol := range []string{"websocket", "SPDY/3.1"} {
		t.Run(protocol, func(t *testing.T) {
			p, _, _ := proxyServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, rw, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", protocol)
				rw.Flush()
				b := make([]byte, 4)
				if _, err := io.ReadFull(rw, b); err == nil {
					rw.Write(b)
					rw.Flush()
				}
			}))
			u, _ := url.Parse(p.URL)
			c, err := net.Dial("tcp", u.Host)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			fmt.Fprintf(c, "GET /api/v1/namespaces/app/pods/api/exec?command=private HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", u.Host, protocol)
			rw := bufio.NewReader(c)
			res, err := http.ReadResponse(rw, nil)
			if err != nil || res.StatusCode != 101 {
				t.Fatalf("upgrade: %v %v", res, err)
			}
			io.WriteString(c, "ping")
			b := make([]byte, 4)
			if _, err := io.ReadFull(rw, b); err != nil || string(b) != "ping" {
				t.Fatalf("duplex: %s %v", b, err)
			}
		})
	}
}

func TestProxyCapsNeverTruncateAndStreamsMetadataOnly(t *testing.T) {
	for _, query := range []string{"", "?follow=true", "?watch=true"} {
		t.Run(query, func(t *testing.T) {
			payload := strings.Repeat("x", 100<<10)
			p, dir, store := proxyServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, payload) }))
			res, err := p.Client().Get(p.URL + "/api/v1/namespaces/app/pods/api/log" + query)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if string(body) != payload {
				t.Fatal("stream truncated")
			}
			store.Close()
			h, err := apihistory.Read(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(h.Events) != 2 {
				t.Fatalf("events: %+v", h)
			}
			b := h.Body(h.Events[1])
			if h.Events[1].Omission == "" {
				t.Fatal("missing redaction marker")
			}
			if len(b) != 0 {
				t.Fatal("stream payload stored")
			}
		})
	}
}

func TestProxyUpstreamFailureDoesNotRetryWrite(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	p := httptest.NewServer(New(u, http.DefaultTransport, http.DefaultTransport, nil))
	defer p.Close()
	r, _ := http.NewRequestWithContext(context.Background(), "POST", p.URL+"/api/v1/namespaces/app/pods", strings.NewReader("{}"))
	res, err := p.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 502 || !strings.Contains(string(b), `"kind":"Status"`) || calls.Load() != 1 {
		t.Fatalf("failure: %s calls %d", b, calls.Load())
	}
}

func TestProxyRecordingFailureStillForwards(t *testing.T) {
	p, _, store := proxyServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "unchanged") }))
	store.Close()
	res, err := p.Client().Get(p.URL + "/api/v1/pods")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if string(b) != "unchanged" || res.StatusCode != 200 {
		t.Fatalf("recording changed response: %s", b)
	}
}

func TestProxyIdentifiesNamespaceSubresourcesAndWatchForms(t *testing.T) {
	for _, tc := range []struct{ path, resource, namespace, name, subresource, verb string }{
		{"/api/v1/namespaces/app/status", "namespaces", "", "app", "status", "get"},
		{"/api/v1/namespaces/app/pods?watch=TRUE", "pods", "app", "", "", "watch"},
		{"/api/v1/watch/namespaces/app/pods", "pods", "app", "", "", "watch"},
	} {
		r := httptest.NewRequest("GET", tc.path, nil)
		e := requestEvent(r)
		if e.Resource != tc.resource || e.Namespace != tc.namespace || e.Name != tc.name || e.Subresource != tc.subresource || e.Verb != tc.verb {
			t.Errorf("%s: %+v", tc.path, e)
		}
	}
}

func TestDryRunOnlyRecognizesSupportedOperations(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		{"PATCH", "/api/v1/namespaces/app/pods/p?dryRun=All", true},
		{"PATCH", "/api/v1/namespaces/app/pods/p/status?dryRun=All", true},
		{"PATCH", "/api/v1/namespaces/app/pods/p?dryRun=invalid", false},
		{"PATCH", "/api/v1/namespaces/app/pods/p?dryRun=All&dryRun=invalid", false},
		{"GET", "/api/v1/namespaces/app/pods/p?dryRun=All", false},
		{"POST", "/api/v1/namespaces/app/pods/p/exec?dryRun=All", false},
		{"POST", "/api/v1/namespaces/app/pods/p/attach?dryRun=All", false},
		{"POST", "/api/v1/namespaces/app/pods/p/portforward?dryRun=All", false},
		{"POST", "/api/v1/namespaces/app/serviceaccounts/sa/token?dryRun=All", false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			event := requestEvent(httptest.NewRequest(tc.method, tc.path, nil))
			if event.DryRun != tc.want {
				t.Fatalf("dryRun=%v want %v", event.DryRun, tc.want)
			}
		})
	}
}
