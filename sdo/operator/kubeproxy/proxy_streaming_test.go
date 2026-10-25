package kubeproxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

// Abort a real multi-megabyte response after the byte count from the CI failure,
// then reuse both transports. Neither an incomplete capture nor a cancelled
// upstream body may prevent the next response from reaching the caller.
func TestProxyLargeResponseAfterInterruptedBody(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 3<<16)
	const prefix = 875290
	for _, fault := range []string{"client_cancel", "upstream_abort"} {
		t.Run(fault, func(t *testing.T) {
			var requests atomic.Int32
			upstreamDone := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					defer close(upstreamDone)
					if fault == "upstream_abort" {
						conn, rw, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						defer conn.Close()
						fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(payload))
						rw.Write(payload[:prefix])
						rw.Flush()
						return
					}
					w.Header().Set("Content-Type", "application/json")
					w.Write(payload[:prefix])
					http.NewResponseController(w).Flush()
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(payload)
			}))
			defer up.Close()
			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			store, err := apihistory.Open(t.TempDir(), "interrupted", apihistory.Limits{Queue: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			u, _ := url.Parse(up.URL)
			proxy := httptest.NewServer(New(u, transport, transport, store))
			defer proxy.Close()
			clientTransport := http.DefaultTransport.(*http.Transport).Clone()
			defer clientTransport.CloseIdleConnections()
			client := &http.Client{Transport: clientTransport, Timeout: 5 * time.Second}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/api/v1/namespaces/app/pods", nil)
			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			first := make([]byte, prefix)
			_, err = io.ReadFull(res.Body, first)
			if err != nil || !bytes.Equal(first, payload[:prefix]) {
				res.Body.Close()
				t.Fatalf("partial response changed: %v", err)
			}
			if fault == "client_cancel" {
				cancel()
			}
			_, err = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if err == nil {
				t.Fatal("interrupted response reported a complete body")
			}
			select {
			case <-upstreamDone:
			case <-time.After(5 * time.Second):
				t.Fatal("upstream did not finish after interruption")
			}
			res, err = client.Get(proxy.URL + "/api/v1/namespaces/app/pods")
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
				t.Fatalf("response after interruption: status=%d bytes=%d/%d error=%v", res.StatusCode, len(body), len(payload), err)
			}
		})
	}
}
