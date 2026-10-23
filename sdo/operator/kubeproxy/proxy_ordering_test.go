package kubeproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// A known-length transport probes for surplus bytes after copying the body.
// Hold that probe until response flushing so the two directions overlap.
type flushGatedBody struct {
	io.ReadCloser
	remaining int64
	flushed   <-chan struct{}
	probed    chan<- struct{}
	once      sync.Once
}

func (b *flushGatedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		<-b.flushed
		defer b.once.Do(func() { close(b.probed) })
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}

type flushSignalingWriter struct {
	http.ResponseWriter
	flushed chan struct{}
	once    sync.Once
}

func (w *flushSignalingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *flushSignalingWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.once.Do(func() { close(w.flushed) })
	return err
}

func TestProxyResponseFlushDoesNotCloseRequestBody(t *testing.T) {
	flushed := make(chan struct{})
	probed := make(chan struct{})
	payload := strings.Repeat("x", 100<<10)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
			return
		}
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
			return
		}
		select {
		case <-probed:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, payload)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	handler := New(u, transport, transport, nil)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &flushGatedBody{ReadCloser: r.Body, remaining: r.ContentLength, flushed: flushed, probed: probed}
		handler.ServeHTTP(&flushSignalingWriter{ResponseWriter: w, flushed: flushed}, r)
	}))
	defer proxy.Close()
	client := proxy.Client()
	client.Timeout = 5 * time.Second
	req, err := http.NewRequest(http.MethodGet, proxy.URL+"/api/v1/namespaces/app/pods/p/log", strings.NewReader(strings.Repeat("q", 8192)))
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || string(body) != payload || res.StatusCode != http.StatusOK {
		t.Fatalf("pass-through changed: status=%d bytes=%d want=%d error=%v", res.StatusCode, len(body), len(payload), err)
	}
}
