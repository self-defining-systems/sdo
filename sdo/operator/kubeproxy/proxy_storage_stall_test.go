//go:build linux

package kubeproxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

func TestProxyLargeResponseWhileHistoryWriteBlocked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	// A full FIFO deterministically blocks the real history worker's next
	// filesystem write. Keep its reader open without consuming anything until
	// after the client has received the entire response, including HTTP EOF.
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NONBLOCK, 0600)
	if err != nil {
		t.Fatal(err)
	}
	reader := os.NewFile(uintptr(fd), path)
	t.Cleanup(func() { reader.Close() })
	fill := make([]byte, 4096)
	for {
		_, err := syscall.Write(fd, fill)
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	store, err := apihistory.Open(dir, "blocked-storage", apihistory.Limits{Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	// At least one event is accepted into the initially empty queue and cannot
	// reach disk until the FIFO is drained. Later admissions may be dropped.
	store.Record(apihistory.Event{Phase: "start", Verb: "get", RequestID: "blocked-storage"}, nil)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 3<<16)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	}))
	t.Cleanup(up.Close)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	u, _ := url.Parse(up.URL)
	proxy := httptest.NewServer(New(u, transport, transport, store))
	t.Cleanup(func() {
		// Unblock storage before waiting for handlers, also on a failed assertion.
		drained := make(chan struct{})
		go func() {
			io.Copy(io.Discard, reader)
			close(drained)
		}()
		proxy.Close()
		store.Close()
		reader.Close()
		<-drained
	})
	client := proxy.Client()
	client.Timeout = 5 * time.Second
	res, err := client.Get(proxy.URL + "/api/v1/namespaces/app/pods")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || res.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("blocked history changed response: status=%d bytes=%d/%d error=%v", res.StatusCode, len(body), len(payload), err)
	}
}
