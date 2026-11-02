package operatorcli_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

type forwardingRecorder struct {
	*apihistory.Store
	completed chan apihistory.Event
	rawBytes  int
}

func (r *forwardingRecorder) Record(event apihistory.Event, body []byte) {
	r.Store.Record(event, body)
	if event.Phase == "end" {
		r.rawBytes = len(body)
		r.completed <- event
	}
}

// Keep the historical CI selector, but prove the forwarding property through
// ordering, not a p95 contaminated by TCP scheduling and other CI processes.
// BenchmarkLargeResponseWarmAddedP95 retains the paired timing measurement.
func TestLargeResponseWarmAddedP95(t *testing.T) {
	item := `{"metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"c","env":[{"name":"MODE","value":"` + strings.Repeat("x", 3072) + `"}]}]}}`
	payload := []byte(`{"apiVersion":"v1","kind":"PodList","items":[` + strings.TrimSuffix(strings.Repeat(item+",", 950), ",") + `]}`)
	cut := apihistory.MaxProjectionBytes + 1
	proceed := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload[:cut])
		w.(http.Flusher).Flush()
		<-proceed
		_, _ = w.Write(payload[cut:])
	}))
	defer up.Close()
	defer release()
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recorder := &forwardingRecorder{Store: store, completed: make(chan apihistory.Event, 1)}
	u, _ := url.Parse(up.URL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	proxy := httptest.NewServer(kubeproxy.New(u, transport, transport, recorder))
	defer proxy.Close()
	// Unblock the upstream before closing either server on an assertion failure.
	defer release()
	response, err := proxy.Client().Get(proxy.URL + "/api/v1/namespaces/app/pods")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	prefix := make([]byte, cut)
	if _, err := io.ReadFull(response.Body, prefix); err != nil || !bytes.Equal(prefix, payload[:cut]) {
		t.Fatalf("proxy did not stream the prefix while upstream was blocked: %v", err)
	}
	select {
	case <-recorder.completed:
		t.Fatal("history recorded a complete response before upstream finished")
	default:
	}
	release()
	remainder, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(remainder, payload[cut:]) {
		t.Fatalf("large response changed or truncated: %v", err)
	}
	end := <-recorder.completed
	if end.Omission != "redacted_projection_budget" || !end.Partial || recorder.rawBytes != 0 {
		t.Fatalf("over-budget body reached projection: event=%+v raw=%d", end, recorder.rawBytes)
	}
	proxy.Close()
	store.Close()
	history, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Events) != 2 || history.Manifest.Dropped == 0 || history.Manifest.Complete {
		t.Fatalf("large-response recording gap was lost: %+v", history)
	}
	for _, event := range history.Events {
		if len(history.Body(event)) != 0 {
			t.Fatal("over-budget response persisted a body")
		}
	}
}
