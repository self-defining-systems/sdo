package runtime

import (
	"context"
	"k8s.io/client-go/kubernetes/fake"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// handlerTransport exercises client-go serialization and the handler without
// opening a socket or scheduling a server goroutine.
type handlerTransport struct{ handler http.Handler }

func newHandlerTransport(handler http.Handler) handlerTransport { return handlerTransport{handler} }
func (tr handlerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	recorder := httptest.NewRecorder()
	tr.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

func TestKubernetesCacheObservationUsesInjectedClock(t *testing.T) {
	at := time.Unix(123, 456).UTC()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), Clock: func() time.Time { return at }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.(interface{ ObservedAt() time.Time }).ObservedAt(); !got.Equal(at) {
		t.Fatalf("observed at %s, want %s", got, at)
	}
}
