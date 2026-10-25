package kubeproxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sdo.dev/operator/apihistory"
	"testing"
	"time"
)

func TestProxyIncidentFencePreservesReadsAndRejectsWrites(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write([]byte(`{"normal":"unchanged"}`)) }))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	observe, err := gate.TryObserve()
	if err != nil {
		t.Fatal(err)
	}
	if err := observe.Fence(time.Unix(42, 0)); err != nil {
		t.Fatal(err)
	}
	observe.Close()
	handler := NewWithMutationGate(target, http.DefaultTransport, http.DefaultTransport, nil, gate)
	for _, method := range []string{"GET", "PATCH", "POST", "DELETE"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, "/api/v1/namespaces/app/services/api", nil))
		if method == "GET" {
			if w.Code != 200 || w.Body.String() != `{"normal":"unchanged"}` {
				t.Fatal(w.Body.String())
			}
		} else if w.Code != 409 {
			t.Fatalf("write admitted: %s %d", method, w.Code)
		}
	}
	if calls != 1 {
		t.Fatalf("forwarded %d calls", calls)
	}
}
