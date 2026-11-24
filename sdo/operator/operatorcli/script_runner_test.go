package operatorcli

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
)

func TestScriptProxyHonorsFenceAndForcesFieldManager(t *testing.T) {
	for _, state := range []string{"open", "verified", "closed"} {
		t.Run(state, func(t *testing.T) {
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if state != "open" {
				guard, err := gate.TryObserve()
				if err != nil || guard == nil {
					t.Fatalf("observe gate: %v", err)
				}
				if state == "verified" {
					err = guard.Fence(time.Unix(1, 0))
				} else {
					err = guard.Seal()
				}
				guard.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			upstream, _ := url.Parse("https://synthetic.invalid")
			calls := 0
			transport := secretCaptureTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Query().Get("fieldManager") != sdk.ResponderFieldManager {
					t.Error("script selected its own field manager")
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			})
			proxy := scriptProxy(upstream, transport, transport, nil, gate)
			response := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPatch, "/api/v1/namespaces/app/configmaps/target?fieldManager=other", strings.NewReader("{}"))
			proxy.ServeHTTP(response, req)
			if state == "open" && (calls != 1 || response.Code != http.StatusOK) ||
				state != "open" && (calls != 0 || response.Code == http.StatusOK) {
				t.Fatalf("%s forwarded %d writes with status %d", state, calls, response.Code)
			}
		})
	}
}

func TestScriptTimeoutDrainsPatchBeforeFence(t *testing.T) {
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	upstream, _ := url.Parse("https://synthetic.invalid")
	runCtx, stop := context.WithCancel(t.Context())
	defer stop()
	admitted, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	transport := secretCaptureTransport(func(r *http.Request) (*http.Response, error) {
		close(admitted)
		<-release
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	proxy := drainScriptRequests(runCtx, t.Context(), scriptProxy(upstream, transport, transport, nil, gate))
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/namespaces/app/configmaps/target", strings.NewReader("{}"))
	response := httptest.NewRecorder()
	go func() {
		proxy.ServeHTTP(response, req.WithContext(runCtx))
		close(done)
	}()
	<-admitted
	stop()
	if guard, err := gate.TryObserve(); guard != nil || err != nil {
		t.Fatalf("write lease missing: %v %v", guard, err)
	}
	close(release)
	<-done
	guard, err := gate.TryObserve()
	if err != nil || guard == nil {
		t.Fatalf("observe drained write: %v", err)
	}
	defer guard.Close()
	if err := guard.Fence(time.Unix(1, 0)); err != nil || response.Code != http.StatusOK {
		t.Fatalf("drained PATCH status %d, fence: %v", response.Code, err)
	}
}
