//go:build integration

package kubeproxy_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

// abandonThroughGate sends one request through a gated proxy whose upstream
// never answers, closes the client connection before any response, and
// returns the gate's observation once the proxy has finished the request.
func abandonThroughGate(t *testing.T, requestLine, body string, headers ...string) *apihistory.MutationObservation {
	t.Helper()
	arrived, finished := make(chan struct{}, 1), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reading the body to its end lets the server notice the proxy
		// hanging up.
		_, _ = io.Copy(io.Discard, r.Body)
		arrived <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-finished:
		}
	}))
	t.Cleanup(upstream.Close)
	t.Cleanup(func() { close(finished) })
	address, _ := url.Parse(upstream.URL)
	gate, err := apihistory.PrepareMutationGate(filepath.Join(t.TempDir(), "gate"))
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(kubeproxy.NewRecording(address, http.DefaultTransport, http.DefaultTransport, kubeproxy.Config{Gate: gate}))
	t.Cleanup(proxy.Close)

	conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		headers = append(headers, fmt.Sprintf("Content-Length: %d", len(body)))
	}
	request := requestLine + "\r\nHost: x\r\n" + strings.Join(append(headers, ""), "\r\n") + "\r\n" + body
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the upstream")
	}
	// The client gives up before the upstream answers (a `kubectl port-forward`
	// killed while it is still connecting).
	conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := gate.TryObserve()
		if err != nil {
			t.Fatal(err)
		}
		if observation != nil {
			t.Cleanup(func() { observation.Close() })
			return observation
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the proxy still holds the request's mutation lease")
	return nil
}

// A request that cannot have changed anything leaves no uncertain mutation
// behind when its client gives up before the upstream answers: an uncertain
// record blocks the incident's closure for good.
func TestAbandonedRequestThatCannotWriteLeavesNoUncertainMutation(t *testing.T) {
	cases := map[string]struct {
		line, body string
		headers    []string
	}{
		"port-forward before the upgrade": {
			line:    "POST /api/v1/namespaces/app/pods/web-0/portforward?ports=8080 HTTP/1.1",
			headers: []string{"Connection: Upgrade", "Upgrade: SPDY/3.1"},
		},
		"access review": {
			line: "POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews HTTP/1.1", body: "{}",
			headers: []string{"Content-Type: application/json"},
		},
		"server-side dry run": {
			line: "PATCH /apis/apps/v1/namespaces/app/deployments/web?dryRun=All HTTP/1.1", body: "{}",
			headers: []string{"Content-Type: application/merge-patch+json"},
		},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			observation := abandonThroughGate(t, request.line, request.body, request.headers...)
			if err := observation.Fence(time.Unix(1000, 0)); err != nil {
				t.Fatalf("abandoned %s blocked closure: %v", name, err)
			}
		})
	}
}

// A real write the client abandoned may still be applied by the API server:
// it stays uncertain.
func TestAbandonedWriteStaysUncertain(t *testing.T) {
	for _, request := range []struct {
		name, line, body string
		headers          []string
	}{
		{
			name:    "ordinary patch",
			line:    "PATCH /apis/apps/v1/namespaces/app/deployments/web HTTP/1.1",
			body:    "{}",
			headers: []string{"Content-Type: application/merge-patch+json"},
		},
		{
			name:    "connection request ignores dry run query",
			line:    "POST /api/v1/namespaces/app/pods/web-0/exec?dryRun=All HTTP/1.1",
			headers: []string{"Connection: Upgrade", "Upgrade: SPDY/3.1"},
		},
		{
			name:    "delete options body overrides dry run query",
			line:    "DELETE /api/v1/namespaces/app/pods/web-0?dryRun=All HTTP/1.1",
			body:    "{}",
			headers: []string{"Content-Type: application/json"},
		},
	} {
		t.Run(request.name, func(t *testing.T) {
			observation := abandonThroughGate(t, request.line, request.body, request.headers...)
			if err := observation.Fence(time.Unix(1000, 0)); err == nil {
				t.Fatal("an abandoned write must block closure")
			}
		})
	}
}
