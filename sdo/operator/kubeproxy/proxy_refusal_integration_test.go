//go:build integration

package kubeproxy_test

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

type serverLog struct {
	mu   sync.Mutex
	text bytes.Buffer
}

func (l *serverLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.Write(p)
}

func (l *serverLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.String()
}

// A request answered without forwarding leaves its body unread. The server
// must still be able to drain it: no panic, and the connection stays usable.
func TestARequestAnsweredWithoutForwardingLeavesTheServerHealthy(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	address, _ := url.Parse(dead.URL)
	dead.Close()
	for name, tc := range map[string]struct {
		end    func(*apihistory.MutationObservation) error
		status int
	}{
		"closed":               {(*apihistory.MutationObservation).Seal, http.StatusConflict},
		"upstream unreachable": {nil, http.StatusBadGateway},
	} {
		t.Run(name, func(t *testing.T) {
			gate, err := apihistory.PrepareMutationGate(filepath.Join(t.TempDir(), "gate"))
			if err != nil {
				t.Fatal(err)
			}
			if tc.end != nil {
				observation, err := gate.TryObserve()
				if err != nil || observation == nil {
					t.Fatal(err)
				}
				if err := tc.end(observation); err != nil {
					t.Fatal(err)
				}
				observation.Close()
			}
			var logged serverLog
			proxy := httptest.NewUnstartedServer(kubeproxy.NewRecording(address, http.DefaultTransport, http.DefaultTransport, kubeproxy.Config{Gate: gate}))
			proxy.Config.ErrorLog = log.New(&logged, "", 0)
			proxy.Start()
			for _, size := range []int{1, 2000, 100000, 600000} {
				for range 5 {
					response, err := proxy.Client().Post(proxy.URL+"/api/v1/namespaces/app/configmaps", "application/json", strings.NewReader(strings.Repeat("x", size)))
					if err != nil {
						t.Errorf("%d bytes: %v", size, err)
						continue
					}
					_, _ = io.Copy(io.Discard, response.Body)
					response.Body.Close()
					if response.StatusCode != tc.status {
						t.Errorf("%d bytes: status %d, want %d", size, response.StatusCode, tc.status)
					}
				}
			}
			proxy.Close() // waits for the handlers, so every panic is logged
			if text := logged.String(); strings.Contains(text, "panic") || strings.Contains(text, "Body.Read") {
				t.Fatalf("the proxy panicked:\n%.600s", text)
			}
		})
	}
}
