//go:build integration

package prober_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
	"testing"
	"time"
)

// TestBlockedListenerIsClassifiedAsADialFailureWithinAFewSeconds models a
// NetworkPolicy that denies new connections to the target Service: it does
// not tear down connections already established, so the listener is closed
// (refusing new connections) while any request already in flight would keep
// being served. It confirms the prober's own client dials fresh, so the
// very next burst after the block sees only failed dials, is unhealthy, and
// is classified as a dial failure within a few seconds rather than hanging
// or being masked by a leftover connection.
func TestBlockedListenerIsClassifiedAsADialFailureWithinAFewSeconds(t *testing.T) {
	application := &app{}
	server := httptest.NewServer(application)
	workloads, err := prober.ParseWorkloads(documents())
	if err != nil {
		t.Fatalf("parse workloads: %v", err)
	}
	p, err := prober.New(prober.Config{
		Namespace: "shop", Catalog: catalog(), Workloads: workloads,
		BaseURL: func(traffic.Target) string { return server.URL },
	})
	if err != nil {
		t.Fatalf("new prober: %v", err)
	}

	warm, err := p.Burst(context.Background(), prober.BurstRequest{Workload: "verify"})
	if err != nil {
		t.Fatalf("warm burst: %v", err)
	}
	if !warm.Healthy {
		t.Fatalf("expected the warm burst to be healthy: %+v", warm)
	}

	if err := server.Listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	started := time.Now()
	blocked, err := p.Burst(context.Background(), prober.BurstRequest{Workload: "verify"})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("burst: %v", err)
	}
	if blocked.Healthy {
		t.Fatalf("a burst against a blocked listener must not be healthy: %+v", blocked)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("a blocked dial must be classified within a few seconds, took %s", elapsed)
	}
	var sawDialFailure bool
	for _, observed := range blocked.Window.Scenarios {
		for _, sample := range observed.Samples {
			if sample.DialFailed {
				sawDialFailure = true
			}
		}
	}
	if !sawDialFailure {
		t.Fatalf("a blocked listener must be classified as a dial failure, got %+v", blocked.Window)
	}
}

// TestKeepAliveConnectionsWouldMaskABlockedListener confirms the mechanism
// the fix relies on: a client that pools connections keeps using one it
// already opened even after the listener stops accepting new ones, so the
// very next request after a NetworkPolicy is applied can still succeed. The
// prober avoids this with NewHTTPClient (DisableKeepAlives), proven by
// TestBlockedListenerIsClassifiedAsADialFailureWithinAFewSeconds above.
func TestKeepAliveConnectionsWouldMaskABlockedListener(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: time.Second}

	warm, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("warm-up request: %v", err)
	}
	// The response body must be read to EOF, not merely closed, for the
	// Transport to return the connection to its idle pool for reuse.
	if _, err := io.ReadAll(warm.Body); err != nil {
		t.Fatalf("drain warm-up body: %v", err)
	}
	warm.Body.Close()

	if err := server.Listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	masked, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("a pooled keep-alive connection should keep working once the listener stops accepting new ones (masking the fault), got %v", err)
	}
	_, _ = io.ReadAll(masked.Body)
	masked.Body.Close()
}
