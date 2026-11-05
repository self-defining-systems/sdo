package prober

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// fixtureStore is a synthetic application: POST /items creates the marked
// item (409 "already exists" when present), GET /items lists stored items.
type fixtureStore struct {
	mu    sync.Mutex
	items map[string]bool
	posts int
}

func (s *fixtureStore) Do(request *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, body := http.StatusOK, ""
	if request.Method == http.MethodPost {
		s.posts++
		name := request.URL.Query().Get("name")
		if s.items[name] {
			status, body = http.StatusConflict, "item already exists"
		} else {
			s.items[name] = true
			status, body = http.StatusCreated, "created"
		}
	} else {
		for name := range s.items {
			body += name + "\n"
		}
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// A verify burst is a fresh engine, but it must not write a read-later
// fixture again once the steady probe created it: data lost after setup stays
// detected by the burst instead of being restored by it.
func TestVerifyBurstDoesNotRecreateDataTheSteadyProbeSetUp(t *testing.T) {
	scenario := traffic.Scenario{
		ID: "items-later", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Marker: "sdo-later",
		Setup: []traffic.Step{{Name: "ensure-fixture", Endpoint: traffic.POST("/items", traffic.Params{"name": traffic.Const("sdo-later-fixture")}).
			OrExisting(http.StatusConflict, "already exists")}},
		Steps: []traffic.Step{{Name: "read-fixture", Endpoint: traffic.GET("/items", nil).Contains("sdo-later-fixture")}},
	}
	steady := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe-read-later",
		Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: scenario.ID}}}
	verify := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "verify",
		Purpose: traffic.PurposeVerifyBurst, RatePerSecond: 20, Duration: traffic.Duration(200 * time.Millisecond),
		Scenarios: []traffic.WorkloadScenario{{ID: scenario.ID}}}
	store := &fixtureStore{items: map[string]bool{}}
	p, err := New(Config{Namespace: "app", Catalog: traffic.Catalog{scenario}, Workloads: []traffic.Workload{steady, verify},
		Client: store, BaseURL: func(traffic.Target) string { return "http://web:80" }})
	if err != nil {
		t.Fatal(err)
	}
	if _, sample := p.probes["probe-read-later"].engine.Iterate(context.Background(), 0); sample.Outcome != traffic.OutcomeOK {
		t.Fatalf("steady probe on a fresh application: %+v", sample)
	}
	store.mu.Lock()
	store.items = map[string]bool{}
	store.mu.Unlock()
	result, err := p.Burst(context.Background(), BurstRequest{Workload: "verify"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Healthy {
		t.Fatalf("verify burst after the fixture was lost reported healthy: %+v", result.Verdicts)
	}
	if store.posts != 1 {
		t.Fatalf("setup wrote the fixture %d times, want once", store.posts)
	}
	if _, sample := p.probes["probe-read-later"].engine.Iterate(context.Background(), 1); sample.Outcome == traffic.OutcomeOK {
		t.Fatalf("steady probe passed after the lost fixture: %+v", sample)
	}
}
