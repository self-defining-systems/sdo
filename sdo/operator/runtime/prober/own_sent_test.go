package prober

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// Every request the prober sends and the application answers is in the
// snapshot's windows exactly once (traffic.SentFrom), with its service,
// method, path and status: what request metrics subtract as SDO's own traffic.
func TestWindowsCarryEveryAnsweredRequestTheProberSentOnce(t *testing.T) {
	read := traffic.Scenario{ID: "items-read", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "list", Endpoint: traffic.GET("/items", nil)}}}
	steady := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe",
		Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: read.ID}}}
	verify := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "verify",
		Purpose: traffic.PurposeVerifyBurst, RatePerSecond: 20, Duration: traffic.Duration(200 * time.Millisecond),
		Scenarios: []traffic.WorkloadScenario{{ID: read.ID}}}
	store := &countingDoer{inner: &fixtureStore{items: map[string]bool{}}}
	p, err := New(Config{Namespace: "app", Catalog: traffic.Catalog{read}, Workloads: []traffic.Workload{steady, verify},
		Client: store, BaseURL: func(traffic.Target) string { return "http://web:80" }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Burst(context.Background(), BurstRequest{Workload: "probe", Scenarios: []string{read.ID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Burst(context.Background(), BurstRequest{Workload: "verify"}); err != nil {
		t.Fatal(err)
	}
	var sent []traffic.SentRequest
	for _, window := range p.Windows() {
		sent = append(sent, window.Sent...)
	}
	if len(sent) < 2 || len(sent) != store.count() {
		t.Fatalf("sent = %+v, want each of the %d requests the application answered (the probe's and the verify burst's)", sent, store.count())
	}
	for _, request := range sent {
		if request.Service != "web" || request.Method != "GET" || request.Path != "/items" || request.Status == 0 || request.At.IsZero() {
			t.Fatalf("sent request %+v", request)
		}
	}
}

type countingDoer struct {
	mu    sync.Mutex
	n     int
	inner traffic.Doer
}

func (d *countingDoer) Do(request *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.n++
	d.mu.Unlock()
	return d.inner.Do(request)
}

func (d *countingDoer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}
