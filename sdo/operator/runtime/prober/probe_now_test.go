package prober

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/traffic"
)

// A health-probe workload's burst sends the named user test's scheduled
// request once, now, and records it in the steady window the detectors read.
func TestBurstOnAHealthProbeSendsTheScheduledRequestNowAndRecordsIt(t *testing.T) {
	read := traffic.Scenario{ID: "items-read", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "list", Endpoint: traffic.GET("/items", nil)}}}
	other := traffic.Scenario{ID: "items-home", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "home", Endpoint: traffic.GET("/", nil)}}}
	steady := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe",
		Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: other.ID}, {ID: read.ID}}}
	store := &fixtureStore{items: map[string]bool{}}
	p, err := New(Config{Namespace: "app", Catalog: traffic.Catalog{read, other}, Workloads: []traffic.Workload{steady},
		Client: store, BaseURL: func(traffic.Target) string { return "http://web:80" }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Burst(context.Background(), BurstRequest{Workload: "probe", Scenarios: []string{read.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Healthy || len(result.Verdicts) != 1 || result.Verdicts[0].Scenario != read.ID || len(result.Window.Scenarios) != 1 {
		t.Fatalf("result = %+v", result)
	}
	sent := result.Window.Scenarios[0].Samples
	if len(sent) != 1 || sent[0].Requests == 0 {
		t.Fatalf("sent samples = %+v", sent)
	}
	var recorded []traffic.Sample
	for _, observed := range p.Windows()["probe"].Scenarios {
		if observed.Scenario.ID == read.ID {
			recorded = observed.Samples
		} else if len(observed.Samples) > 0 {
			t.Fatalf("another test was sent: %+v", observed)
		}
	}
	if len(recorded) != 1 || recorded[0].Iteration != sent[0].Iteration {
		t.Fatalf("steady window = %+v, want the sample sent now", recorded)
	}
	if _, err := p.Burst(context.Background(), BurstRequest{Workload: "probe", Scenarios: []string{"nope"}}); err == nil {
		t.Fatal("an unknown test must be refused")
	}
	if _, err := p.Burst(context.Background(), BurstRequest{Workload: "probe"}); err == nil {
		t.Fatal("a health probe runs named tests only, never its whole workload")
	}
}
