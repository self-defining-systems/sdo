package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/runtime/internal/testprober"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// fakeFrontend stands in for an application front end reached through a
// Service; broken models a Service whose selector matches no pod.
type fakeFrontend struct{ broken atomic.Bool }

func (f *fakeFrontend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if f.broken.Load() {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("rpc error: code = Unavailable desc = connection error"))
		return
	}
	_, _ = w.Write([]byte(`{"type": "FeatureCollection", "features": []}`))
}

func frontendCatalog() traffic.Catalog {
	read := func(id string, path string, dependsOn ...string) traffic.Scenario {
		return traffic.Scenario{
			ID: id, Target: traffic.Target{Service: "frontend", Port: 5000}, DependsOn: dependsOn, SideEffect: traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: id, Endpoint: traffic.GET(path, nil).Contains("FeatureCollection")}},
		}
	}
	return traffic.Catalog{read("search", "/hotels", "search", "geo"), read("recommend", "/recommendations", "recommendation")}
}

// runProber starts a real prober process stand-in against application and
// returns a client for its API.
func runProber(t *testing.T, application http.Handler) HTTPProberClient {
	t.Helper()
	app := httptest.NewServer(application)
	t.Cleanup(app.Close)
	workloads, err := prober.ParseWorkloads(map[string][]byte{
		"health": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "health",
			"purpose": "health-probe", "ratePerSecond": 20, "scenarios": [{"id": "search"}, {"id": "recommend"}]}`),
		"verify": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "verify",
			"purpose": "verify-burst", "ratePerSecond": 20, "duration": "500ms", "scenarios": [{"id": "search"}, {"id": "recommend"}]}`),
	})
	if err != nil {
		t.Fatalf("workloads: %v", err)
	}
	p, err := prober.New(prober.Config{
		Namespace: "hotel-reservation", Catalog: frontendCatalog(), Workloads: workloads,
		BaseURL: func(traffic.Target) string { return app.URL },
	})
	if err != nil {
		t.Fatalf("prober: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	api := httptest.NewServer(testprober.Handler(p))
	t.Cleanup(func() { cancel(); <-done; api.Close() })
	return HTTPProberClient{BaseURL: StaticProberURL(api.URL)}
}

func trafficDetector(id string, workload string) sdk.Detector {
	return traffic.NewDetector(sdk.DetectorSpec{
		ID: id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{traffic.Watch}, Interval: 10 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "lifecycle-bootstrap",
	}, workload)
}

func TestTrafficWorkloadNamesListsConsumedWorkloads(t *testing.T) {
	names := TrafficWorkloadNames([]sdk.Detector{trafficDetector("a", "health"), trafficDetector("b", "api"), trafficDetector("c", "health")})
	if strings.Join(names, ",") != "api,health" {
		t.Fatalf("consumed workloads: %v", names)
	}
}

func TestObserverNotifiesOnlyWhileFailuresAreInTheWindow(t *testing.T) {
	for _, scenario := range frontendCatalog() {
		for _, outcome := range []traffic.Outcome{traffic.OutcomeError, traffic.OutcomeTimeout} {
			t.Run(scenario.ID+"/"+string(outcome), func(t *testing.T) {
				// Windows count completed samples, not elapsed time. Serve exact
				// snapshots so neither probe arrival timing nor HTTP scheduling
				// determines whether the last failure has left the SLO window.
				window := traffic.Window{Workload: traffic.Workload{
					Scenarios: []traffic.WorkloadScenario{
						{ID: "search"},
						{ID: "recommend", SLO: &traffic.SLO{Window: 3}},
					},
				}}
				for _, descriptor := range frontendCatalog() {
					window.Scenarios = append(window.Scenarios, traffic.ScenarioObservations{
						Scenario: descriptor.Describe(), Qualified: true,
						Samples: []traffic.Sample{{Outcome: traffic.OutcomeOK}},
					})
				}
				var mu sync.Mutex
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Path != testprober.PathWindows {
						http.NotFound(w, r)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if err := json.NewEncoder(w).Encode(testprober.WindowsResponse{Windows: map[string]traffic.Window{"health": window}}); err != nil {
						t.Errorf("encode windows: %v", err)
					}
				}))
				t.Cleanup(api.Close)
				observer := NewTrafficObserver(HTTPProberClient{BaseURL: StaticProberURL(api.URL)}, []string{"health"}, nil, 0)
				poll := func(want bool) {
					t.Helper()
					if got := observer.Poll(context.Background()); got != want {
						t.Fatalf("wake controller = %t, want %t: %+v", got, want, observer.Windows())
					}
					if got := observer.Windows()["health"]; len(got.Scenarios) != 2 || got.Scenarios[0].Scenario.DependsOn[0] != "search" {
						t.Fatalf("window must carry scenario descriptors: %+v", got)
					}
				}
				appendSample := func(outcome traffic.Outcome) {
					mu.Lock()
					defer mu.Unlock()
					for i := range window.Scenarios {
						if window.Scenarios[i].Scenario.ID == scenario.ID {
							window.Scenarios[i].Samples = append(window.Scenarios[i].Samples, traffic.Sample{Outcome: outcome})
						}
					}
				}
				poll(false)
				appendSample(outcome)
				poll(true)
				size := window.Workload.ScenarioSLO(scenario.ID).Window
				for successes := 1; successes < size; successes++ {
					appendSample(traffic.OutcomeOK)
					poll(true)
				}
				appendSample(traffic.OutcomeOK)
				poll(false)
			})
		}
	}
}

func TestObserverReportsAnUnreachableProberAsALoadError(t *testing.T) {
	down := httptest.NewServer(http.NotFoundHandler())
	address := down.URL
	down.Close()
	observer := NewTrafficObserver(HTTPProberClient{BaseURL: StaticProberURL(address)}, []string{"health"}, nil, 0)
	observer.Poll(context.Background())
	window := observer.Windows()["health"]
	if !strings.Contains(window.LoadError, "prober unavailable") {
		t.Fatalf("an unreachable prober must surface as a load error, got %+v", window)
	}
	if NewTrafficObserver(nil, []string{"health"}, nil, 0) != nil || NewTrafficObserver(HTTPProberClient{}, nil, nil, 0) != nil {
		t.Fatalf("no prober or no consuming detector means no observer")
	}
}

func TestProberClientRefreshesTheAddressAfterATransportError(t *testing.T) {
	healthy := runProber(t, &fakeFrontend{})
	good, _ := healthy.BaseURL(context.Background(), false)
	down := httptest.NewServer(http.NotFoundHandler())
	stale := down.URL
	down.Close()
	refreshed := 0
	client := HTTPProberClient{BaseURL: func(_ context.Context, refresh bool) (string, error) {
		if refresh {
			refreshed++
			return good, nil
		}
		return stale, nil
	}}
	if _, err := client.Windows(context.Background()); err != nil || refreshed != 1 {
		t.Fatalf("client must re-locate a replaced prober once: refreshed=%d err=%v", refreshed, err)
	}
}

// The synthetic-traffic detector opens an incident when the application
// fails for the prober, and the evidence carries each scenario's error body.
func TestControllerOpensIncidentFromFailingSyntheticTraffic(t *testing.T) {
	// Exact windows isolate controller persistence from prober arrival timing.
	// The HTTP client and observer still run through the real API transport.
	var broken atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		window := traffic.Window{Workload: traffic.Workload{Name: "health", Purpose: traffic.PurposeHealthProbe}}
		for _, scenario := range frontendCatalog() {
			sample := traffic.Sample{Outcome: traffic.OutcomeOK, Status: 200}
			if broken.Load() {
				sample.Outcome = traffic.OutcomeError
				sample.Status = 500
				sample.Error = "rpc error: code = Unavailable desc = connection error"
			}
			window.Scenarios = append(window.Scenarios, traffic.ScenarioObservations{
				Scenario: scenario.Describe(), Qualified: true, Samples: []traffic.Sample{sample, sample, sample},
			})
		}
		if err := json.NewEncoder(w).Encode(testprober.WindowsResponse{Windows: map[string]traffic.Window{"health": window}}); err != nil {
			t.Errorf("encode windows: %v", err)
		}
	}))
	defer api.Close()
	observer := NewTrafficObserver(HTTPProberClient{BaseURL: StaticProberURL(api.URL)}, []string{"health"}, nil, 0)

	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(ControllerConfig{
		Application: "hotel", Namespace: "hotel-reservation", SourceCommit: "source", DeployedCommit: "deployed",
		RepositoryWorktree: "/tmp/hotel",
		ResponseTimeout:    time.Minute, FiringThreshold: 2, ClearThreshold: 2,
	}, []sdk.Detector{trafficDetector("traffic-health", "health")}, TrafficSnapshotProvider{
		Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "hotel-reservation"}}, Observer: observer,
	}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	ctx := context.Background()
	observer.Poll(ctx)
	now := time.Unix(0, 0)
	if err := controller.StepEvents(ctx, now, []sdk.WatchKind{traffic.Watch}); err != nil {
		t.Fatalf("healthy step: %v", err)
	}
	if controller.IncidentOpen() {
		t.Fatalf("healthy traffic must not open an incident")
	}
	broken.Store(true)
	observer.Poll(ctx)
	// traffic-health additionally requires the finding to persist for
	// traffic.DefaultHealthMinDuration (9s) before it fires, so
	// step past that duration, not just past the firing evaluation count.
	for step := 1; step <= 10; step++ {
		if err := controller.StepEvents(ctx, now.Add(time.Duration(step)*time.Second), []sdk.WatchKind{traffic.Watch}); err != nil {
			t.Fatalf("failing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	rules := make([]string, 0, len(request.Findings))
	for _, finding := range request.Findings {
		rules = append(rules, finding.RuleID)
		if !strings.Contains(finding.Evidence, "rpc error") {
			t.Fatalf("incident finding must carry the scenario's error body, got %q", finding.Evidence)
		}
	}
	if strings.Join(rules, ",") != "scenario-slo.recommend,scenario-slo.search" {
		t.Fatalf("incident must name every failing scenario, got %v", rules)
	}
}
