package prober_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

// app is a stand-in application that can be broken and repaired.
type app struct{ broken atomic.Bool }

func (a *app) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if a.broken.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no healthy upstream"))
		return
	}
	_, _ = w.Write([]byte(`{"status": "fine"}`))
}

func catalog() traffic.Catalog {
	read := func(id string, path string) traffic.Scenario {
		return traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
			DependsOn: []string{"backend"},
			Steps:     []traffic.Step{{Name: id, Endpoint: traffic.GET(path, nil).Contains("fine")}}}
	}
	return traffic.Catalog{read("home", "/"), read("status", "/status")}
}

func documents() map[string][]byte {
	return map[string][]byte{
		"health": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "health", "purpose": "health-probe",
			"ratePerSecond": 20, "scenarios": [{"id": "home"}, {"id": "status"}]}`),
		"verify": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "verify", "purpose": "verify-burst",
			"ratePerSecond": 20, "duration": "600ms", "scenarios": [{"id": "home"}, {"id": "status"}]}`),
	}
}

func newProber(t *testing.T, application http.Handler) *prober.Prober {
	t.Helper()
	server := httptest.NewServer(application)
	t.Cleanup(server.Close)
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
	return p
}

func probeFor(t *testing.T, p *prober.Prober, duration time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	p.Run(ctx)
}

func TestServiceBaseURLUsesClusterDNS(t *testing.T) {
	got := prober.ServiceBaseURL(traffic.Target{Service: "frontend", Port: 5000}, "hotel-reservation", "cluster.local")
	if got != "http://frontend.hotel-reservation.svc.cluster.local:5000" {
		t.Fatalf("base URL: %s", got)
	}
}

func TestProberObservesHealthyThenBrokenTraffic(t *testing.T) {
	application := &app{}
	p := newProber(t, application)
	probeFor(t, p, 600*time.Millisecond)
	window := p.Windows()["health"]
	if len(window.Scenarios) != 2 || window.Workload.Name != "health" {
		t.Fatalf("window must cover the workload's scenarios: %+v", window)
	}
	for _, verdict := range traffic.Judge(window) {
		if !verdict.Evaluated || !verdict.Healthy {
			t.Fatalf("healthy app must be judged healthy: %+v", verdict)
		}
	}
	if window.Scenarios[0].Scenario.DependsOn[0] != "backend" {
		t.Fatalf("observations must carry scenario descriptors: %+v", window.Scenarios[0].Scenario)
	}

	application.broken.Store(true)
	probeFor(t, p, 600*time.Millisecond)
	for _, verdict := range traffic.Judge(p.Windows()["health"]) {
		if verdict.Healthy || verdict.StatusCounts["503"] == 0 {
			t.Fatalf("broken app must violate the SLO with its status: %+v", verdict)
		}
	}

	p.Reset()
	for _, observed := range p.Windows()["health"].Scenarios {
		if len(observed.Samples) != 0 || !observed.Qualified {
			t.Fatalf("reset must forget samples while retaining healthy qualification: %+v", observed)
		}
	}
}

func TestMaintenanceResetPreservesPreviouslyHealthyScenarioQualification(t *testing.T) {
	application := &app{}
	p := newProber(t, application)
	probeFor(t, p, 600*time.Millisecond)
	for _, observed := range p.Windows()["health"].Scenarios {
		if !observed.Qualified {
			t.Fatalf("healthy baseline must qualify %s", observed.Scenario.ID)
		}
	}

	application.broken.Store(true)
	// Maintenance clears pre-fault samples, not the fact that these exact
	// scenario definitions worked before the fault.
	p.Reset()
	probeFor(t, p, 600*time.Millisecond)
	window := p.Windows()["health"]
	verdicts := traffic.Judge(window)
	if len(verdicts) != 2 {
		t.Fatalf("previously healthy scenarios disappeared after reset: %+v", window)
	}
	for _, verdict := range verdicts {
		if !verdict.Evaluated || verdict.Healthy {
			t.Fatalf("current failures must remain detector findings: %+v", verdict)
		}
	}
	result, err := p.Burst(context.Background(), prober.BurstRequest{Workload: "verify"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Healthy {
		t.Fatalf("closure skipped baseline-passing scenarios after maintenance: %+v", result)
	}
	for _, verdict := range result.Verdicts {
		if !verdict.Qualified || verdict.Healthy {
			t.Fatalf("failed baseline target must block closure: %+v", verdict)
		}
	}
}

func TestVerifyBurstJudgesOnlyTheRequestedScenariosNow(t *testing.T) {
	application := &app{}
	p := newProber(t, application)
	result, err := p.Burst(context.Background(), prober.BurstRequest{Scenarios: []string{"status"}})
	if err != nil {
		t.Fatalf("burst: %v", err)
	}
	if !result.Healthy || result.Workload != "verify" || len(result.Verdicts) != 1 || result.Verdicts[0].Scenario != "status" {
		t.Fatalf("healthy burst over one scenario: %+v", result)
	}
	if result.Duration > 2*time.Second {
		t.Fatalf("burst must stay bounded, took %s", result.Duration)
	}

	application.broken.Store(true)
	result, err = p.Burst(context.Background(), prober.BurstRequest{Workload: "verify"})
	if err != nil {
		t.Fatalf("burst: %v", err)
	}
	if result.Healthy || len(result.Verdicts) != 2 || !strings.Contains(strings.Join(result.Verdicts[0].Failures, " "), "no healthy upstream") {
		t.Fatalf("broken burst must fail with evidence: %+v", result)
	}

	if _, err := p.Burst(context.Background(), prober.BurstRequest{Scenarios: []string{"checkout"}}); err == nil {
		t.Fatalf("a burst naming a scenario the workload does not run must be rejected")
	}
	if _, err := p.Burst(context.Background(), prober.BurstRequest{Workload: "health"}); err == nil {
		t.Fatalf("a health-probe workload is continuous and cannot be burst")
	}
}

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

func TestHandlerServesWindowsBurstsAndReset(t *testing.T) {
	p := newProber(t, &app{})
	probeFor(t, p, 300*time.Millisecond)
	api := httptest.NewServer(prober.Handler(p))
	t.Cleanup(api.Close)

	response, err := http.Get(api.URL + prober.PathWindows)
	if err != nil {
		t.Fatalf("get windows: %v", err)
	}
	var windows prober.WindowsResponse
	if err := json.NewDecoder(response.Body).Decode(&windows); err != nil {
		t.Fatalf("decode windows: %v", err)
	}
	response.Body.Close()
	if len(windows.Windows["health"].Scenarios) != 2 || windows.Windows["health"].Workload.Seed == 0 {
		t.Fatalf("windows must round-trip with the workload seed: %+v", windows)
	}

	response, err = http.Post(api.URL+prober.PathBursts, "application/json", bytes.NewBufferString(`{"scenarios": ["home"]}`))
	if err != nil {
		t.Fatalf("post burst: %v", err)
	}
	var burst prober.BurstResult
	if err := json.NewDecoder(response.Body).Decode(&burst); err != nil || !burst.Healthy {
		t.Fatalf("burst over HTTP: %+v %v", burst, err)
	}
	response.Body.Close()

	response, err = http.Post(api.URL+prober.PathReset, "application/json", nil)
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("reset: %v %v", response, err)
	}
	response.Body.Close()
}

func TestNewRejectsWorkloadsNamingUnknownScenarios(t *testing.T) {
	workloads, err := prober.ParseWorkloads(map[string][]byte{"health": []byte(`{"apiVersion": "sdo.dev/v1alpha1",
		"kind": "TrafficWorkload", "name": "health", "purpose": "health-probe", "scenarios": [{"id": "checkout"}]}`)})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := prober.New(prober.Config{Namespace: "shop", Catalog: catalog(), Workloads: workloads}); err == nil {
		t.Fatalf("an unknown scenario must be rejected before any traffic is sent")
	}
	if _, err := prober.ParseWorkloads(map[string][]byte{"other": documents()["health"]}); err == nil {
		t.Fatalf("a workload's name must match its file")
	}
}

// pathApp serves "fine" everywhere except on paths it was told to break.
type pathApp struct{ broken map[string]*atomic.Bool }

func (a *pathApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if flag, ok := a.broken[r.URL.Path]; ok && flag.Load() {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = w.Write([]byte(`{"status": "fine"}`))
}

func TestVerifyBurstDoesNotBlockOnScenariosTheProbeNeverSawPass(t *testing.T) {
	status := &atomic.Bool{}
	status.Store(true)
	home := &atomic.Bool{}
	application := &pathApp{broken: map[string]*atomic.Bool{"/status": status, "/": home}}
	p := newProber(t, application)
	// The steady probe has only ever seen /status fail: its generator is
	// wrong for this application, so it never qualified and cannot fire.
	probeFor(t, p, 600*time.Millisecond)
	p.Reset()
	probeFor(t, p, 600*time.Millisecond)

	result, err := p.Burst(context.Background(), prober.BurstRequest{Workload: "verify"})
	if err != nil {
		t.Fatalf("burst: %v", err)
	}
	verdicts := map[string]prober.ScenarioVerdict{}
	for _, verdict := range result.Verdicts {
		verdicts[verdict.Scenario] = verdict
	}
	if verdicts["status"].Qualified || verdicts["status"].Healthy {
		t.Fatalf("a never-qualified scenario must be reported unqualified and failing: %+v", verdicts["status"])
	}
	if !verdicts["home"].Qualified || !verdicts["home"].Healthy {
		t.Fatalf("a qualified healthy scenario: %+v", verdicts["home"])
	}
	if !result.Healthy {
		t.Fatalf("an unqualified scenario must not block the burst, as it cannot block closure: %+v", result)
	}

	// A qualified scenario that fails still blocks.
	home.Store(true)
	result, err = p.Burst(context.Background(), prober.BurstRequest{Workload: "verify"})
	if err != nil {
		t.Fatalf("burst: %v", err)
	}
	if result.Healthy {
		t.Fatalf("a failing qualified scenario must block the burst: %+v", result)
	}
}
