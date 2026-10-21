package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

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
	api := httptest.NewServer(prober.Handler(p))
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
					if r.Method != http.MethodGet || r.URL.Path != prober.PathWindows {
						http.NotFound(w, r)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if err := json.NewEncoder(w).Encode(prober.WindowsResponse{Windows: map[string]traffic.Window{"health": window}}); err != nil {
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
	frontend := &fakeFrontend{}
	observer := NewTrafficObserver(runProber(t, frontend), []string{"health"}, nil, 0)
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
	observer.WaitWarm(ctx, 5*time.Second)
	time.Sleep(300 * time.Millisecond)
	observer.Poll(ctx)
	now := time.Unix(0, 0)
	if err := controller.StepEvents(ctx, now, []sdk.WatchKind{traffic.Watch}); err != nil {
		t.Fatalf("healthy step: %v", err)
	}
	if controller.IncidentOpen() {
		t.Fatalf("healthy traffic must not open an incident")
	}
	frontend.broken.Store(true)
	time.Sleep(600 * time.Millisecond)
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

func proberPodConfig(t *testing.T, client *fake.Clientset) *ProberPod {
	t.Helper()
	mount := t.TempDir()
	binary := filepath.Join(mount, ".sdo-prober", "abc", "sdo-prober")
	if err := os.MkdirAll(filepath.Dir(binary), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("prober build 1"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &ProberPod{
		Client: client, Namespace: "hotel-reservation-sdo", AppNamespace: "hotel-reservation", Image: "sdo-controller:v0.1.0",
		RepositoryPVC: "repo", RepositoryMountPath: mount, RepositoryPVCSubPath: "tenant", Binary: binary,
		ReadyTimeout: 300 * time.Millisecond,
	}
}

func TestProberPodIsIsolated(t *testing.T) {
	policy, pod, err := proberPodConfig(t, fake.NewSimpleClientset()).Manifests()
	if err != nil {
		t.Fatalf("manifests: %v", err)
	}
	spec := pod.Spec
	container := spec.Containers[0]
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Fatalf("the prober must not receive Kubernetes credentials")
	}
	if container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() ||
		!*container.SecurityContext.ReadOnlyRootFilesystem || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("the prober must be resource limited and locked down: %+v", container)
	}
	mount := container.VolumeMounts[0]
	if !mount.ReadOnly || mount.SubPath != "tenant/.sdo-prober/abc" || !spec.Volumes[0].PersistentVolumeClaim.ReadOnly ||
		container.Command[0] != "/opt/sdo-prober/sdo-prober" {
		t.Fatalf("the prober must mount only its binary, read-only: %+v %+v", mount, container.Command)
	}
	if strings.Join(container.Args, " ") != "--namespace hotel-reservation --listen :8080" {
		t.Fatalf("prober args: %v", container.Args)
	}
	egress := policy.Spec.Egress
	if len(policy.Spec.PolicyTypes) != 2 || len(egress) != 2 ||
		egress[0].To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "hotel-reservation" ||
		egress[1].Ports[0].Port.IntValue() != 53 {
		t.Fatalf("egress must reach only the application namespace and DNS: %+v", policy.Spec)
	}
}

func TestProberPodEnsureReusesAMatchingProberAndReplacesAStaleOne(t *testing.T) {
	client := fake.NewSimpleClientset()
	pods := client.CoreV1().Pods("hotel-reservation-sdo")
	config := proberPodConfig(t, client)
	_, desired, err := config.Manifests()
	if err != nil {
		t.Fatal(err)
	}
	ready := desired.DeepCopy()
	ready.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.7",
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	if _, err := pods.Create(context.Background(), ready, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	address, err := config.Ensure(context.Background())
	if err != nil || address != "http://10.0.0.7:8080" {
		t.Fatalf("a ready prober with the current binary must be reused: %q %v", address, err)
	}
	if cached, _ := config.Address(context.Background(), false); cached != address {
		t.Fatalf("address must be cached, got %q", cached)
	}

	stale := proberPodConfig(t, client)
	if err := os.WriteFile(stale.Binary, []byte("prober build 2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.Ensure(context.Background()); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("a replaced prober is recreated and awaited, got %v", err)
	}
	replaced, err := pods.Get(context.Background(), ProberName, metav1.GetOptions{})
	if err != nil || replaced.Labels[proberFingerprintLabel] == ready.Labels[proberFingerprintLabel] {
		t.Fatalf("a changed binary must replace the prober pod: %+v %v", replaced.Labels, err)
	}
	if _, err := client.NetworkingV1().NetworkPolicies("hotel-reservation-sdo").Get(context.Background(), ProberName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the prober NetworkPolicy must exist: %v", err)
	}
}

// A responder can run for many minutes. A pod IP handed to it at dispatch goes
// stale when the prober pod is replaced, and every `sdo incident status` then
// times out, so the responder gets the prober's stable Service address.
func TestResponderGetsAStableProberAddressThatSurvivesPodReplacement(t *testing.T) {
	client := fake.NewSimpleClientset()
	config := proberPodConfig(t, client)
	_, desired, err := config.Manifests()
	if err != nil {
		t.Fatal(err)
	}
	ready := desired.DeepCopy()
	ready.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.7",
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	if _, err := client.CoreV1().Pods("hotel-reservation-sdo").Create(context.Background(), ready, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	environment := ProberEnvironment(config.ServiceAddress)(context.Background())

	if got := environment[ProberURLEnvironment]; got != "http://sdo-prober.hotel-reservation-sdo.svc:8080" {
		t.Fatalf("responder must get the prober Service address, got %q", got)
	}
	service, err := client.CoreV1().Services("hotel-reservation-sdo").Get(context.Background(), ProberName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the prober Service must exist: %v", err)
	}
	if service.Spec.Selector["app.kubernetes.io/name"] != ProberName || service.Spec.Ports[0].Port != 8080 {
		t.Fatalf("the Service must select the prober pod on its API port: %+v", service.Spec)
	}
}

func TestProberEnvironmentTellsTheResponderWhereToVerify(t *testing.T) {
	environment := ProberEnvironment(StaticProberURL("http://10.0.0.7:8080"))(context.Background())
	if environment[ProberURLEnvironment] != "http://10.0.0.7:8080" || len(environment) != 1 {
		t.Fatalf("responder must receive the prober URL: %v", environment)
	}
	if got := ProberEnvironment(nil)(context.Background()); len(got) != 0 {
		t.Fatalf("without synthetic traffic the responder gets no prober URL: %v", got)
	}
	unreachable := func(context.Context, bool) (string, error) { return "", context.DeadlineExceeded }
	if got := ProberEnvironment(unreachable)(context.Background()); len(got) != 0 {
		t.Fatalf("an unresolvable prober must not block dispatch: %v", got)
	}
}
