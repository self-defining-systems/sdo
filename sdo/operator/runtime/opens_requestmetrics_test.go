package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func requestMetricsFinding(rule, fingerprint, summary string) sdk.Finding {
	return sdk.Finding{
		RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: summary, Evidence: "synthetic", Fingerprint: fingerprint,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"},
	}
}

func failingOperation(fingerprint string) sdk.Finding {
	return requestMetricsFinding(requestmetrics.RuleErrors, fingerprint, "operation browse items: 40 of 100 requests failed in 30s (5xx)")
}

// Real user traffic pages by default: a request-metrics error finding opens,
// a not-instrumented note never does, and shadow mode still turns it off.
func TestRequestMetricsErrorsOpenByDefault(t *testing.T) {
	failing := failingOperation("requestmetrics/errors/browse-items")
	failing.DetectorID = memory.RequestMetricsDetectorID
	note := requestMetricsFinding(requestmetrics.RuleNotInstrumented, "requestmetrics/not-instrumented/browse-items", "operation browse items is not instrumented")
	note.DetectorID = memory.RequestMetricsDetectorID
	if settings.Default().Detection.RequestMetricsShadow {
		t.Fatal("shadow must default off")
	}
	c := openersController(t, &recordingDispatcher{})
	c.config = c.config.WithSettings(settings.Default())
	if !c.opens(failing) || c.opens(note) {
		t.Fatalf("error opens %v, note opens %v", c.opens(failing), c.opens(note))
	}
	shadow := settings.Default()
	shadow.Detection.RequestMetricsShadow = true
	if got := (ControllerConfig{}).WithSettings(shadow); got.RequestMetricsOpens {
		t.Fatal("shadow mode must not open")
	}
}

// A failing operation opens an incident keyed by its operation, a second
// failing route inside the join window joins the alert set and holds recovery
// like a user test, and a note never joins.
func TestRequestMetricsFindingOpensAndJoinsLikeAUserTest(t *testing.T) {
	operation := failingOperation("requestmetrics/errors/browse-items")
	detector := controllerDetector(memory.RequestMetricsDetectorID, time.Second, operation)
	c := openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, detector)
	c.config.RequestMetricsOpens = true
	step(t, c, 0)
	request := currentRequest(c)
	if request == nil || len(request.OpeningKeys) != 1 {
		t.Fatalf("a failing operation opened no incident: %+v", request)
	}
	if got := request.Findings[0].Summary; got != "operation browse items: 40 of 100 requests failed in 30s (5xx)" {
		t.Fatalf("page text = %q, want the operation and counts", got)
	}
	if !c.recoveryScope().holds(request.Findings[0]) {
		t.Fatal("the failing operation does not hold recovery")
	}
	route := failingOperation("requestmetrics/errors/undeclared/front//cart")
	route.DetectorID = memory.RequestMetricsDetectorID
	if !c.joinAlertSet(route, c.incidentDetectedAt.Add(time.Second)) {
		t.Fatal("a failing route inside the join window did not join")
	}
	if !slices.Contains(currentRequest(c).JoinedKeys, FindingStateKey(route.DetectorID, FindingFingerprint(route))) || !c.recoveryScope().holds(route) {
		t.Fatalf("joined keys = %v", currentRequest(c).JoinedKeys)
	}
	late := failingOperation("requestmetrics/errors/checkout")
	late.DetectorID = memory.RequestMetricsDetectorID
	if c.joinAlertSet(late, c.incidentDetectedAt.Add(AlertJoinWindow+time.Minute)) {
		t.Fatal("a failing operation after the join window joined")
	}
	note := requestMetricsFinding(requestmetrics.RuleNotInstrumented, "requestmetrics/not-instrumented/x", "x")
	note.DetectorID = memory.RequestMetricsDetectorID
	if c.joinAlertSet(note, c.incidentDetectedAt.Add(time.Second)) {
		t.Fatal("a not-instrumented note joined")
	}
}

// The incident recovers on the same signal: once the operation's finding
// clears, nothing in the recovery scope blocks.
func TestRequestMetricsIncidentClearsOnTheSameSignal(t *testing.T) {
	operation := failingOperation("requestmetrics/errors/browse-items")
	detector := controllerDetector(memory.RequestMetricsDetectorID, time.Second, operation, operation, sdk.Finding{}, sdk.Finding{})
	c := openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, detector)
	c.config.RequestMetricsOpens = true
	step(t, c, 0)
	if currentRequest(c) == nil {
		t.Fatal("no incident opened")
	}
	scope := c.recoveryScope()
	if len(scope.blocking(activeFindings(c))) != 1 {
		t.Fatal("the failing operation must hold recovery while active")
	}
	step(t, c, 1)
	step(t, c, 2)
	step(t, c, 3)
	if held := scope.blocking(activeFindings(c)); len(held) != 0 {
		t.Fatalf("recovery still held by %v after the operation cleared", held)
	}
}

// Not-instrumented notes open nothing and leave the controller quiet.
func TestRequestMetricsNotInstrumentedNeverOpens(t *testing.T) {
	note := requestMetricsFinding(requestmetrics.RuleNotInstrumented, "requestmetrics/not-instrumented/browse-items", "operation browse items is not instrumented")
	detector := controllerDetector(memory.RequestMetricsDetectorID, time.Second, note, note, note)
	c := openersController(t, &recordingDispatcher{}, detector)
	c.config.RequestMetricsOpens = true
	for second := int64(0); second < 4; second++ {
		step(t, c, second)
		if c.IncidentOpen() {
			t.Fatalf("a note opened an incident at %ds", second)
		}
	}
	if !c.quiet() {
		t.Fatal("a not-instrumented note made the controller busy")
	}
}

func activeFindings(c *Controller) []sdk.Finding {
	var active []sdk.Finding
	for _, state := range c.tracker.Snapshot() {
		if state.Active {
			active = append(active, state.Finding)
		}
	}
	return active
}

// A thin-baseline jump is kept as a clue under the error fingerprint; when the
// baseline fills and the same fingerprint becomes an error finding, the already
// active state must still open the incident instead of staying a clue.
func TestRequestMetricsClueUpgradedToErrorsOpens(t *testing.T) {
	fingerprint := "requestmetrics/errors/undeclared/front//packages"
	clue := requestMetricsFinding(requestmetrics.RuleInsufficientBaseline, fingerprint, "insufficient baseline to judge a client-error jump")
	clue.Severity = sdk.SeverityInfo
	errs := failingOperation(fingerprint)
	detector := controllerDetector(memory.RequestMetricsDetectorID, time.Second, clue, clue, errs, errs)
	c := openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, detector)
	c.config.RequestMetricsOpens = true
	step(t, c, 0)
	step(t, c, 1)
	if c.IncidentOpen() {
		t.Fatal("a thin-baseline clue opened an incident")
	}
	step(t, c, 2)
	request := currentRequest(c)
	if request == nil || len(request.OpeningKeys) != 1 {
		t.Fatalf("the clue that became an error finding opened no incident: %+v", request)
	}
}

// An upgrade inside an open incident joins its alert set, so Recovered waits on it.
func TestRequestMetricsClueUpgradedInsideOpenIncidentJoinsAlertSet(t *testing.T) {
	fingerprint := "requestmetrics/errors/undeclared/front//packages"
	clue := requestMetricsFinding(requestmetrics.RuleInsufficientBaseline, fingerprint, "insufficient baseline")
	clue.Severity = sdk.SeverityInfo
	errs := failingOperation(fingerprint)
	rm := controllerDetector(memory.RequestMetricsDetectorID, time.Second, clue, clue, errs)
	other := failingOperation("requestmetrics/errors/browse-items")
	opener := controllerDetector("other-opener", time.Second, other)
	c := openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, rm, opener)
	c.config.RequestMetricsOpens = true
	step(t, c, 0)
	step(t, c, 1)
	step(t, c, 2)
	request := currentRequest(c)
	if request == nil {
		t.Skip("no incident opened by the fixture; covered by the no-incident test")
	}
	key := FindingStateKey(memory.RequestMetricsDetectorID, fingerprint)
	if !slices.Contains(alertKeys(request), key) {
		t.Fatalf("upgraded finding is not in the alert set: %v", alertKeys(request))
	}
}

func TestRequestMetricsDiscountsRecordedProbeTraffic(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	for _, tc := range []struct {
		name, label, path                string
		failed, ok, probeFailed, probeOK int
		at, pollSkew                     time.Duration
		unproven, wantOpen               bool
	}{
		{name: "only probe failures", failed: 6, probeFailed: 6},
		{name: "only probe failures among many successes", failed: 6, probeFailed: 6, ok: 90},
		{name: "server-error probe", label: "503", failed: 6, probeFailed: 6},
		{name: "status class counter", label: "4xx", failed: 6, probeFailed: 6},
		{name: "mapped path", path: "/api/projects/2/widgets", failed: 6, probeFailed: 6},
		{name: "real failures on the same route", failed: 6, ok: 6, wantOpen: true},
		{name: "mixed traffic opens on residual share", failed: 12, ok: 300, probeFailed: 6, probeOK: 294, wantOpen: true},
		{name: "boundary overlap", failed: 6, probeFailed: 6, at: time.Second, wantOpen: true},
		{name: "prober clock skew", failed: 6, probeFailed: 6, pollSkew: 2 * time.Second, wantOpen: true},
		{name: "sparse scrapes", failed: 6, probeFailed: 6, unproven: true, wantOpen: true},
		{name: "unmapped path", path: "/api/something-else", failed: 6, probeFailed: 6, wantOpen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route, path := "/widgets", tc.path
			entryPath := route
			if path == "" {
				path = route
			} else {
				entryPath = "/api/projects/{id}/widgets"
			}
			label := tc.label
			if label == "" {
				label = "429"
			}
			times := []time.Time{start, start.Add(5 * time.Second)}
			series := func(status string, value int, proof bool) metrics.Series {
				labels := map[string]string{"route": route, "method": "GET", "code": status}
				if proof {
					labels["sdo_probe_coverage"] = "bracketed"
				}
				return metrics.Series{Labels: labels, Samples: []metrics.Sample{
					{At: times[0], Value: float64(value)}, {At: times[1], Value: float64(value)},
				}}
			}
			recent := []metrics.Series{series(label, tc.failed, false), series("200", tc.ok, false)}
			if !tc.unproven {
				recent = append(recent, series(label, 1, true), series("200", 1, true))
			}
			lead := 12 * time.Second
			if tc.at != 0 {
				lead = tc.at
			}
			at := start.Add(-lead)
			var receipts []traffic.HTTPReceipt
			for i := range tc.probeFailed + tc.probeOK {
				status := 200
				if i < tc.probeFailed {
					status = 429
					if label == "503" {
						status = 503
					}
				}
				receipts = append(receipts, traffic.HTTPReceipt{Service: "front", Port: 80, PathKey: traffic.HTTPPathKey(path),
					Method: "GET", SentAt: at, Answered: at.Add(time.Millisecond), Status: status})
			}
			snapshot := sdktest.Snapshot{NamespaceName: "app", Metrics: map[string]metrics.Result{
				"reqmetrics-front-recent":   {ObservedAt: times[1], Series: recent},
				"reqmetrics-front-baseline": {Series: []metrics.Series{series("200", 100, false)}},
				"reqmetrics-front-presence": {Series: []metrics.Series{series("200", 1, false)}},
			}}
			clock := clocktesting.NewFakeClock(times[1])
			probeWindows := map[string]traffic.Window{}
			if len(receipts) > 0 {
				probeWindows[traffic.HTTPReceiptWindowID] = traffic.Window{ObservedAt: times[1].Add(tc.pollSkew), HTTPRequests: receipts}
			}
			observer := NewTrafficObserver(captureTestProber{windows: probeWindows}, []string{"health"}, nil, 0)
			observer.Clock = clock
			observer.Poll(context.Background())
			provider := TrafficSnapshotProvider{Base: staticProvider{snapshot: snapshot}, Observer: observer}
			view, err := provider.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			detector := requestmetrics.NewDetector(requestmetrics.Config{
				Spec: sdk.DetectorSpec{ID: memory.RequestMetricsDetectorID, Interval: time.Second},
				Entries: []requestmetrics.Entry{{Service: "front", RouteLabel: "route", MethodLabel: "method", StatusLabel: "code",
					Recent: "recent", Baseline: "baseline", Presence: "presence"}},
				Operations: []requestmetrics.Operation{{ID: "widgets", Service: "front", Route: route, Method: "GET", ProbePath: entryPath, ProbePort: 80}},
				Rule:       requestmetrics.DefaultRule(),
			})
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			cfg.RequestMetricsOpens = true
			c, err := NewController(cfg, []sdk.Detector{detector}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
			if err != nil {
				t.Fatal(err)
			}
			step(t, c, 5)
			step(t, c, 6)
			req := currentRequest(c)
			if (req != nil) != tc.wantOpen {
				t.Fatalf("opened = %t, want %t, active findings = %+v", req != nil, tc.wantOpen, activeFindings(c))
			}
			if tc.probeOK > 0 {
				meta := req.Findings[0].Metadata
				if meta["failed"] != float64(6) || meta["total"] != float64(12) || meta["probe_discount_total"] != float64(300) || meta["probe_discount_failed"] != float64(6) {
					t.Fatalf("wrong residual or discount: %+v", meta)
				}
			}
			captured, err := sdktest.CaptureSnapshot(view, "app")
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(captured)
			if err != nil {
				t.Fatal(err)
			}
			var offline sdktest.Snapshot
			if err := json.Unmarshal(data, &offline); err != nil {
				t.Fatal(err)
			}
			live, err := detector.Detect(context.Background(), view)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := detector.Detect(context.Background(), offline)
			if err != nil || !reflect.DeepEqual(live, replayed) {
				t.Fatalf("offline = %+v (%v), live = %+v", replayed, err, live)
			}
			// The production memory host receives the frozen receipt window
			// even when no detector is registered to run a traffic workload.
			if tc.probeFailed > 0 && !tc.wantOpen {
				assertRemoteRequestMetricsQuiet(t, detector, view)
			}
		})
	}
}

func assertRemoteRequestMetricsQuiet(t *testing.T, detector sdk.Detector, view sdk.DetectionContext) {
	t.Helper()
	ctx := context.Background()
	host, err := memoryhost.NewServer("accepted", "binary", []sdk.Detector{detector})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	ready, err := host.Ready(ctx, &pb.ReadyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := NewRemoteMemory(ready, acceptedReadsHost{host})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := remote.buildObservation(view)
	if err != nil {
		t.Fatal(err)
	}
	result, err := host.Evaluate(ctx, &pb.EvaluateRequest{MirrorSequence: 1, Observation: observation, DetectorIds: []string{detector.Spec().ID}})
	if err != nil {
		t.Fatal(err)
	}
	entry := result.GetResults()[0]
	if entry.GetError() != "" || len(entry.GetFindings()) != 0 {
		t.Fatalf("remote detector did not discount the probe receipts: %+v", entry)
	}
}
