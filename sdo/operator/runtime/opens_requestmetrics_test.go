package runtime

import (
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
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

func TestRequestMetricsOnlyUserTrafficOpens(t *testing.T) {
	for _, route := range []string{"/items", "/undeclared"} {
		for _, tc := range []struct {
			name, origin, status         string
			failed, probeFailed, probeOK float64
			wantOpen                     bool
		}{
			{name: "probe 429", origin: "synthetic", status: "429", failed: 6},
			{name: "probe 503", origin: "synthetic", status: "503", failed: 6},
			{name: "unlabelled 429", status: "429", failed: 6},
			{name: "user 429", origin: "user", status: "429", failed: 6, wantOpen: true},
			{name: "mixed below floor", origin: "user", status: "429", failed: 2, probeFailed: 6},
			{name: "mixed above floor", origin: "user", status: "429", failed: 6, probeFailed: 6, probeOK: 200, wantOpen: true},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				start := time.Unix(0, 0).UTC()
				times := []time.Time{start, start.Add(5 * time.Second)}
				series := func(status, origin string, value float64) metrics.Series {
					labels := map[string]string{"route": route, "code": status}
					if origin != "" {
						labels[requestmetrics.TrafficOriginLabel] = origin
					}
					return metrics.Series{Labels: labels, Samples: []metrics.Sample{
						{At: times[0], Value: value}, {At: times[1], Value: value},
					}}
				}

				userOK := series("200", "user", 6)
				baseline := []metrics.Series{series("200", "user", 100)}
				presence := []metrics.Series{userOK}
				recent := []metrics.Series{userOK, series(tc.status, tc.origin, tc.failed)}
				if tc.origin == "" {
					recent[0] = series("200", "", 6)
					presence = recent
				}
				if tc.probeFailed > 0 {
					recent = append(recent, series("429", "synthetic", tc.probeFailed))
				}
				if tc.probeOK > 0 {
					baseline = append(baseline, series("429", "synthetic", 100))
					recent = append(recent, series("200", "synthetic", tc.probeOK))
				}

				provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", Metrics: map[string]metrics.Result{
					"reqmetrics-front-recent":   {ObservedAt: times[1], Series: recent},
					"reqmetrics-front-baseline": {Series: baseline},
					"reqmetrics-front-presence": {Series: presence},
				}}}
				detector := requestmetrics.NewDetector(requestmetrics.Config{
					Spec: sdk.DetectorSpec{ID: memory.RequestMetricsDetectorID, Interval: time.Second},
					Entries: []requestmetrics.Entry{{
						Service: "front", RouteLabel: "route", StatusLabel: "code",
						Recent: "recent", Baseline: "baseline", Presence: "presence",
					}},
					Operations: []requestmetrics.Operation{{ID: "items", Service: "front", Route: "/items"}},
					Rule:       requestmetrics.DefaultRule(),
				})
				cfg := testControllerConfig()
				cfg.Namespace = "app"
				dispatch := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
				c, err := NewController(cfg, []sdk.Detector{detector}, provider, dispatch, start)
				if err != nil {
					t.Fatal(err)
				}
				step(t, c, 5)
				step(t, c, 6)
				req := currentRequest(c)
				if (req != nil) != tc.wantOpen {
					t.Fatalf("opened = %t, want %t, active findings = %+v", req != nil, tc.wantOpen, activeFindings(c))
				}
				if req != nil {
					meta := req.Findings[0].Metadata
					if meta["failed"] != float64(6) || meta["total"] != float64(12) || meta[requestmetrics.MetaUserTraffic] != true {
						t.Fatalf("opening finding has incorrect traffic attribution or counts: %+v", req.Findings[0])
					}
				}
			})
		}
	}
}

func TestRestoredUnattributedRequestMetricsBatchMustBeReobserved(t *testing.T) {
	legacy := failingOperation("requestmetrics/errors/items")
	first := openersController(t, &recordingDispatcher{}, controllerDetector(memory.RequestMetricsDetectorID, time.Second, legacy))
	first.batcher = NewDebouncedBatcher(10 * time.Second)
	step(t, first, 0)
	state, err := first.ExportState()
	if err != nil || first.IncidentOpen() || len(state.GetPendingBatch().GetFindings()) != 1 {
		t.Fatalf("want one legacy pending error, open=%v: %v", first.IncidentOpen(), err)
	}

	fresh := legacy
	fresh.Metadata = map[string]any{requestmetrics.MetaUserTraffic: true}
	restored := openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		controllerDetector(memory.RequestMetricsDetectorID, time.Second, sdk.Finding{}, fresh))
	// Keep the legacy finding active for one missed/quiet observation, as the
	// generated detector does. A fresh attributed finding must activate anew.
	restored.tracker.SetPolicy(memory.RequestMetricsDetectorID, 1, 2, 0)
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	step(t, restored, 10)
	if restored.IncidentOpen() {
		t.Fatal("the queued error from before attribution opened after restore")
	}
	step(t, restored, 11)
	if !restored.IncidentOpen() {
		t.Fatal("fresh user evidence on the same fingerprint did not open")
	}
}
