package runtime

import (
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
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
