package runtime

import (
	"context"
	"runtime"
	"slices"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func userTestFinding(scenario string) sdk.Finding {
	return sdk.Finding{
		RuleID: "scenario-slo." + scenario, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: scenario + " fails", Evidence: "synthetic 500",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"},
	}
}

func clueFinding(name string) sdk.Finding {
	return sdk.Finding{
		RuleID: "selector-mismatch", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: name + " selects no Pods", Evidence: "synthetic",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: name},
	}
}

func openersController(t *testing.T, dispatcher Dispatcher, detectors ...sdk.Detector) *Controller {
	t.Helper()
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	c, err := NewController(cfg, detectors, staticProvider{}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.gitRead = fakeArchitecture("")
	return c
}

func step(t *testing.T, c *Controller, second int64) {
	t.Helper()
	at := time.Unix(second, 0).UTC()
	c.now = func() time.Time { return at }
	if err := c.Step(context.Background(), at, nil); err != nil {
		t.Fatal(err)
	}
}

func currentRequest(c *Controller) *IncidentRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneIncidentRequest(c.currentIncidentRequest)
}

// Drift and other clues never open an incident. When a user test then fails,
// the incident opens on the test alone and carries the active clues as
// evidence.
func TestCluesOpenNothingButJoinTheIncidentAUserTestOpens(t *testing.T) {
	wiring := controllerDetector("cluster-wiring", time.Second, clueFinding("api"))
	probe := controllerDetector("traffic-probe", time.Second, sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, userTestFinding("browse-items"))
	c := openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, wiring, probe)
	c.queueUnexplainedFinding(time.Unix(0, 0), sdk.DriftEvent{
		Ref:            sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "api-config"},
		Classification: sdk.DriftUnexplained, Source: "differs from accepted source", ObservedAt: time.Unix(0, 0).UTC(),
	})
	for second := int64(0); second < 3; second++ {
		step(t, c, second)
		if c.IncidentOpen() {
			t.Fatalf("a clue opened an incident at %ds", second)
		}
	}
	step(t, c, 3)
	request := currentRequest(c)
	if request == nil {
		t.Fatal("the failing user test opened no incident")
	}
	var detectors []string
	for _, finding := range request.Findings {
		detectors = append(detectors, finding.DetectorID)
	}
	if !slices.Contains(detectors, "drift.unexplained") || !slices.Contains(detectors, "cluster-wiring") || !slices.Contains(detectors, "traffic-probe") {
		t.Fatalf("request findings = %v, want the user test with both clues", detectors)
	}
	if len(request.OpeningKeys) != 1 || request.OpeningKeys[0] != FindingStateKey("traffic-probe", FindingFingerprint(request.Findings[slices.Index(detectors, "traffic-probe")])) {
		t.Fatalf("opening keys = %v, want only the user test", request.OpeningKeys)
	}
}

// With an accepted map, only a scenario covering an operation that enters at
// a user entry is a user test; a probe of an internal operation is a clue.
func TestOnlyEntryUserTestsOpen(t *testing.T) {
	c := openersController(t, &recordingDispatcher{})
	c.gitRead = fakeArchitecture(verifyArch)
	entry := userTestFinding("browse-items")
	entry.DetectorID = "traffic-probe"
	internal := userTestFinding("warm-cache")
	internal.DetectorID = "traffic-probe"
	authored := userTestFinding("browse-items")
	authored.DetectorID = "checkout-objective"
	if !c.opens(entry) || c.opens(internal) || c.opens(authored) {
		t.Fatalf("opens: entry=%v internal=%v authored=%v, want true false false", c.opens(entry), c.opens(internal), c.opens(authored))
	}
	condition := clueFinding("api")
	condition.DetectorID = "workload-controllers"
	if !c.opens(condition) {
		t.Fatal("a held Kubernetes condition must open")
	}
}

// A clue that activates after the incident opened joins its findings (before
// launch, for the automatic stage) but never its opening alerts, and never
// holds its recovery.
func TestLateClueNeverEntersOpeningKeys(t *testing.T) {
	probe := controllerDetector("traffic-probe", time.Second, userTestFinding("browse-items"))
	wiring := controllerDetector("cluster-wiring", time.Second, sdk.Finding{}, sdk.Finding{}, clueFinding("api"))
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	c := openersController(t, dispatcher, probe, wiring)
	step(t, c, 0)
	opening := slices.Clone(currentRequest(c).OpeningKeys)
	step(t, c, 1)
	step(t, c, 2)
	request := currentRequest(c)
	if !slices.Equal(request.OpeningKeys, opening) || len(opening) != 1 {
		t.Fatalf("opening keys moved from %v to %v", opening, request.OpeningKeys)
	}
	joined := false
	for _, finding := range request.Findings {
		joined = joined || finding.DetectorID == "cluster-wiring"
	}
	if !joined {
		t.Fatal("a clue activating before launch must join the request")
	}
	if c.recoveryScope().holds(request.Findings[slices.IndexFunc(request.Findings, func(f sdk.Finding) bool { return f.DetectorID == "cluster-wiring" })]) {
		t.Fatal("a clue holds the incident's recovery")
	}
}

// A persistent clue does not stall the healthy baselines: healthy means no
// opener is active and no incident is open.
func TestPersistentClueIsStillQuiet(t *testing.T) {
	wiring := controllerDetector("cluster-wiring", time.Second, clueFinding("api"))
	c := openersController(t, &recordingDispatcher{}, wiring)
	step(t, c, 0)
	if !c.quiet() || !c.completeHealthyEvaluation() {
		t.Fatal("an active clue made the controller not healthy")
	}
	condition := controllerDetector("workload-health", time.Second, clueFinding("db"))
	c = openersController(t, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, condition)
	step(t, c, 0)
	if c.quiet() {
		t.Fatal("an active opener counted as healthy")
	}
}

// The operator reset closes an open incident as RESET (archived as failed: no
// outcome, no learning), stops its responder and drops tracked findings, so a
// fault that is still present opens a fresh incident.
func TestResetClosesTheOpenIncidentAndStartsClean(t *testing.T) {
	runOnOneProcessor(t)
	probe := controllerDetector("traffic-probe", time.Second, userTestFinding("browse-items"))
	dispatcher := &blockingDispatcher{}
	c := openersController(t, dispatcher, probe)
	var failures []ClosureFailure
	c.OnClosureFailed = func(failure ClosureFailure) { failures = append(failures, failure) }
	step(t, c, 0)
	executePendingEffect(t, c)
	for !dispatcher.hasStarted() {
		runtime.Gosched()
	}
	first := currentRequest(c).IncidentID
	reset, err := c.ResetIncidents(context.Background(), time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if reset != first || c.IncidentOpen() {
		t.Fatalf("reset %q, open %v; want %q closed", reset, c.IncidentOpen(), first)
	}
	if len(failures) != 1 || failures[0].Reason != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_RESET {
		t.Fatalf("closure failures = %+v, want one RESET", failures)
	}
	if len(c.tracker.Snapshot()) != 0 {
		t.Fatal("reset kept tracked findings")
	}
	c.Wait()
	step(t, c, 2)
	if next := currentRequest(c); next == nil || next.IncidentID == first {
		t.Fatalf("the still-failing test did not open a fresh incident: %+v", next)
	}
}
