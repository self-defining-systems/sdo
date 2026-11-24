package runtime

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
)

func TestProvisionalClosureAdmitsNewOpeners(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	for _, phase := range []string{"running", "returned", "report retired"} {
		t.Run(phase, func(t *testing.T) {
			c, dispatcher := earlyClosureController(t, now)
			c.config.RequestMetricsOpens = true
			condition := userTestFinding("original")
			condition.DetectorID, condition.RuleID = "workload-controllers", "statefulset-unready"
			request := c.currentIncidentRequest
			request.Findings = []sdk.Finding{condition}
			request.OpeningKeys = findingKeys(request.Findings)
			c.incidentDetectedAt, c.incidentDispatchedAt = now.Add(-30*time.Second), now.Add(-25*time.Second)
			c.pendingClosure.Request = *cloneIncidentRequest(request)
			c.pendingClosure.DetectedAt, c.pendingClosure.DispatchedAt = c.incidentDetectedAt, c.incidentDispatchedAt
			c.pendingClosure.FinalDetectorStates = []DetectorEvaluation{{DetectorID: condition.DetectorID, Status: DetectorEvaluationClear, EvaluatedAt: now}}
			c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			c.closureStableSince = now.Add(-15 * time.Second)

			finish := func(at time.Time) {
				c.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, result: completedResult(request.IncidentID)}, at)
			}
			if phase != "running" {
				finish(now.Add(time.Second))
				if phase == "report retired" {
					c.finishEarlyReport(context.Background(), now.Add(2*time.Second))
				}
			}

			route := failingOperation("requestmetrics/errors/undeclared/front//write")
			route.DetectorID = memory.RequestMetricsDetectorID
			key := FindingStateKey(route.DetectorID, FindingFingerprint(route))
			at := now.Add(9 * time.Second)
			if !c.joinAlertSet(route, at) {
				t.Fatal("a new route firing before closure publication was not admitted")
			}
			if !slices.Contains(*c.heldKeys.Load(), key) || !c.closureStableSince.IsZero() {
				t.Fatalf("joining must hold the route and reset the proof: held=%v stable=%s", *c.heldKeys.Load(), c.closureStableSince)
			}
			if phase == "running" {
				if c.incidentVerificationWarning(request.IncidentID) != nil {
					t.Fatal("the old recovery proof still marked the incident verified")
				}
				if dispatcher.stopCount() != 0 {
					t.Fatal("joining interrupted the responder's report")
				}
				finish(at)
			}
			c.finishEarlyReport(context.Background(), at.Add(time.Second))
			if !c.IncidentOpen() || !c.responderDone || c.pendingClosure != nil || !c.recoveryScope().holds(route) {
				t.Fatalf("expected the same unresolved incident with a completed responder: open=%v done=%v pending=%v held=%v", c.IncidentOpen(), c.responderDone, c.pendingClosure != nil, c.recoveryScope())
			}
			if dispatcher.stopCount() != 1 || c.currentIncidentRequest.IncidentID != request.IncidentID {
				t.Fatalf("responder was not retired exactly once for the original incident: stops=%d", dispatcher.stopCount())
			}
		})
	}
}

// The route has not run in the scheduled loop when the closure effect starts.
// It must still be read before a verified outcome can reach the broker.
func TestClosurePublicationChecksOpenersOutsideTheInitialProof(t *testing.T) {
	t.Run("persisted opener joins", func(t *testing.T) { testClosureAdmission(t, "persistent") })
	t.Run("single blip clears", func(t *testing.T) { testClosureAdmission(t, "blip") })
	t.Run("non-member observer fails", func(t *testing.T) { testClosureAdmission(t, "unavailable") })
	t.Run("known broken probes are excluded", func(t *testing.T) { testClosureAdmission(t, "known-broken") })
}

type countedClosureProbe struct{ *sequenceDetector }

func (countedClosureProbe) ScenarioSLOWorkload() string { return "known-broken" }

func testClosureAdmission(t *testing.T, scenario string) {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	condition := userTestFinding("original")
	condition.DetectorID, condition.RuleID = "workload-controllers", "statefulset-unready"
	route := failingOperation("requestmetrics/errors/undeclared/front//write")
	route.DetectorID = memory.RequestMetricsDetectorID
	probe := controllerDetector(condition.DetectorID, time.Second, sdk.Finding{})
	metrics := controllerDetector(route.DetectorID, time.Second, route)
	metrics.spec.Persistence.Firing = 2
	metrics.spec.Persistence.Clearing = 1
	probeTest := controllerDetector("traffic-known-broken", time.Second, userTestFinding("browse-items"))
	probeTest.errors[0] = errors.New("probe unavailable")
	if scenario == "unavailable" {
		metrics.errors[0] = errors.New("Prometheus unavailable")
	}
	if scenario == "known-broken" {
		metrics.samples = [][]sdk.Finding{nil}
	}
	c := openersController(t, &recordingDispatcher{}, probe, metrics, countedClosureProbe{probeTest})
	c.gitRead = func(ctx context.Context, repo string, args ...string) ([]byte, error) {
		if len(args) == 2 && args[0] == "show" && args[1] == "synthetic-accepted:"+layout.TrafficWorkloadsDir+"/known-broken.yaml" {
			return []byte("name: known-broken\npurpose: health-probe\nscenarios:\n- id: browse-items\n  knownBroken: {reason: test fixture, failsWith: ['404']}\n"), nil
		}
		return fakeArchitecture(verifyArch)(ctx, repo, args...)
	}
	c.now = func() time.Time { return now }
	c.config.RequestMetricsOpens = true
	c.broker = &riskBroker{}
	result := completedResult("synthetic")
	c.config.ClosureStabilityWindow = 5 * time.Second
	request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, IncidentID: result.IncidentID, Application: "demo", Namespace: "app",
		SourceCommit: "source", DeployedCommit: "deployed", RepositoryWorktree: "synthetic-worktree",
		RepositoryBaseCommit: "source", CancellationToken: "synthetic-token", RepairPolicy: "commit",
		OpeningKeys: findingKeys([]sdk.Finding{condition}), Findings: []sdk.Finding{condition}, ResponseDeadline: now.Add(time.Minute),
		DetectorHistory: []DetectorEvaluation{{DetectorID: condition.DetectorID, Status: DetectorEvaluationFiring, EvaluatedAt: now.Add(-time.Minute)}}}
	c.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: now.Add(-time.Minute),
		DispatchedAt: now.Add(-30 * time.Second), ResponderCompletedAt: now.Add(-time.Second), VerifiedAt: now.Add(-time.Second),
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: condition.DetectorID, Status: DetectorEvaluationClear, EvaluatedAt: now.Add(-time.Second)}}}
	c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING

	execute := func() {
		t.Helper()
		effect, ok := c.PendingClosureEffect()
		if !ok {
			t.Fatal("fixture has no pending closure publication")
		}
		if err := c.ExecuteClosureEffect(context.Background(), effect); err != nil {
			t.Fatal(err)
		}
	}
	requireFrozen := func() {
		t.Helper()
		if c.pendingClosure == nil || !c.pendingClosure.Frozen || len(c.pendingClosure.Request.JoinedKeys) != 0 {
			t.Fatalf("expected a verified freeze with the original alert set: %+v", c.pendingClosure)
		}
	}
	execute()
	if scenario == "known-broken" || scenario == "unavailable" {
		if probeTest.calls != 0 {
			t.Error("known-broken-only probe was sampled")
		}
		requireFrozen()
		return
	}
	if c.pendingClosure == nil || c.pendingClosure.Frozen || c.IncidentOpen() || len(c.pendingClosure.Request.JoinedKeys) != 0 {
		t.Fatalf("a single raw finding must wait for persistence: open=%v, closure=%+v", c.IncidentOpen(), c.pendingClosure)
	}
	if scenario == "blip" {
		metrics.samples = append(metrics.samples, nil)
	}
	c.now = func() time.Time { return now.Add(time.Second) }
	execute()
	if scenario == "blip" {
		requireFrozen()
		return
	}
	key := findingKey(route)
	if !c.IncidentOpen() || c.pendingClosure != nil || !slices.Contains(c.currentIncidentRequest.JoinedKeys, key) {
		t.Fatalf("closure should return to observation with the new route: open=%v pending=%v request=%+v", c.IncidentOpen(), c.pendingClosure != nil, c.currentIncidentRequest)
	}
	if got := c.broker.(*riskBroker).closures(); len(got) != 0 {
		t.Fatalf("broker received a contradicted closure: %+v", got)
	}
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("a new alert requested another responder")
	}
	if _, ok := c.PendingClosureEffect(); ok {
		t.Fatal("a new alert left the old proof publishable")
	}

	saved, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	restarted := openersController(t, &recordingDispatcher{}, probe, metrics)
	if err := restarted.RestoreState(saved); err != nil {
		t.Fatal(err)
	}
	if !restarted.IncidentOpen() || !restarted.responderDone || !slices.Contains(*restarted.heldKeys.Load(), key) {
		t.Fatalf("joined alert or responder result lost on restart: open=%v done=%v held=%v", restarted.IncidentOpen(), restarted.responderDone, *restarted.heldKeys.Load())
	}

	step(t, c, now.Unix()+2)
	if !c.IncidentOpen() || c.pendingClosure != nil {
		t.Fatal("the incident closed while the admitted route still failed")
	}
	metrics.samples = [][]sdk.Finding{nil}
	step(t, c, now.Unix()+3)
	step(t, c, now.Unix()+4)
	if !c.IncidentOpen() || c.pendingClosure != nil {
		t.Fatal("the incident reused its stability proof from before the route joined")
	}
	for second := int64(5); second <= 15 && c.pendingClosure == nil; second++ {
		step(t, c, now.Unix()+second)
	}
	if c.pendingClosure == nil || !slices.Contains(c.pendingClosure.Request.JoinedKeys, key) || c.pendingClosure.EscalatedAt.IsZero() {
		t.Fatal("a fresh continuous clear window did not produce an escalated closure covering the route")
	}
}
