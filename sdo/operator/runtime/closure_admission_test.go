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

// A closure verified while its responder runs admits a new opener; once the
// responder returned the opener waits for the next incident.
func TestProvisionalClosureAdmitsNewOpenersOnlyWhileTheResponderRuns(t *testing.T) {
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
			if phase != "running" {
				// The responder returned: the route opens the next incident.
				if c.joinAlertSet(route, at) || c.IncidentOpen() || c.pendingClosure == nil || len(c.pendingClosure.Request.JoinedKeys) != 0 {
					t.Fatalf("a route after the responder returned joined: open=%v pending=%+v", c.IncidentOpen(), c.pendingClosure)
				}
				return
			}
			if !c.joinAlertSet(route, at) {
				t.Fatal("a new route firing before closure publication was not admitted")
			}
			if !slices.Contains(*c.heldKeys.Load(), key) || !c.closureStableSince.IsZero() {
				t.Fatalf("joining must hold the route and reset the proof: held=%v stable=%s", *c.heldKeys.Load(), c.closureStableSince)
			}
			if c.incidentVerificationWarning(request.IncidentID) != nil {
				t.Fatal("the old recovery proof still marked the incident verified")
			}
			if dispatcher.stopCount() != 0 {
				t.Fatal("joining interrupted the responder's report")
			}
			finish(at)
			c.finishEarlyReport(context.Background(), at.Add(time.Second))
			if !c.IncidentOpen() || !c.responderDone || c.pendingClosure != nil || !c.recoveryScope().holds(route) {
				t.Fatalf("expected the same unresolved incident with a completed responder: open=%v done=%v pending=%v held=%v", c.IncidentOpen(), c.responderDone, c.pendingClosure != nil, c.recoveryScope())
			}
			// A running session goes on and returns by itself.
			if dispatcher.stopCount() != 0 || c.currentIncidentRequest.IncidentID != request.IncidentID {
				t.Fatalf("a running responder was retired for the original incident: stops=%d", dispatcher.stopCount())
			}
		})
	}
}

// The closure effect runs after the responder returned, so a new opener
// outside the alert set opens the next incident: publication reads only the
// alert set's owners and freezes the verified closure on its own set, without
// sampling any other detector.
func TestClosurePublicationReadsOnlyTheAlertSet(t *testing.T) {
	t.Run("agent close", func(t *testing.T) { testClosurePublication(t, "persistent") })
	t.Run("playbook-only close", func(t *testing.T) { testClosurePublication(t, "stage-only") })
	t.Run("non-member observer fails", func(t *testing.T) { testClosurePublication(t, "unavailable") })
}

type countedClosureProbe struct{ *sequenceDetector }

func (countedClosureProbe) ScenarioSLOWorkload() string { return "known-broken" }

func testClosurePublication(t *testing.T, scenario string) {
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
	probeTest := controllerDetector("traffic-known-broken", time.Second, sdk.Finding{})
	probeTest.errors[0] = errors.New("probe unavailable")
	if scenario == "unavailable" {
		metrics.errors[0] = errors.New("Prometheus unavailable")
	}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	c := openersController(t, dispatcher, probe, metrics, countedClosureProbe{probeTest})
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
	if scenario == "stage-only" {
		result.ResolutionPath = "deterministic"
		result.ResponderSessionID = ""
		c.pendingClosure.Request.PriorStages = []*contracts.StageReport{{
			Stage:            "deterministic",
			Mitigations:      []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{Applied: true}}},
			ApplyCompletedAt: contractTimestamp(now.Add(-30 * time.Second)),
			RecoveryDeadline: contractTimestamp(now.Add(2 * time.Second)),
			RecoveryWait:     &contracts.RecoveryWait{ExitReason: contracts.RecoveryWait_EXIT_REASON_CLEARED},
		}}
		c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	}
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
		if c.pendingClosure == nil || !c.pendingClosure.Frozen || len(c.pendingClosure.Request.JoinedKeys) != 0 ||
			c.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
			t.Fatalf("expected a verified freeze with the original alert set: %+v", c.pendingClosure)
		}
	}
	execute()
	if metrics.calls != 0 || probeTest.calls != 0 {
		t.Fatalf("publication sampled detectors outside the alert set: metrics=%d probe=%d", metrics.calls, probeTest.calls)
	}
	requireFrozen()
	if c.IncidentOpen() {
		t.Fatal("publication reopened the incident")
	}
}
