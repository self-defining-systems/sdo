package runtime

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// switchedProbe is a health-probe traffic detector whose single scenario fails
// while its switch is on.
type switchedProbe struct {
	id       string
	scenario string
	failing  atomic.Bool
}

func (d *switchedProbe) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}

func (d *switchedProbe) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.failing.Load() {
		return []sdk.Finding{}, nil
	}
	return []sdk.Finding{d.finding()}, nil
}

func (d *switchedProbe) finding() sdk.Finding {
	finding := sdk.Finding{DetectorID: d.id, RuleID: traffic.RuleIDPrefix + d.scenario, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: d.scenario + " fails", Evidence: "synthetic",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "demo", Name: d.scenario}}
	finding.Fingerprint = FindingFingerprint(finding)
	return finding
}

// alertSetController is a controller with an open, dispatched, completed
// incident that opened on the "opening" user test alone.
func alertSetController(t *testing.T, now time.Time, probes ...*switchedProbe) (*Controller, *IncidentClosure) {
	t.Helper()
	detectors := make([]sdk.Detector, 0, len(probes))
	for _, probe := range probes {
		detectors = append(detectors, probe)
	}
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = time.Second
	c, err := NewController(cfg, detectors, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	opening := probes[0].finding()
	c.incidentOpen, c.responderDone = true, true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", Findings: []sdk.Finding{opening},
		OpeningKeys: findingKeys([]sdk.Finding{opening}), ResponseDeadline: now.Add(time.Hour)}
	c.incidentFindingKeys = findingKeys([]sdk.Finding{opening})
	c.incidentDetectedAt, c.incidentDispatchedAt, c.responderCompletedAt = now.Add(-3*time.Second), now.Add(-2*time.Second), now.Add(-time.Second)
	result := completedResult("synthetic")
	result.RepairActions = []RepairActionReceipt{{ActionID: "synthetic-scale", Kind: "kubectl-scale", Success: true}}
	c.currentIncidentResult = &result
	closed := new(IncidentClosure)
	c.OnIncidentClosed = func(closure IncidentClosure) { *closed = closure }
	return c, closed
}

// The opening alert was a side effect: it clears while a second user test,
// which started failing while the responder still ran, still hurts users.
// Recovered waits on the whole alert set, so the incident stays open until that
// test passes too.
func TestRecoveredWaitsForUserTestsThatJoinWhileTheResponderRuns(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	hurt := &switchedProbe{id: memory.TrafficDetectorPrefix + "hurt", scenario: "clone"}
	hurt.failing.Store(true)
	c, closed := alertSetController(t, now, opening, hurt)
	c.responderDone = false
	c.admitActivation(ctx, hurt.finding(), now, hurt.Spec())
	c.responderDone = true
	stepFor(t, c, now, 8)
	if c.currentIncidentRequest == nil || closed.Result != nil {
		t.Fatalf("the incident closed while a user test that joined it still failed: %+v", closed.Result)
	}
	if got, want := c.currentIncidentRequest.JoinedKeys, findingKeys([]sdk.Finding{hurt.finding()}); len(got) != 1 || got[0] != want[0] {
		t.Fatalf("joined keys = %v, want %v", got, want)
	}
	if len(c.currentIncidentRequest.OpeningKeys) != 1 {
		t.Fatalf("opening keys changed: %v; joiners are not what opened the incident", c.currentIncidentRequest.OpeningKeys)
	}
}

// A user test that starts failing past the join window is not the open
// incident's: the incident closes on its own alert set, verified, and the test
// opens the next incident. (Within the window it joins, even after the
// responder returned: an open incident is always worked.)
func TestAUserTestFailingPastTheJoinWindowOpensTheNextIncident(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, closed, hurt := lateUserTestController(t, now)
	c.incidentDetectedAt = now.Add(-AlertJoinWindow - time.Second)
	first := c.currentIncidentRequest.IncidentID
	for step := 0; step < 20 && (c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID == first); step++ {
		stepFor(t, c, now, 1)
		now = now.Add(time.Second)
	}
	if closed.Result == nil || closed.Request.IncidentID != first || len(closed.Request.JoinedKeys) != 0 ||
		len(closed.FinalDetectorStates) == 0 {
		t.Fatalf("the first incident did not close verified on its own alert set: %+v", closed)
	}
	next := c.currentIncidentRequest
	if next == nil || next.IncidentID == first || !slices.Equal(next.OpeningKeys, findingKeys([]sdk.Finding{hurt.finding()})) {
		t.Fatalf("the late user test did not open the next incident: %+v", next)
	}
}

// lateUserTestController is an incident that opened on a Kubernetes condition
// (cleared now) and whose responder returned; a user test fails from now on.
func lateUserTestController(t *testing.T, now time.Time) (*Controller, *IncidentClosure, *switchedProbe) {
	t.Helper()
	condition := &switchedProbe{id: memory.KubernetesConditionDetectorIDs()[0], scenario: "not-ready"}
	hurt := &switchedProbe{id: memory.TrafficDetectorPrefix + "hurt", scenario: "clone"}
	hurt.failing.Store(true)
	c, closed := alertSetController(t, now, condition, hurt)
	opening := sdk.Finding{DetectorID: condition.id, RuleID: "workload-container-not-ready", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "web not ready", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}}
	opening.Fingerprint = FindingFingerprint(opening)
	c.currentIncidentRequest.Findings, c.currentIncidentRequest.OpeningKeys = []sdk.Finding{opening}, findingKeys([]sdk.Finding{opening})
	c.incidentFindingKeys = findingKeys([]sdk.Finding{opening})
	return c, closed, hurt
}

// Every user test and failing user traffic uses the same join window.
// Kubernetes conditions never join; clues never hold recovery.
func TestOnlyUserTestsInTheJoinWindowJoinTheAlertSet(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	c, _ := alertSetController(t, now, opening)
	c.responderDone = false
	userTest := (&switchedProbe{id: memory.TrafficDetectorPrefix + "late", scenario: "clone"}).finding()
	clue := sdk.Finding{DetectorID: "resource-saturation", RuleID: "cpu", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "cpu", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "web"}}
	condition := sdk.Finding{DetectorID: memory.KubernetesConditionDetectorIDs()[0], RuleID: "stuck", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "stuck", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "web"}}
	ctx := context.Background()
	c.admitActivation(ctx, clue, now, sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.JoinedKeys) != 0 {
		t.Fatalf("a clue joined the alert set: %v", c.currentIncidentRequest.JoinedKeys)
	}
	c.admitActivation(ctx, condition, now, sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.JoinedKeys) != 0 {
		t.Fatalf("a Kubernetes condition joined the alert set: %v", c.currentIncidentRequest.JoinedKeys)
	}
	if batched := c.batcher.Snapshot().Findings; len(batched) != 1 || findingKey(batched[0]) != findingKey(condition) {
		t.Fatalf("the condition must wait for the next incident: batched %v", batched)
	}
	c.admitActivation(ctx, userTest, c.incidentDetectedAt.Add(AlertJoinWindow), sdk.DetectorSpec{})
	if got := c.currentIncidentRequest.JoinedKeys; len(got) != 1 || got[0] != findingKey(userTest) {
		t.Fatalf("expected the user test to join: %v", got)
	}
	late := (&switchedProbe{id: memory.TrafficDetectorPrefix + "independent", scenario: "write"}).finding()
	c.admitActivation(ctx, late, c.incidentDetectedAt.Add(AlertJoinWindow+time.Second), sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.JoinedKeys) != 1 {
		t.Fatalf("a user test after the window joined: %v", c.currentIncidentRequest.JoinedKeys)
	}
}

// sdo detect and the controller verification name the alert set Recovered waits on.
func TestAlertSetLineNamesJoinedUserTests(t *testing.T) {
	opening := (&switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}).finding()
	joined := (&switchedProbe{id: memory.TrafficDetectorPrefix + "hurt", scenario: "clone"}).finding()
	request := IncidentRequest{Findings: []sdk.Finding{opening, joined}, OpeningKeys: findingKeys([]sdk.Finding{opening})}
	if line := alertSetLine(&request); line != "" {
		t.Fatalf("no joiner, no line: %q", line)
	}
	request.JoinedKeys = findingKeys([]sdk.Finding{joined})
	line := alertSetLine(&request)
	if !strings.Contains(line, "1 opening + 1 joined") || !strings.Contains(line, "clone") {
		t.Fatalf("alert set line = %q", line)
	}
}

// The alert set survives the persisted projection.
func TestJoinedKeysRoundTripThroughTheContract(t *testing.T) {
	request := IncidentRequest{IncidentID: "synthetic", OpeningKeys: []string{"a"}, JoinedKeys: []string{"b"}}
	message, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	if got := incidentRequestState(message).JoinedKeys; len(got) != 1 || got[0] != "b" {
		t.Fatalf("joined keys = %v", got)
	}
	if got := alertKeys(&request); len(got) != 2 {
		t.Fatalf("alert keys = %v, want opening and joined", got)
	}
}

// handoffCounter counts the agent hand-offs the controller prepares.
type handoffCounter struct {
	recordingDispatcher
	prepared atomic.Int32
}

func (d *handoffCounter) PrepareDispatch(_ context.Context, request IncidentRequest) (IncidentRequest, error) {
	d.prepared.Add(1)
	return request, nil
}

// afterPlaybookJoinedController is an incident whose applied playbook wrote,
// whose opening alert has cleared and whose joined user test still fails.
func afterPlaybookJoinedController(t *testing.T, now time.Time, sessionID string) (*Controller, *handoffCounter) {
	t.Helper()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	hurt := &switchedProbe{id: memory.TrafficDetectorPrefix + "hurt", scenario: "clone"}
	hurt.failing.Store(true)
	c, _ := alertSetController(t, now, opening, hurt)
	dispatcher := &handoffCounter{}
	c.dispatcher = dispatcher
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	c.config.VerificationTimeout = time.Hour
	c.currentIncidentResult.ResolutionPath = "deterministic"
	c.currentIncidentResult.ResponderSessionID = sessionID
	c.currentIncidentRequest.PriorStages = []*contracts.StageReport{{Stage: "deterministic",
		Mitigations: []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{Applied: true}}}}}
	return c, dispatcher
}

func stepFor(t *testing.T, c *Controller, now time.Time, count int) {
	t.Helper()
	for range count {
		if err := c.Step(context.Background(), now, nil); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
}

// After an applied playbook, an opening alert that cleared while a joined user
// test keeps failing hands the incident to the agent.
func TestJoinedTestStillFailingAfterAPlaybookHandsOffToTheAgent(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, dispatcher := afterPlaybookJoinedController(t, now, "")
	stepFor(t, c, now, 8)
	if _, err := c.PrepareDispatchStep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.prepared.Load() == 0 {
		t.Fatalf("no agent hand-off for a joined test still failing: joined %v", c.currentIncidentRequest.JoinedKeys)
	}
}

// The agent runs at most once per incident: with its session already on the
// result, a joined test that still fails prepares no second hand-off.
func TestAgentThatAlreadyRanIsNeverDispatchedAgain(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, dispatcher := afterPlaybookJoinedController(t, now, "synthetic-session")
	stepFor(t, c, now, 8)
	if got := dispatcher.prepared.Load(); got != 0 {
		t.Fatalf("%d second hand-offs for an incident whose agent already ran", got)
	}
}
