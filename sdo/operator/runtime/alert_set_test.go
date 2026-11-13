package runtime

import (
	"context"
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
// failing since the incident opened, still hurts users. Recovered waits on the
// whole alert set, so the incident stays open until that test passes too.
func TestRecoveredWaitsForUserTestsThatFailWhileTheIncidentIsOpen(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	hurt := &switchedProbe{id: memory.TrafficDetectorPrefix + "hurt", scenario: "clone"}
	hurt.failing.Store(true)
	c, closed := alertSetController(t, now, opening, hurt)
	step := func(count int) {
		for range count {
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Second)
		}
	}
	step(8)
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

// Only a user test inside the join window joins. A clue, a Kubernetes
// condition and a test that starts failing after the window never do.
func TestOnlyUserTestsInTheJoinWindowJoinTheAlertSet(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	c, _ := alertSetController(t, now, opening)
	userTest := (&switchedProbe{id: memory.TrafficDetectorPrefix + "late", scenario: "clone"}).finding()
	clue := sdk.Finding{DetectorID: "resource-saturation", RuleID: "cpu", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "cpu", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "web"}}
	condition := sdk.Finding{DetectorID: memory.KubernetesConditionDetectorIDs()[0], RuleID: "stuck", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "stuck", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "web"}}
	ctx := context.Background()
	for _, finding := range []sdk.Finding{clue, condition} {
		c.admitActivation(ctx, finding, now, sdk.DetectorSpec{})
	}
	if len(c.currentIncidentRequest.JoinedKeys) != 0 {
		t.Fatalf("a clue or condition joined the alert set: %v", c.currentIncidentRequest.JoinedKeys)
	}
	c.admitActivation(ctx, userTest, c.incidentDetectedAt.Add(AlertJoinWindow), sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.JoinedKeys) != 1 {
		t.Fatalf("a user test at the window edge did not join: %v", c.currentIncidentRequest.JoinedKeys)
	}
	c.currentIncidentRequest.JoinedKeys = nil
	c.admitActivation(ctx, userTest, c.incidentDetectedAt.Add(AlertJoinWindow+time.Second), sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.JoinedKeys) != 0 {
		t.Fatalf("a user test after the window joined: %v", c.currentIncidentRequest.JoinedKeys)
	}
}

// sdo detect and sdo verify name the alert set Recovered waits on.
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
