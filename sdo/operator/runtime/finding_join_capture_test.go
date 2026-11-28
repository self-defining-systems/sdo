package runtime

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

// A cause that starts while the incident is open can show only as a clue (a
// store's state, a drift, a learned detector's finding), never as a joining
// alert. Its first activation stores the open state it appeared in, once, so
// the closing turn can bind a playbook written for it to that capture.
func TestANewFindingDuringTheIncidentCapturesTheStateItAppearedIn(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	clue := &switchedProbe{id: "synthetic-store-state", scenario: "store"}
	opening.failing.Store(true)
	c, _ := alertSetController(t, now, opening, clue)
	root := t.TempDir()
	c.config.CorpusSessionRoot = root
	c.OnError = func(err error) { t.Log(err) }
	c.responderDone = false
	c.provider = staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
	c.currentIncidentRequest.Namespace = "demo"
	stepFor(t, c, now, 2)
	dir, err := memory.IncidentCorpusDir(root, "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if phases := sdktest.JoinCapturePhases(dir); len(phases) != 0 {
		t.Fatalf("captured with no new finding: %v", phases)
	}
	clue.failing.Store(true)
	stepFor(t, c, now.Add(2*time.Second), 2)
	if len(c.currentIncidentRequest.JoinedKeys) != 0 {
		t.Fatalf("a clue joined the alert set: %v", c.currentIncidentRequest.JoinedKeys)
	}
	for _, name := range []string{"open@join-1.json", "open@join-1-recordings.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("no capture of the state the finding appeared in: %v", err)
		}
	}
	record, err := memory.ReadJoinCaptureContext(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	key := FindingStateKey(clue.id, clue.finding().Fingerprint)
	if !slices.Contains(record.NewFindings, key) || len(record.JoinedKeys) != 0 {
		t.Fatalf("capture context = %+v, want new finding %s and no joined alert", record, key)
	}
	// The same finding clearing and firing again is not new.
	clue.failing.Store(false)
	stepFor(t, c, now.Add(5*time.Second), 2)
	clue.failing.Store(true)
	stepFor(t, c, now.Add(8*time.Second), 2)
	if phases := sdktest.JoinCapturePhases(dir); len(phases) != 1 {
		t.Fatalf("one new finding, captures %v", phases)
	}
}

// Captures of new findings are bounded per incident.
func TestNewFindingCapturesAreBoundedPerIncident(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	probes := []*switchedProbe{opening}
	for i := range maxFindingCaptures + 2 {
		probes = append(probes, &switchedProbe{id: "synthetic-clue-" + string(rune('a'+i)), scenario: "store"})
	}
	opening.failing.Store(true)
	c, _ := alertSetController(t, now, probes...)
	root := t.TempDir()
	c.config.CorpusSessionRoot = root
	c.responderDone = false
	c.provider = staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
	c.currentIncidentRequest.Namespace = "demo"
	at := now
	for _, probe := range probes[1:] {
		probe.failing.Store(true)
		stepFor(t, c, at, 2)
		at = at.Add(3 * time.Second)
	}
	dir, _ := memory.IncidentCorpusDir(root, "synthetic")
	if phases := sdktest.JoinCapturePhases(dir); len(phases) != maxFindingCaptures {
		t.Fatalf("captures = %v, want at most %d", phases, maxFindingCaptures)
	}
}

// findingCaptureController is an open incident with an opening alert and the
// given clue probes, storing captures under a fresh session root.
func findingCaptureController(t *testing.T, now time.Time, clues ...*switchedProbe) (*Controller, string) {
	t.Helper()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	opening.failing.Store(true)
	c, _ := alertSetController(t, now, append([]*switchedProbe{opening}, clues...)...)
	root := t.TempDir()
	c.config.CorpusSessionRoot = root
	c.responderDone = false
	c.provider = staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
	c.currentIncidentRequest.Namespace = "demo"
	stepFor(t, c, now, 2)
	dir, err := memory.IncidentCorpusDir(root, "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	return c, dir
}

// The session directory is writable by the responder, so a file planted there
// never decides the next capture's number: the controller keeps the count.
func TestAPlantedJoinCaptureDoesNotNumberTheNextOne(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	clue := &switchedProbe{id: "synthetic-store-state", scenario: "store"}
	c, dir := findingCaptureController(t, now, clue)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "open@join-99999999999999999999.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	clue.failing.Store(true)
	stepFor(t, c, now.Add(2*time.Second), 2)
	if _, err := os.Stat(filepath.Join(dir, "open@join-1.json")); err != nil {
		t.Fatalf("a planted capture blocked the next one: %v", err)
	}
	// The count survives a restart.
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if state.GetJoinCaptureIncidentId() != "synthetic" || state.GetJoinCaptures() != 1 {
		t.Fatalf("persisted join captures = %q/%d, want synthetic/1", state.GetJoinCaptureIncidentId(), state.GetJoinCaptures())
	}
	restarted, _ := alertSetController(t, now, clue)
	if err := restarted.RestoreState(&contracts.ControllerState{
		Version: RuntimeStateVersion, JoinCaptureIncidentId: state.GetJoinCaptureIncidentId(), JoinCaptures: state.GetJoinCaptures(),
	}); err != nil {
		t.Fatal(err)
	}
	if restarted.joinCaptures != (joinCaptureCounter{incident: "synthetic", taken: 1}) {
		t.Fatalf("restored join captures = %+v", restarted.joinCaptures)
	}
}

// A capture that fails keeps its findings due: the next pass captures them.
func TestAFailedFindingCaptureIsRetriedNextPass(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	clue := &switchedProbe{id: "synthetic-store-state", scenario: "store"}
	c, dir := findingCaptureController(t, now, clue)
	var errs []error
	c.OnError = func(err error) { errs = append(errs, err) }
	// A directory where the first capture's file goes makes it fail.
	if err := os.MkdirAll(filepath.Join(dir, "open@join-1-recordings.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	clue.failing.Store(true)
	stepFor(t, c, now.Add(2*time.Second), 2)
	if len(errs) == 0 {
		t.Fatal("the planted directory did not fail the capture")
	}
	stepFor(t, c, now.Add(4*time.Second), 2)
	record, err := memory.ReadJoinCaptureContext(dir, 2)
	if err != nil {
		t.Fatalf("the failed capture's finding was dropped: %v", err)
	}
	if key := FindingStateKey(clue.id, clue.finding().Fingerprint); !slices.Equal(record.NewFindings, []string{key}) {
		t.Fatalf("retried capture new findings = %v, want [%s]", record.NewFindings, key)
	}
}

// New findings are listed in key order, not in the order detectors finished.
func TestNewFindingsAreListedInKeyOrder(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	late := &switchedProbe{id: "synthetic-z-clue", scenario: "store"}
	early := &switchedProbe{id: "synthetic-a-clue", scenario: "store"}
	c, dir := findingCaptureController(t, now, late, early)
	c.queueFindingCapture(late.finding())
	c.queueFindingCapture(early.finding())
	stepFor(t, c, now.Add(2*time.Second), 2)
	record, err := memory.ReadJoinCaptureContext(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.IsSorted(record.NewFindings) || len(record.NewFindings) != 2 {
		t.Fatalf("new findings = %v, want both in key order", record.NewFindings)
	}
}
