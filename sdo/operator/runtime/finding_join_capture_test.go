package runtime

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

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
