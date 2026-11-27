package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

// A fault that starts while the incident is open shows only in state observed
// after the opening capture. Its alert joining the set stores the open state it
// joined in, so the closing turn's own-incident check can match a playbook
// written for that later fault.
func TestAJoinedAlertCapturesTheOpenStateItJoinedIn(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	later := &switchedProbe{id: memory.TrafficDetectorPrefix + "later", scenario: "clone"}
	opening.failing.Store(true)
	c, _ := alertSetController(t, now, opening, later)
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
	if matches, _ := filepath.Glob(filepath.Join(dir, "open@join-*.json")); len(matches) != 0 {
		t.Fatalf("captured a join with nothing joined: %v", matches)
	}
	later.failing.Store(true)
	stepFor(t, c, now.Add(2*time.Second), 2)
	if len(c.currentIncidentRequest.JoinedKeys) != 1 {
		t.Fatalf("joined keys = %v", c.currentIncidentRequest.JoinedKeys)
	}
	for _, name := range []string{"open@join-1.json", "open@join-1-recordings.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("no capture of the state the alert joined in: %v", err)
		}
	}
	if err := c.Step(context.Background(), now.Add(5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "open@join-*-recordings.json")); len(matches) != 1 {
		t.Fatalf("one join, captures %v", matches)
	}
}
