package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// changingProvider serves the snapshot a test sets between steps.
type changingProvider struct{ snapshot sdktest.Snapshot }

func (p *changingProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return p.snapshot, nil
}

func lateCaptures(t *testing.T, c *Controller) []sdktest.Snapshot {
	t.Helper()
	c.Wait()
	dir, err := memory.IncidentCorpusDir(c.config.CorpusSessionRoot, "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	var got []sdktest.Snapshot
	for _, phase := range sdktest.LateCapturePhases() {
		raw, err := os.ReadFile(filepath.Join(dir, phase+".json"))
		if os.IsNotExist(err) {
			continue
		}
		var snapshot sdktest.Snapshot
		if err != nil || json.Unmarshal(raw, &snapshot) != nil {
			t.Fatalf("capture %s: %v", phase, err)
		}
		got = append(got, snapshot)
	}
	return got
}

// lateCaptureWorld opens the incident at the returned time with the opening
// test failing and a second, not yet failing.
func lateCaptureWorld(t *testing.T) (*Controller, *changingProvider, *switchedProbe, *switchedProbe, time.Time) {
	t.Helper()
	opened := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	second := &switchedProbe{id: memory.TrafficDetectorPrefix + "second", scenario: "clone"}
	opening.failing.Store(true)
	c, _ := alertSetController(t, opened, opening, second)
	provider := &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
	c.provider = provider
	c.config.CorpusSessionRoot = t.TempDir()
	c.config.Namespace = "demo"
	c.currentIncidentRequest.Namespace = "demo"
	c.incidentDetectedAt = opened
	c.responderDone = false
	return c, provider, opening, second, opened
}

func stepAt(t *testing.T, c *Controller, at time.Time) {
	t.Helper()
	c.now = func() time.Time { return at }
	if err := c.Step(context.Background(), at, nil); err != nil {
		t.Fatal(err)
	}
}

// The second cause of a compound fault appears after the opening capture. Its
// alert joins, and the incident keeps the state observed then, before any repair.
func TestASecondCauseIsCapturedWhenItsAlertJoins(t *testing.T) {
	c, provider, _, second, opened := lateCaptureWorld(t)
	stepAt(t, c, opened.Add(time.Second))
	if got := lateCaptures(t, c); len(got) != 0 {
		t.Fatalf("no new finding, yet %d captures", len(got))
	}
	provider.snapshot.ServiceList = []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "second-cause"}}}
	second.failing.Store(true)
	stepAt(t, c, opened.Add(40*time.Second))
	got := lateCaptures(t, c)
	if len(got) != 1 || len(got[0].Services()) != 1 || got[0].Services()[0].Name != "second-cause" {
		t.Fatalf("late captures = %+v, want the state holding the second cause", got)
	}
	// The same alert keeps failing: nothing new is observed, nothing is captured.
	stepAt(t, c, opened.Add(200*time.Second))
	if got := lateCaptures(t, c); len(got) != 1 {
		t.Fatalf("captures = %d after no new finding", len(got))
	}
}

// Each capture waits for twice the incident age of the one before, so a storm
// of findings costs few captures and later causes are still seen.
func TestLateCapturesAreSpacedAndBounded(t *testing.T) {
	c, _, _, _, opened := lateCaptureWorld(t)
	stepAt(t, c, opened.Add(time.Second))
	for age := time.Second; age < time.Hour; age += 5 * time.Second {
		at := opened.Add(age)
		c.mu.Lock()
		c.timeline = append(c.timeline, DetectorTimelineEntry{DetectorID: "clue", Fingerprint: age.String(), FirstActivatedAt: at})
		c.mu.Unlock()
		c.captureLate(context.Background(), at, sdktest.Snapshot{NamespaceName: "demo"})
		c.Wait()
		if age < lateCaptureFirstAge && len(lateCaptures(t, c)) != 0 {
			t.Fatalf("captured at age %s", age)
		}
	}
	if got := len(lateCaptures(t, c)); got != len(sdktest.LateCapturePhases()) {
		t.Fatalf("captures = %d, want the bound %d", got, len(sdktest.LateCapturePhases()))
	}
}

// Once the alerts cleared, the state is the recovered one and is never taken as
// an open capture.
func TestNoLateCaptureAfterRecoveryBegan(t *testing.T) {
	c, _, _, _, opened := lateCaptureWorld(t)
	at := opened.Add(time.Minute)
	c.mu.Lock()
	c.closureStableSince = opened.Add(50 * time.Second)
	c.timeline = append(c.timeline, DetectorTimelineEntry{DetectorID: "clue", FirstActivatedAt: at})
	c.mu.Unlock()
	c.captureLate(context.Background(), at, sdktest.Snapshot{NamespaceName: "demo"})
	if got := lateCaptures(t, c); len(got) != 0 {
		t.Fatalf("captures = %d after recovery began", len(got))
	}
}

// A restarted controller continues the numbering and the spacing.
func TestLateCapturesContinueAfterARestart(t *testing.T) {
	c, _, _, _, opened := lateCaptureWorld(t)
	stepAt(t, c, opened.Add(time.Second))
	capture := func(age time.Duration) {
		at := opened.Add(age)
		c.mu.Lock()
		c.timeline = append(c.timeline, DetectorTimelineEntry{DetectorID: "clue", Fingerprint: age.String(), FirstActivatedAt: at})
		c.mu.Unlock()
		c.captureLate(context.Background(), at, sdktest.Snapshot{NamespaceName: "demo"})
		c.Wait()
	}
	capture(20 * time.Second)
	c.late = lateCaptureState{}
	capture(25 * time.Second)
	if got := len(lateCaptures(t, c)); got != 1 {
		t.Fatalf("captures = %d: a restart repeated a capture at once", got)
	}
	capture(60 * time.Second)
	if got := len(lateCaptures(t, c)); got != 2 {
		t.Fatalf("captures = %d, want the second", got)
	}
}
