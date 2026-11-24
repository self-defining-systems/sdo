package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// An evaluation that could not run (the memory host still running the previous
// pass, an abandoned or overrun evaluation) observed nothing: it is no new
// observation, so it never ends a clear streak. Only an evaluation that read
// the cluster and found an active alert, or a detector failure, ends it.
func TestUnavailableDetectorEvaluationKeepsClearStreak(t *testing.T) {
	since := time.Unix(100, 0).UTC()
	for name, err := range map[string]error{
		"still running":    sdk.ErrorWithClass("detector is still running a previous evaluation", sdk.ReadClassUnavailable),
		"abandoned":        sdk.ErrorWithClass("detector evaluation abandoned: context deadline exceeded", sdk.ReadClassUnavailable),
		"transport":        fmt.Errorf("memory host evaluate: %w", context.DeadlineExceeded),
		"wrapped sentinel": fmt.Errorf("read: %w", sdk.ErrUnavailable),
	} {
		t.Run(name, func(t *testing.T) {
			c := openersController(t, &recordingDispatcher{})
			c.trackClearStreak("traffic-probe-read-later", since, true)
			c.recordDetectorError("traffic-probe-read-later", since.Add(30*time.Second), err)
			if got := c.detectorClearSince["traffic-probe-read-later"]; !got.Equal(since) {
				t.Fatalf("clear streak = %v after an unavailable evaluation, want it kept at %v", got, since)
			}
			// The evaluation is still recorded, so nothing reads the missing
			// pass as a clear one.
			last := c.history[len(c.history)-1]
			if last.Status != DetectorEvaluationError {
				t.Fatalf("recorded status %q, want error", last.Status)
			}
		})
	}
}

// A detector that failed (panic, invalid finding, refused read, unregistered)
// observed nothing trustworthy about health either way: its streak ends.
func TestDetectorFailureEndsClearStreak(t *testing.T) {
	since := time.Unix(100, 0).UTC()
	for name, err := range map[string]error{
		"plain":   errors.New("boom"),
		"invalid": sdk.ErrorWithClass("detector panic: boom", sdk.ReadClassInvalid),
		"refused": sdk.ErrorWithClass("forbidden", sdk.ReadClassRefused),
	} {
		t.Run(name, func(t *testing.T) {
			c := openersController(t, &recordingDispatcher{})
			c.trackClearStreak("traffic-probe-read-later", since, true)
			c.recordDetectorError("traffic-probe-read-later", since.Add(30*time.Second), err)
			if _, ok := c.detectorClearSince["traffic-probe-read-later"]; ok {
				t.Fatal("a failed detector kept its clear streak")
			}
		})
	}
}

// Tracker history survives an unavailable pass, but its recovery proof must
// start again after the observation gap.
func TestSkippedRequiredPassRestartsRecoveryWindow(t *testing.T) {
	const id = "traffic-probe-read-later"
	since := time.Unix(100, 0).UTC()
	window := 60 * time.Second
	c := openersController(t, &recordingDispatcher{})
	c.config.ClosureStabilityWindow = window
	c.healthDetectorIDs = []string{id}
	c.incidentOpen = true
	c.currentIncidentRequest = &IncidentRequest{OpeningKeys: []string{FindingStateKey(id, "x")}}
	c.recoverySafetyLost = false
	scope := newRecoveryScope([]string{FindingStateKey(id, "x")})

	c.recordEvaluation(id, since, nil, scope)
	c.recordDetectorError(id, since.Add(20*time.Second), sdk.ErrorWithClass("detector is still running a previous evaluation", sdk.ReadClassUnavailable))
	now := since.Add(65 * time.Second)
	c.recordEvaluation(id, now, nil, scope)

	states, ok := c.latestEvaluations([]string{id}, time.Time{}, true)
	if !ok {
		t.Fatal("latest evaluation is not clear")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recoveryProofForEpochLocked(now, states, true, since) {
		t.Fatal("recovery reused the clear window across an unavailable observation")
	}
	if !c.recoveryProofForEpochLocked(now.Add(window), states, true, since) {
		t.Fatal("recovery did not complete after a new stability window")
	}

	// Never observed clear: no streak, no proof.
	other := "traffic-probe-write"
	c.history = append(c.history, DetectorEvaluation{DetectorID: other, EvaluatedAt: now, Status: DetectorEvaluationClear})
	otherStates, _ := c.latestEvaluations([]string{other}, time.Time{}, true)
	if c.recoveryProofForEpochLocked(now, otherStates, true, since) {
		t.Fatal("a detector never observed clear passed the proof")
	}
}

// Findings outside the incident's alert set (clues, detectors that own none of
// its alerts) are not observations of the incident: they neither fire the
// detector's evaluation nor end the alert-set detectors' clear streaks.
func TestFindingsOutsideAlertSetKeepRecoveryStreak(t *testing.T) {
	const alertDetector, clueDetector = "traffic-probe-read-later", "traffic-links"
	since := time.Unix(100, 0).UTC()
	c := openersController(t, &recordingDispatcher{})
	c.healthDetectorIDs = []string{alertDetector, clueDetector}
	opening := sdk.Finding{DetectorID: alertDetector, Status: sdk.FindingActive, Fingerprint: "read-later"}
	request := IncidentRequest{IncidentID: "i", Findings: []sdk.Finding{opening}, OpeningKeys: []string{FindingStateKey(alertDetector, "read-later")}}
	c.incidentOpen, c.currentIncidentRequest = true, &request
	c.trackClearStreak(alertDetector, since, true)
	c.closureStableSince = since

	clue := sdk.Finding{DetectorID: clueDetector, Status: sdk.FindingActive, Fingerprint: "latency"}
	scope := c.recoveryScope()
	c.recordEvaluation(clueDetector, since.Add(30*time.Second), []sdk.Finding{clue}, scope)

	if got := c.detectorClearSince[alertDetector]; !got.Equal(since) {
		t.Fatalf("alert detector streak = %v, want %v", got, since)
	}
	if last := c.history[len(c.history)-1]; last.Status != DetectorEvaluationClear {
		t.Fatalf("a finding outside the alert set made its detector %q, want clear", last.Status)
	}
	if got := c.recoveryDetectorIDs(request); len(got) != 1 || got[0] != alertDetector {
		t.Fatalf("recovery waits on %v, want only %s", got, alertDetector)
	}
	if got := c.activeHealthDetectors(); len(got) != 0 {
		t.Fatalf("active health detectors %v, want none for a clue outside the alert set", got)
	}
}
