//go:build integration

package runtime

import (
	"context"
	"testing"
	"time"
)

// A verified incident's closure evidence (corpus captures, replayed reads,
// settlement) can take seconds on a real cluster. Detectors must keep
// evaluating meanwhile: a pause there is blindness to every other fault, and
// the controller's detection-continuity contract bounds it.
func TestSimSlowClosureEvidenceDoesNotStopDetection(t *testing.T) {
	w := newSimWorld(t, 41)
	w.client.ReactionChain = w.client.ReactionChain[2:]
	w.responder.behavior = func() simBehavior { return simFixAll }
	w.broker.reject = func() bool { return false }
	w.config.CorpusSessionRoot = t.TempDir()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var evidenceCalls int
	w.configure = func(c *Controller) {
		c.closureEvidence = func(context.Context, closureEvidenceInput) (closureEvidence, error) {
			evidenceCalls++
			entered <- struct{}{}
			<-release
			return closureEvidence{}, nil
		}
	}
	w.restart()
	w.setPort(simServiceName(0), 9999)

	// step runs one controller pass. A pass that waits on the slow evidence is
	// the failure under test, so it is reported rather than waited out.
	step := func() {
		t.Helper()
		w.clock.Advance(time.Second)
		result := make(chan error, 1)
		go func() { result <- w.controller.Step(context.Background(), w.clock.Now(), nil) }()
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("step at %s: %v", w.clock.Now(), err)
			}
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatal("a controller pass blocked on closure evidence; detection stopped")
		}
		w.persist()
		w.drainEffects()
	}
	for tick := 0; len(entered) == 0; tick++ {
		if tick > 300 {
			t.Fatal("incident never reached verified closure evidence")
		}
		step()
		time.Sleep(time.Millisecond)
	}

	iterationsBefore := w.evaluationIterations()
	for tick := 0; tick < 20; tick++ {
		step()
	}
	if got := w.evaluationIterations() - iterationsBefore; got < 3 {
		t.Fatalf("detectors evaluated %d times in 20 virtual seconds of slow closure evidence", got)
	}
	if _, closing := w.controller.PendingIncidentClosure(); closing || !w.controller.IncidentOpen() {
		t.Fatal("the closure froze before its evidence was ready")
	}

	close(release)
	for tick := 0; ; tick++ {
		if tick > 300 {
			t.Fatal("closure never froze after its evidence completed")
		}
		step()
		if _, closing := w.controller.PendingIncidentClosure(); closing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if evidenceCalls != 1 {
		t.Fatalf("closure evidence gathered %d times, want once", evidenceCalls)
	}
}

func (w *simWorld) evaluationIterations() int {
	w.controller.mu.Lock()
	defer w.controller.mu.Unlock()
	return w.controller.evaluationIteration
}

// drainEffects starts the pending workspace and dispatch effects without
// waiting on closure effects, which the caller observes itself.
func (w *simWorld) drainEffects() {
	ctx := context.Background()
	if effect, ok := w.controller.PendingWorkspaceEffect(); ok {
		if err := w.controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
			w.t.Fatal(err)
		}
		w.await(func() bool { return len(w.controller.workspaceResults) > 0 })
	} else if effect, ok := w.controller.PendingDispatchEffect(); ok {
		if err := w.controller.ExecuteDispatchEffect(ctx, effect); err != nil {
			w.t.Fatal(err)
		}
		w.await(func() bool { return len(w.controller.results) > 0 })
	}
}
