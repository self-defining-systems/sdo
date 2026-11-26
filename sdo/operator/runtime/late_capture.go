package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// lateCaptureFirstAge is the incident age before which no later capture is
// taken: open@detect already holds that state.
const lateCaptureFirstAge = 15 * time.Second

// lateCaptureState is the open incident's later captures. Guarded by c.mu.
type lateCaptureState struct {
	incidentID string
	// taken counts the phases used; after is when the last was taken (the
	// incident's detection before any), and age the incident's age then.
	taken int
	after time.Time
	age   time.Duration
	// done is closed when the capture in flight is written; nil when none is.
	done chan struct{}
}

// captureLate keeps the state of an incident whose alerts still fail when a
// finding new since the last capture appeared: a compound fault's later cause,
// or one that became visible only now, is absent from open@detect. It is the
// observation this step already made, so it costs no extra cluster read beyond
// the capture's own. Each capture waits for twice the incident age of the one
// before (at least lateCaptureFirstAge), so a storm of findings takes few and at
// most sdktest.MaxLateCaptures. None is taken once recovery began: that state is
// the recovered one. The write runs off the step loop; closure evidence waits
// for it.
func (c *Controller) captureLate(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) {
	root := c.config.CorpusSessionRoot
	if root == "" || snapshot == nil {
		return
	}
	alertFailing := c.alertSetFailing()
	c.mu.Lock()
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil || !c.closureStableSince.IsZero() || c.earlyReportPendingLocked() || !alertFailing {
		c.mu.Unlock()
		return
	}
	if c.late.incidentID != request.IncidentID {
		c.late = c.restoreLateCaptureLocked(root, request.IncidentID, now)
	}
	late := &c.late
	age := now.Sub(c.incidentDetectedAt)
	changed := slices.ContainsFunc(c.timeline, func(entry DetectorTimelineEntry) bool { return entry.FirstActivatedAt.After(late.after) })
	if late.done != nil || late.taken >= sdktest.MaxLateCaptures || !changed || age < max(lateCaptureFirstAge, 2*late.age) {
		c.mu.Unlock()
		return
	}
	late.taken++
	late.after, late.age = now, age
	phase := sdktest.LateCapturePhases()[late.taken-1]
	done := make(chan struct{})
	late.done = done
	id, namespace, refs := request.IncidentID, request.Namespace, findingCaptureRefs(request.Findings)
	c.mu.Unlock()
	comparable := c.drift != nil && c.drift.View != nil
	c.goBackground(func() {
		defer func() {
			c.mu.Lock()
			if c.late.done == done {
				c.late.done = nil
			}
			c.mu.Unlock()
			close(done)
		}()
		if err := memory.CaptureIncidentSnapshot(root, id, phase, snapshot, namespace, refs...); err != nil {
			c.reportLateCapture(fmt.Errorf("capture incident %s snapshot: %w", phase, err))
			return
		}
		if !comparable {
			return
		}
		budget, cancel := context.WithTimeout(ctx, sourceComparisonCaptureBudget)
		comparisons := boundaryObservations(budget, snapshot, c.drift.View(snapshot))
		cancel()
		if err := memory.AppendCapturedRecordings(root, id, phase, comparisons); err != nil {
			c.reportLateCapture(fmt.Errorf("capture incident %s source comparisons: %w", phase, err))
		}
	})
}

func (c *Controller) reportLateCapture(err error) {
	if c.OnError != nil {
		c.OnError(err)
	}
}

// restoreLateCaptureLocked continues a restarted controller's later captures
// from the files already written: the next phase, and spacing from now.
func (c *Controller) restoreLateCaptureLocked(root, incidentID string, now time.Time) lateCaptureState {
	state := lateCaptureState{incidentID: incidentID, after: c.incidentDetectedAt}
	dir, err := memory.IncidentCorpusDir(root, incidentID)
	if err != nil {
		return state
	}
	for _, phase := range sdktest.LateCapturePhases() {
		if _, err := os.Stat(filepath.Join(dir, phase+".json")); err != nil {
			break
		}
		state.taken++
	}
	if state.taken > 0 {
		state.after, state.age = now, now.Sub(c.incidentDetectedAt)
	}
	return state
}

// alertSetFailing reports whether an alert of the open incident is active, or
// the incident has no alert set (a page).
func (c *Controller) alertSetFailing() bool {
	c.mu.Lock()
	keys := alertKeys(c.currentIncidentRequest)
	c.mu.Unlock()
	if len(keys) == 0 {
		return true
	}
	for _, state := range c.tracker.Snapshot() {
		if state.Active && slices.Contains(keys, FindingStateKey(state.DetectorID, FindingFingerprint(state.Finding))) {
			return true
		}
	}
	return false
}

// awaitLateCapture returns once the later capture in flight is written, so the
// closure's evidence and the closing turn see a settled capture directory.
func (c *Controller) awaitLateCapture(ctx context.Context) {
	c.mu.Lock()
	done := c.late.done
	c.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}
