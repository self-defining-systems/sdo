package runtime

import (
	"slices"
	"strings"

	"sdo.dev/operator/sdk"
)

// maxFindingCaptures bounds the join captures new findings take per incident.
// Captures for joined alerts are bounded by the join window instead.
const maxFindingCaptures = 6

// findingCaptureState is the open incident's new non-alert findings: those
// seen once (deduplicated by finding key) and those still due a capture.
type findingCaptureState struct {
	incident string
	seen     map[string]struct{}
	due      []sdk.Finding
	passes   int
}

// queueFindingCapture marks a finding that first became active in the open
// incident without joining its alert set (a clue, a Kubernetes condition, a
// finding attached before launch). A cause that starts while the incident is
// open may show only there; the next observation stores the state it appeared
// in as a join capture, so a playbook written for that cause can bind to it.
// A finding is captured once per incident.
func (c *Controller) queueFindingCapture(finding sdk.Finding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil || c.config.CorpusSessionRoot == "" {
		return
	}
	state := &c.findingCaptures
	if state.incident != request.IncidentID {
		*state = findingCaptureState{incident: request.IncidentID, seen: map[string]struct{}{}}
	}
	finding.Fingerprint = FindingFingerprint(finding)
	key := FindingStateKey(finding.DetectorID, finding.Fingerprint)
	if _, seen := state.seen[key]; seen {
		return
	}
	state.seen[key] = struct{}{}
	state.due = append(state.due, finding)
}

// dueFindingCapturesLocked returns the findings due a capture for incident,
// in key order, and counts the pass against the bound; past it, it returns
// none. They stay due until findingCapturesTakenLocked: a failed capture
// retries them next pass. Caller holds c.mu.
func (c *Controller) dueFindingCapturesLocked(incident string) []sdk.Finding {
	state := &c.findingCaptures
	if state.incident != incident || len(state.due) == 0 || state.passes >= maxFindingCaptures {
		return nil
	}
	state.passes++
	due := slices.Clone(state.due)
	slices.SortFunc(due, func(a, b sdk.Finding) int {
		return strings.Compare(FindingStateKey(a.DetectorID, a.Fingerprint), FindingStateKey(b.DetectorID, b.Fingerprint))
	})
	return due
}

// findingCapturesTakenLocked clears the findings a capture stored. Findings
// queued since dueFindingCapturesLocked stay due. Caller holds c.mu.
func (c *Controller) findingCapturesTakenLocked(incident string, taken []sdk.Finding) {
	state := &c.findingCaptures
	if state.incident != incident {
		return
	}
	state.due = slices.DeleteFunc(state.due, func(finding sdk.Finding) bool {
		return slices.ContainsFunc(taken, func(t sdk.Finding) bool {
			return t.DetectorID == finding.DetectorID && t.Fingerprint == finding.Fingerprint
		})
	})
}

// joinCaptureCounter is how many join captures (open@join-n) incident has
// taken. The controller keeps and persists it: the session directory the
// captures live in is writable by the responder, so a file planted there
// never decides a number.
type joinCaptureCounter struct {
	incident string
	taken    int
}

// nextJoinCaptureLocked reserves incident's next join capture number. A
// number is used once even when its capture fails, since a failed capture can
// leave a write-once file behind. Caller holds c.mu.
func (c *Controller) nextJoinCaptureLocked(incident string) int {
	if c.joinCaptures.incident != incident {
		c.joinCaptures = joinCaptureCounter{incident: incident}
	}
	c.joinCaptures.taken++
	return c.joinCaptures.taken
}
