package runtime

import (
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

// takeFindingCapturesLocked returns the findings due a capture for incident
// and clears them; past the bound it returns none. Caller holds c.mu.
func (c *Controller) takeFindingCapturesLocked(incident string) []sdk.Finding {
	state := &c.findingCaptures
	if state.incident != incident || len(state.due) == 0 {
		return nil
	}
	due := state.due
	state.due = nil
	if state.passes >= maxFindingCaptures {
		return nil
	}
	state.passes++
	return due
}
