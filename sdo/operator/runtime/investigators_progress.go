package runtime

import (
	"slices"
	"strings"
)

// alertSetProgressed reports whether an alert of the open incident id cleared since detection: its detector's
// latest evaluation is clear, or fires without the alert's fingerprint. A clue clearing is not progress. It reads
// only the controller's own evaluations, so it holds with agent.recording off.
func (c *Controller) alertSetProgressed(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	request := c.currentIncidentRequest
	if request == nil || request.IncidentID != id || !c.incidentOpen {
		return false
	}
	for _, key := range alertKeys(request) {
		detectorID, fingerprint, _ := strings.Cut(key, "\x00")
		states, _ := c.latestEvaluations([]string{detectorID}, request.DetectedAt, false)
		if len(states) == 0 {
			continue
		}
		switch states[0].Status {
		case DetectorEvaluationClear:
			return true
		case DetectorEvaluationFiring:
			if !slices.Contains(states[0].Fingerprints, fingerprint) {
				return true
			}
		}
	}
	return false
}
