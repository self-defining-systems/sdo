package runtime

import "sdo.dev/operator/sdk"

// recoveryScope is the opening-alert scope: only the alerts an incident
// opened on (its opening keys, at any severity) hold its recovery proof. A
// clue, or a later opener waiting in the batcher for the next incident, is not
// evidence that this repair failed. A nil scope (no open incident) holds every
// finding.
type recoveryScope map[string]struct{}

func newRecoveryScope(keys []string) recoveryScope {
	scope := make(recoveryScope, len(keys))
	for _, key := range keys {
		scope[key] = struct{}{}
	}
	return scope
}

// recoveryScopeLocked is the open incident's scope, or nil. Caller holds c.mu.
func (c *Controller) recoveryScopeLocked() recoveryScope {
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		return nil
	}
	return newRecoveryScope(alertKeys(c.currentIncidentRequest))
}

func (c *Controller) recoveryScope() recoveryScope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recoveryScopeLocked()
}

// holds reports whether an active finding holds the recovery proof.
func (s recoveryScope) holds(finding sdk.Finding) bool {
	if s == nil {
		return true
	}
	_, own := s[FindingStateKey(finding.DetectorID, FindingFingerprint(finding))]
	return own
}

// blocking returns the active findings the scope holds.
func (s recoveryScope) blocking(findings []sdk.Finding) []sdk.Finding {
	var held []sdk.Finding
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive && s.holds(finding) {
			held = append(held, finding)
		}
	}
	return held
}
