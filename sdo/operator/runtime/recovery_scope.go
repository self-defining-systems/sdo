package runtime

import "sdo.dev/operator/sdk"

// recoveryScope decides which active health findings hold an incident's
// recovery proof. Every outage finding (critical or latent) does. A
// degradation finding (warning or info severity) does only when the incident
// opened on it: a degradation that appears elsewhere during the incident, such
// as a slow link on a loaded node, is not evidence that this repair failed. It
// stays tracked and opens its own incident. A nil scope (no open incident)
// holds every finding.
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
	return newRecoveryScope(c.incidentFindingKeys)
}

func (c *Controller) recoveryScope() recoveryScope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recoveryScopeLocked()
}

// holds reports whether an active finding holds the recovery proof.
func (s recoveryScope) holds(finding sdk.Finding) bool {
	if s == nil || (finding.Severity != sdk.SeverityWarning && finding.Severity != sdk.SeverityInfo) {
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
