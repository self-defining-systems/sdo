package runtime

import (
	"sort"
	"time"

	"sdo.dev/operator/sdk"
)

// ControllerStateEnvironment names the responder environment variable that
// locates the controller's state ConfigMap as NAMESPACE/NAME, so `sdo incident
// status` can read the open incident's IncidentView.
const ControllerStateEnvironment = "SDO_CONTROLLER_STATE"

const controllerStateConfigMap = "sdo-controller-state"

func controllerStateLocation(controlNamespace string) string {
	return controlNamespace + "/" + controllerStateConfigMap
}

// IncidentView is the open incident as the controller sees it at its latest
// evaluation. The dispatched request is a snapshot taken when the incident
// opened; the view keeps moving, so a responder can ask what still blocks
// closure and what changed since dispatch. It is published with the runtime
// state but never restored: a restarted controller republishes it once it has
// evaluated again.
type IncidentView struct {
	IncidentID string    `json:"incident_id"`
	ObservedAt time.Time `json:"observed_at"`
	// BlockingFindings are the active findings closure waits on (every
	// health detector's, or without any, the incident's own) that the submit
	// gate has not confirmed clear (GateConfirmationPolicy); BlockingDetectors
	// names their detectors. A responder must not submit while any remains.
	BlockingDetectors []string      `json:"blocking_detectors"`
	BlockingFindings  []sdk.Finding `json:"blocking_findings"`
	// ClearingFindings are the findings closure still counts as active but
	// the gate has confirmed clear on consecutive fresh evaluations. They
	// hold closure only until the health judge's own clear persistence is
	// met, and do not block submission.
	ClearingFindings []sdk.Finding `json:"clearing_findings"`
	// StateChanges is the current configuration diff against the healthy
	// baseline, including changes made after the request was taken.
	StateChanges *StateChanges `json:"state_changes,omitempty"`
}

// refreshIncidentView recomputes the view after an evaluation, or when the
// published view does not belong to the open incident.
func (c *Controller) refreshIncidentView(now time.Time, evaluated bool) {
	c.mu.Lock()
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		c.incidentView = nil
		c.mu.Unlock()
		return
	}
	incidentID := c.currentIncidentRequest.IncidentID
	if !evaluated && c.incidentView != nil && c.incidentView.IncidentID == incidentID {
		c.mu.Unlock()
		return
	}
	active := c.blockingFindingsLocked()
	c.mu.Unlock()

	findings := make([]sdk.Finding, 0, len(active))
	clearing := make([]sdk.Finding, 0)
	for _, finding := range active {
		if c.gateConfirmedKey(FindingStateKey(finding.DetectorID, FindingFingerprint(finding)), now) {
			clearing = append(clearing, finding)
			continue
		}
		findings = append(findings, finding)
	}
	blocking := make([]string, 0)
	seen := make(map[string]struct{})
	for _, finding := range findings {
		if _, ok := seen[finding.DetectorID]; !ok {
			seen[finding.DetectorID] = struct{}{}
			blocking = append(blocking, finding.DetectorID)
		}
	}
	sort.Strings(blocking)
	var changes *StateChanges
	if c.Baseline != nil {
		changes = cloneStateChanges(c.Baseline.Changes(now))
	}
	view := &IncidentView{
		IncidentID: incidentID, ObservedAt: now.UTC(),
		BlockingDetectors: blocking, BlockingFindings: findings, ClearingFindings: clearing,
		StateChanges: changes,
	}
	c.mu.Lock()
	c.incidentView = view
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID {
		c.observeStateChangesLocked(changes, now)
	}
	c.mu.Unlock()
}

// blockingFindingsLocked mirrors finalVerificationStates: closure waits on
// every active health-detector finding, or without health detectors, on the
// incident's own findings.
func (c *Controller) blockingFindingsLocked() []sdk.Finding {
	health := make(map[string]struct{}, len(c.healthDetectorIDs))
	for _, detectorID := range c.healthDetectorIDs {
		health[detectorID] = struct{}{}
	}
	incident := make(map[string]struct{}, len(c.incidentFindingKeys))
	for _, key := range c.incidentFindingKeys {
		incident[key] = struct{}{}
	}
	findings := make([]sdk.Finding, 0)
	for key, state := range c.tracker.Snapshot() {
		if !state.Active {
			continue
		}
		_, isHealth := health[state.DetectorID]
		_, isIncident := incident[key]
		if (len(health) > 0 && isHealth) || (len(health) == 0 && isIncident) {
			findings = append(findings, state.Finding)
		}
	}
	sort.Slice(findings, func(left int, right int) bool {
		return findings[left].Fingerprint < findings[right].Fingerprint
	})
	return findings
}

func cloneIncidentView(view *IncidentView) *IncidentView {
	if view == nil {
		return nil
	}
	copy := *view
	// Empty lists stay empty, never null, for the responder's schema.
	copy.BlockingDetectors = append(make([]string, 0, len(view.BlockingDetectors)), view.BlockingDetectors...)
	copy.BlockingFindings = append(make([]sdk.Finding, 0, len(view.BlockingFindings)), view.BlockingFindings...)
	copy.StateChanges = cloneStateChanges(view.StateChanges)
	copy.ClearingFindings = append(make([]sdk.Finding, 0, len(view.ClearingFindings)), view.ClearingFindings...)
	return &copy
}
