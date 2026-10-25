package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Caller holds c.mu. Stable source-absent objects cannot become accepted merely
// by waiting. Pending deletion/GC, changed findings and health windows retain
// the ordinary verification grace. This path never establishes recovery.
func (c *Controller) terminalInventoryBlockerLocked(verification ToolResponse) bool {
	result := c.currentIncidentResult
	if result == nil || result.Status != IncidentFailed || (!result.ResponseLoopExhausted && result.Error != "authorization_denied_no_progress") || verification.Error != "" || len(verification.Findings) == 0 || len(verification.Findings) != len(result.UnresolvedFindings) {
		return false
	}
	prior := map[string]bool{}
	for _, finding := range result.UnresolvedFindings {
		prior[FindingFingerprint(finding)] = true
	}
	if c.drift == nil {
		return false
	}
	source, ok := c.drift.Events.(interface {
		SourceDriftObjects() []*unstructured.Unstructured
	})
	if !ok {
		return false
	}
	objects := source.SourceDriftObjects()
	for _, finding := range verification.Findings {
		if !prior[FindingFingerprint(finding)] || !inventoryFinding(finding) || inventoryEvent(finding).Source != "absent from accepted source" {
			return false
		}
		stable := false
		for _, object := range objects {
			if object != nil && inventoryRef(object) == finding.PrimaryResource {
				stable = object.GetDeletionTimestamp() == nil && len(object.GetOwnerReferences()) == 0
				break
			}
		}
		if !stable {
			return false
		}
	}
	return true
}
