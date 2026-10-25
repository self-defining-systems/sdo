package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Caller holds c.mu. Stable source-absent objects cannot become accepted merely
// by waiting. Pending deletion/GC, changed findings and health windows retain
// the ordinary verification grace. This path never establishes recovery.
func (c *Controller) terminalInventoryBlockerLocked(verification ToolResponse) bool {
	result := c.currentIncidentResult
	if result == nil || result.Status != IncidentFailed || (!result.ResponseLoopExhausted && result.Error != "authorization_denied_no_progress" && result.Error != "no_progress" && result.Error != "shared_ownership") || verification.Error != "" || len(verification.Findings) == 0 || len(verification.Findings) != len(result.UnresolvedFindings) {
		return false
	}
	if result.Error == "shared_ownership" && !sharedOwnershipBlocker(verification, nil) {
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
				_, ttl, _ := unstructured.NestedInt64(object.Object, "spec", "ttlSecondsAfterFinished")
				stable = object.GetDeletionTimestamp() == nil && len(object.GetOwnerReferences()) == 0 && !(object.GetKind() == "Job" && ttl)
				for _, receipt := range result.RepairActions {
					for _, ref := range receipt.Resources {
						if ref == finding.PrimaryResource && (receipt.Success || receipt.FailureReason == "outcome_unknown" || receipt.FailureReason == "delete_not_confirmed") {
							stable = false
						}
					}
				}
				for _, application := range result.PlanApplications {
					if !application.Result.Applied {
						continue
					}
					for _, patch := range application.Result.Patches {
						if patch.Ref == finding.PrimaryResource {
							stable = false
						}
					}
				}
				break
			}
		}
		if !stable {
			return false
		}
	}
	return true
}

// Shared authorization requires escalation rather than another responder turn.
// Successful or uncertain writes retain observation time before this shortcut.
func sharedOwnershipBlocker(verification ToolResponse, repairs []continuationRepair) bool {
	if verification.Error != "" || len(verification.Findings) == 0 {
		return false
	}
	for _, finding := range verification.Findings {
		if !inventoryFinding(finding) || finding.Metadata["removal_blocker"] != "shared_ownership" {
			return false
		}
		for _, repair := range repairs {
			receipt := repair.Receipt
			if !receipt.Success && receipt.FailureReason != "outcome_unknown" && receipt.FailureReason != "delete_not_confirmed" {
				continue
			}
			for _, ref := range receipt.Resources {
				if ref == finding.PrimaryResource {
					return false
				}
			}
		}
	}
	return true
}
