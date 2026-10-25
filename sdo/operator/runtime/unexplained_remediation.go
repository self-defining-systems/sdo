package runtime

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// UnexplainedRemoval binds a menu selection to one observed incarnation. It is
// descriptive until the controller checks it again at the delete boundary.
type UnexplainedRemoval struct {
	FindingFingerprint string
	Ref                sdk.ObjectRef
	UID                types.UID
	ResourceVersion    string
}

func (c *Controller) unexplainedRemovals(incidentID, controlNamespace string) ([]UnexplainedRemoval, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unexplainedRemovalsLocked(incidentID, controlNamespace)
}

// RemovalBlockReason describes why a current finding has no guarded option.
type RemovalBlockReason string

const (
	RemovalSharedOwnership      RemovalBlockReason = "shared_ownership"
	RemovalMissingIdentity      RemovalBlockReason = "missing_object_identity"
	RemovalProtectedObject      RemovalBlockReason = "protected_object"
	RemovalSourceDeclared       RemovalBlockReason = "source_declared"
	RemovalObjectUnavailable    RemovalBlockReason = "object_unavailable"
	RemovalUnavailable          RemovalBlockReason = "removal_unavailable"
	RemovalInventoryUnavailable RemovalBlockReason = "inventory_unavailable"
)

type UnexplainedRemovalBlocker struct {
	FindingFingerprint string             `json:"finding_fingerprint"`
	Ref                sdk.ObjectRef      `json:"resource"`
	Reason             RemovalBlockReason `json:"reason"`
}

type UnexplainedRemovalAssessment struct {
	Removals []UnexplainedRemoval
	Blockers []UnexplainedRemovalBlocker
}

func (c *Controller) unexplainedRemovalAssessment(incidentID, controlNamespace string) (UnexplainedRemovalAssessment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unexplainedRemovalAssessmentLocked(incidentID, controlNamespace)
}

func (c *Controller) unexplainedRemovalsLocked(incidentID, controlNamespace string) ([]UnexplainedRemoval, error) {
	assessment, err := c.unexplainedRemovalAssessmentLocked(incidentID, controlNamespace)
	return assessment.Removals, err
}

func (c *Controller) unexplainedRemovalAssessmentLocked(incidentID, controlNamespace string) (UnexplainedRemovalAssessment, error) {
	result := UnexplainedRemovalAssessment{}
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		return result, fmt.Errorf("incident is no longer current")
	}
	if c.drift == nil || !c.drift.Inventory {
		for _, finding := range c.currentIncidentRequest.Findings {
			if finding.DetectorID == "drift.unexplained" {
				result.Blockers = append(result.Blockers, UnexplainedRemovalBlocker{FindingFingerprint: FindingFingerprint(finding), Ref: finding.PrimaryResource, Reason: RemovalInventoryUnavailable})
			}
		}
		return result, nil
	}
	_, findings := c.verifySourceInventoryLocked(nil)
	source, ok := c.drift.Events.(sourceInventoryReader)
	if !ok || c.sourceInventory == nil {
		return result, fmt.Errorf("accepted source inventory is unavailable")
	}
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for _, object := range source.SourceDriftObjects() {
		if object != nil {
			objects[inventoryRef(object)] = object
		}
	}
	eligible := map[sdk.ObjectRef]bool{}
	for _, finding := range findings {
		if inventoryEvent(finding).Source == "absent from accepted source" {
			eligible[finding.PrimaryResource] = true
		}
	}
	ownership := removalOwnershipObjects(source)
	for _, finding := range findings {
		if finding.DetectorID != "drift.unexplained" {
			continue
		}
		object := objects[finding.PrimaryResource]
		reason := RemovalBlockReason("")
		switch {
		case finding.RuleID == "source-inventory-unavailable":
			reason = RemovalInventoryUnavailable
		case !eligible[finding.PrimaryResource]:
			reason = RemovalSourceDeclared
		case object == nil:
			reason = RemovalObjectUnavailable
		case inventoryOwned(object) || object.GetNamespace() == controlNamespace || object.GetKind() == "Secret":
			reason = RemovalProtectedObject
		case !exclusiveRemovalOwnership(object, c.sourceInventory.Namespace, ownership, eligible):
			reason = RemovalSharedOwnership
		case object.GetUID() == "" || object.GetResourceVersion() == "":
			reason = RemovalMissingIdentity
		}
		if reason != "" {
			result.Blockers = append(result.Blockers, UnexplainedRemovalBlocker{FindingFingerprint: FindingFingerprint(finding), Ref: finding.PrimaryResource, Reason: reason})
			continue
		}
		result.Removals = append(result.Removals, UnexplainedRemoval{FindingFingerprint: FindingFingerprint(finding), Ref: finding.PrimaryResource, UID: object.GetUID(), ResourceVersion: object.GetResourceVersion()})
	}
	return result, nil
}

// Keep accepted-source and incident identity stable across authorization and the
// API write. Kubernetes atomically checks the exact UID and resourceVersion.
func (c *Controller) removeUnexplained(ctx context.Context, incidentID, controlNamespace string, selected UnexplainedRemoval, client PlanLifecycleClient, ref sdk.ObjectRef, uid types.UID, rv string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ref != selected.Ref || uid != selected.UID || rv != selected.ResourceVersion {
		return fmt.Errorf("unexplained removal identity/version changed")
	}
	current, err := c.unexplainedRemovalsLocked(incidentID, controlNamespace)
	if err != nil {
		return err
	}
	for _, candidate := range current {
		if candidate == selected {
			if checker, ok := client.(interface {
				CheckRemovalOwnership(context.Context, sdk.ObjectRef, map[sdk.ObjectRef]bool) error
			}); ok {
				eligible := map[sdk.ObjectRef]bool{}
				for _, option := range current {
					eligible[option.Ref] = true
				}
				if err := checker.CheckRemovalOwnership(ctx, ref, eligible); err != nil {
					return err
				}
			}
			return client.Delete(ctx, ref, uid, rv)
		}
	}
	return fmt.Errorf("object is no longer the selected unexplained finding at %s", time.Now().UTC().Format(time.RFC3339Nano))
}
