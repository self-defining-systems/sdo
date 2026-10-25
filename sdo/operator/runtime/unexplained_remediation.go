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

func (c *Controller) unexplainedRemovalsLocked(incidentID, controlNamespace string) ([]UnexplainedRemoval, error) {
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		return nil, fmt.Errorf("incident is no longer current")
	}
	if c.drift == nil || !c.drift.Inventory {
		return nil, nil
	}
	_, findings := c.verifySourceInventoryLocked(nil)
	source, ok := c.drift.Events.(interface {
		SourceDriftObjects() []*unstructured.Unstructured
	})
	if !ok || c.sourceInventory == nil {
		return nil, fmt.Errorf("accepted source inventory is unavailable")
	}
	objects := source.SourceDriftObjects()
	var removals []UnexplainedRemoval
	for _, finding := range findings {
		if finding.DetectorID != "drift.unexplained" || inventoryEvent(finding).Source != "absent from accepted source" {
			continue
		}
		for _, object := range objects {
			if object == nil || inventoryRef(object) != finding.PrimaryResource || inventoryOwned(object) || object.GetNamespace() == controlNamespace || object.GetKind() == "Secret" {
				continue
			}
			if object.GetUID() == "" || object.GetResourceVersion() == "" {
				continue
			}
			removals = append(removals, UnexplainedRemoval{FindingFingerprint: FindingFingerprint(finding), Ref: finding.PrimaryResource, UID: object.GetUID(), ResourceVersion: object.GetResourceVersion()})
		}
	}
	return removals, nil
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
			return client.Delete(ctx, ref, uid, rv)
		}
	}
	return fmt.Errorf("object is no longer the selected unexplained finding at %s", time.Now().UTC().Format(time.RFC3339Nano))
}
