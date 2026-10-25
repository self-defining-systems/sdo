package runtime

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/outcome_derivation"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

func (c *Controller) prepareSourceInventory(ctx context.Context) error {
	if c.drift == nil || !c.drift.Inventory {
		return nil
	}
	revision := c.CurrentAcceptedCommit()
	c.mu.Lock()
	current := c.sourceInventory
	retryAt := c.sourceInventoryRetryAt
	c.mu.Unlock()
	if current != nil && current.Revision == revision {
		return nil
	}
	if c.drift.Source == nil {
		return fmt.Errorf("accepted source inventory is unavailable")
	}
	if time.Now().Before(retryAt) {
		return nil
	}
	inventory, err := acceptedSourceInventory(ctx, c.drift.Source, revision, c.config.Namespace)
	if err != nil {
		c.mu.Lock()
		c.sourceInventory = nil
		c.sourceInventoryError = err.Error()
		c.sourceInventoryRetryAt = time.Now().Add(2 * time.Second)
		c.mu.Unlock()
		if c.OnError != nil {
			c.OnError(fmt.Errorf("load accepted source inventory; closure remains unverified: %w", err))
		}
		return nil
	}
	index := NewSourceInventory(c.config.Namespace, revision, inventory.Objects)
	index.EvidenceGaps = append(index.EvidenceGaps, inventory.EvidenceGaps...)
	c.mu.Lock()
	c.sourceInventory = index
	c.sourceInventoryError = ""
	c.sourceInventoryRetryAt = time.Time{}
	if c.acceptedSourceDriftRevision != index.Identity {
		c.acceptedSourceDrift = nil
		c.acceptedSourceDriftRevision = index.Identity
	}
	c.mu.Unlock()
	if c.OnError != nil {
		for _, gap := range inventory.EvidenceGaps {
			c.OnError(fmt.Errorf("source inventory remains available with field evidence gap: %w", gap))
		}
	}
	return nil
}

// In-memory source implementations may already provide complete objects. The
// repository edge distinguishes identity evidence from rendered-field evidence.
func acceptedSourceInventory(ctx context.Context, source interface {
	AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error)
}, revision, namespace string) (rendered.Inventory, error) {
	if source, ok := source.(interface {
		AcceptedInventory(context.Context, string, string) (rendered.Inventory, error)
	}); ok {
		return source.AcceptedInventory(ctx, revision, namespace)
	}
	objects, err := source.AcceptedManifests(ctx, revision, namespace)
	return rendered.Inventory{Objects: objects}, err
}
func inventoryFinding(finding sdk.Finding) bool {
	value, _ := finding.Metadata["source_inventory"].(bool)
	return value
}
func inventoryEvent(finding sdk.Finding) sdk.DriftEvent {
	events := driftEventsForFindings(nil, []sdk.Finding{finding})
	if len(events) == 1 {
		return events[0]
	}
	return sdk.DriftEvent{}
}

// checkedInventoryAcceptance uses the outcome edge's existing exact-observation
// binding. Benign assessments need a nonempty reason and concrete evidence;
// labels cannot accept a changed object or invent controller observations.
func checkedInventoryAcceptance(finding sdk.Finding, labels []sdk.DriftEvent) bool {
	event := inventoryEvent(finding)
	checked := outcome_derivation.CheckedDriftAssessments(driftEventsContract([]sdk.DriftEvent{event}), driftEventsContract(labels))
	// A closure carries the result assessment while the same checked label
	// already lives in durable state. Rechecking that identical decision is
	// idempotent; a later valid assessment overrides an earlier one.
	return len(checked) > 0 && checked[len(checked)-1].GetInvestigationVerdict() == "benign"
}

func (c *Controller) observeSourceInventory(now time.Time) {
	if c.drift == nil || !c.drift.Inventory {
		return
	}
	c.mu.Lock()
	index := c.sourceInventory
	accepted := append([]sdk.DriftEvent(nil), c.acceptedSourceDrift...)
	c.mu.Unlock()
	if index == nil {
		return
	}
	if readiness, ok := c.drift.Events.(interface{ SourceDriftReady() bool }); ok && !readiness.SourceDriftReady() {
		return
	}
	reader, ok := c.drift.Events.(interface {
		SourceDriftObjects() []*unstructured.Unstructured
	})
	if !ok {
		return
	}
	findings := index.Findings(reader.SourceDriftObjects(), now)
	c.mu.Lock()
	c.sourceInventoryFindings = findings
	c.mu.Unlock()
	c.tracker.SetPolicy("drift.unexplained", 1, 1, 0)
	active := map[string]bool{}
	for _, finding := range findings {
		if checkedInventoryAcceptance(finding, accepted) {
			continue
		}
		key := FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
		active[key] = true
		prior := c.tracker.Snapshot()[key]
		if prior.Active && sameDriftObservation(inventoryEvent(prior.Finding), inventoryEvent(finding)) {
			continue
		}
		event := inventoryEvent(finding)
		c.mu.Lock()
		c.driftEvents = append(c.driftEvents, event)
		c.mu.Unlock()
		c.observeUnexplainedDrift(now, finding)
		if !c.attachDrift(finding) {
			window := c.drift.BatchWindow
			if window <= 0 {
				window = 20 * time.Second
			}
			c.batcher.AddAtWithDebounce(finding, now, window)
		}
	}
	for key, state := range c.tracker.Snapshot() {
		if !inventoryFinding(state.Finding) || !state.Active || active[key] {
			continue
		}
		changes := c.tracker.ObserveBinding(now, state.Finding, false)
		c.batcher.RemoveKeys(changes.Cleared)
		c.recordFiring(sdk.DetectorSpec{ID: state.DetectorID, Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder}, now, nil, changes)
	}
}

// Caller holds c.mu; neither this check nor acceptance performs external reads.
func (c *Controller) verifySourceInventoryLocked(result *IncidentResult) (bool, []sdk.Finding) {
	if c.drift == nil || !c.drift.Inventory {
		return true, nil
	}
	_, hasReader := c.drift.Events.(interface {
		SourceDriftObjects() []*unstructured.Unstructured
	})
	ready := hasReader
	if source, ok := c.drift.Events.(interface{ SourceDriftReady() bool }); ok {
		ready = source.SourceDriftReady()
	}
	if c.sourceInventory == nil || c.sourceInventory.Revision != c.config.SourceCommit || !ready {
		return false, []sdk.Finding{{DetectorID: "drift.unexplained", RuleID: "source-inventory-unavailable", Status: sdk.FindingActive, Severity: sdk.SeverityLatent, Summary: "accepted source inventory is unavailable", Evidence: bounded(c.sourceInventoryError), PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: c.config.Namespace}}}
	}
	labels := append([]sdk.DriftEvent(nil), c.acceptedSourceDrift...)
	if result != nil {
		labels = append(labels, result.DriftEvents...)
	}
	findings := c.sourceInventoryFindings
	if source, ok := c.drift.Events.(interface {
		SourceDriftObjects() []*unstructured.Unstructured
	}); ok {
		findings = c.sourceInventory.Findings(source.SourceDriftObjects(), time.Now().UTC())
	}
	remaining := []sdk.Finding{}
	for _, finding := range findings {
		if !checkedInventoryAcceptance(finding, labels) {
			remaining = append(remaining, finding)
		}
	}
	return len(remaining) == 0, remaining
}

func (c *Controller) retainSourceInventoryAcceptancesLocked(result *IncidentResult) {
	if result == nil || c.sourceInventory == nil {
		return
	}
	for _, finding := range c.sourceInventoryFindings {
		checked := outcome_derivation.CheckedDriftAssessments(driftEventsContract([]sdk.DriftEvent{inventoryEvent(finding)}), driftEventsContract(result.DriftEvents))
		if len(checked) > 0 && checked[len(checked)-1].GetInvestigationVerdict() == "benign" {
			event := driftEventsState(checked)[len(checked)-1]
			found := false
			for i, old := range c.acceptedSourceDrift {
				if old.Ref == event.Ref {
					c.acceptedSourceDrift[i] = event
					found = true
					break
				}
			}
			if !found {
				c.acceptedSourceDrift = append(c.acceptedSourceDrift, event)
			}
		}
	}
	c.acceptedSourceDriftRevision = c.sourceInventory.Identity
}
