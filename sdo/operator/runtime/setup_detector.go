package runtime

import (
	"context"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

const setupDetectorReason = "fired on the confirmed-healthy app at setup"

func learnedIncidentDetector(spec sdk.DetectorSpec) bool {
	return spec.Class == sdk.DetectorClassIncident && spec.Owner == sdk.DetectorOwnerResponder && spec.OriginatingIncident != ""
}

func cloneDisabledSetupDetectors(records []*contracts.DisabledSetupDetector) []*contracts.DisabledSetupDetector {
	result := make([]*contracts.DisabledSetupDetector, 0, len(records))
	for _, record := range records {
		result = append(result, proto.Clone(record).(*contracts.DisabledSetupDetector))
	}
	return result
}

func setupDetectorExcluded(spec sdk.DetectorSpec, revision string, records []*contracts.DisabledSetupDetector) bool {
	if !learnedIncidentDetector(spec) {
		return false
	}
	for _, record := range records {
		if record.DetectorId == spec.ID && record.AcceptedRevision == revision {
			return true
		}
	}
	return false
}

func withoutDisabledSetupDetectors(detectors []sdk.Detector, revision string, records []*contracts.DisabledSetupDetector) []sdk.Detector {
	result := make([]sdk.Detector, 0, len(detectors))
	for _, detector := range detectors {
		if !setupDetectorExcluded(detector.Spec(), revision, records) {
			result = append(result, detector)
		}
	}
	return result
}

func (c *Controller) setupDetectorDisabled(id string) bool {
	return setupDetectorExcluded(c.detectorSpecs[id], c.config.SourceCommit, c.disabledSetupDetectors)
}

func (c *Controller) enabledSetupDetectors(detectors []sdk.Detector) []sdk.Detector {
	return withoutDisabledSetupDetectors(detectors, c.config.SourceCommit, c.disabledSetupDetectors)
}

type setupDetection struct {
	findings []sdk.Finding
	err      error
}

// Establish health on this same snapshot before interpreting learned findings.
// Caching preserves one execution per detector regardless of registry order.
func (c *Controller) setupHealth(ctx context.Context, snapshot sdk.DetectionContext, detectors []sdk.Detector, candidate *contracts.NamespaceOnboarding) (map[string]setupDetection, bool) {
	if candidate == nil && c.namespaceOnboarding == nil || c.setupEvaluatedRevision == c.config.SourceCommit || len(c.healthDetectorIDs) == 0 || len(detectors) != len(c.detectorList()) {
		return nil, false
	}
	c.mu.Lock()
	busy := c.incidentOpen || c.closureBlocksNextLocked() || c.currentIncidentRequest != nil
	if candidate != nil || c.setupEvaluatedRevision == "" {
		busy = busy || !c.incidentDetectedAt.IsZero() || c.lastAcknowledgedIncidentID != "" || len(c.failedClosures) != 0
	}
	c.mu.Unlock()
	if busy || len(c.sourceInventoryFindings) > 0 {
		return nil, false
	}
	results := map[string]setupDetection{}
	healthy := true
	for _, detector := range detectors {
		findings, err := sdk.DetectObserved(ctx, detector, snapshot)
		results[detector.Spec().ID] = setupDetection{findings, err}
	}
	for _, id := range c.healthDetectorIDs {
		result, found := results[id]
		if !found || result.err != nil || len(result.findings) != 0 || !detectorConfirmed(c.detectors[id], snapshot) {
			healthy = false
		}
	}
	return results, healthy
}

func (c *Controller) disableSetupDetector(spec sdk.DetectorSpec, findings []sdk.Finding, now time.Time, candidate *contracts.NamespaceOnboarding) bool {
	fingerprints := make([]string, 0, len(findings))
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			fingerprints = append(fingerprints, finding.Fingerprint)
		}
	}
	if len(fingerprints) == 0 {
		return false
	}
	sort.Strings(fingerprints)
	record := &contracts.DisabledSetupDetector{DetectorId: spec.ID, AcceptedRevision: c.config.SourceCommit, Fingerprints: fingerprints, DisabledAt: timestamppb.New(now.UTC()), Reason: setupDetectorReason}
	c.mu.Lock()
	// Retain the audit trail across revision changes; only an exact revision disables.
	c.disabledSetupDetectors = append(c.disabledSetupDetectors, record)
	c.mu.Unlock()
	if candidate != nil {
		candidate.DisabledSetupDetectors = cloneDisabledSetupDetectors(c.disabledSetupDetectors)
	}
	return true
}
