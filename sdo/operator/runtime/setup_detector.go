package runtime

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"unicode/utf8"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// Only health-class detectors and user tests decide whether an install or a
// start-up is healthy and accepted. An incident-class detector, designer-authored
// or learned alike, is a clue: when it errors or fires while the app is judged
// healthy it stays enabled and is recorded as noisy evidence, nothing else.

// maxSetupNoiseBytes bounds the detail a noisy-detector record keeps (the
// contract's max_len).
const maxSetupNoiseBytes = 500

const (
	setupNoiseErrored = "errored"
	setupNoiseFired   = "fired"
)

// gatesSetup reports whether spec's result may decide an install: only a
// health-class detector does.
func gatesSetup(spec sdk.DetectorSpec) bool { return spec.Class == sdk.DetectorClassHealth }

// recordSetupNoise notes an incident-class detector that errored or fired on the
// healthy app at setup. It logs, and records the evidence on the install being
// completed. It changes nothing else.
func (c *Controller) recordSetupNoise(ctx context.Context, spec sdk.DetectorSpec, outcome, detail string, candidate *contracts.NamespaceOnboarding) {
	detail = clipSetupDetail(detail)
	slog.WarnContext(ctx, "incident detector is noisy on the healthy app at setup", "detector", spec.ID, "outcome", outcome, "detail", detail)
	if candidate == nil {
		return
	}
	for _, record := range candidate.SetupNoisy {
		if record.DetectorId == spec.ID && record.AcceptedRevision == c.config.SourceCommit && record.Outcome == outcome {
			return
		}
	}
	candidate.SetupNoisy = append(candidate.SetupNoisy, &contracts.SetupNoisyDetector{DetectorId: spec.ID, AcceptedRevision: c.config.SourceCommit, Outcome: outcome, Detail: detail})
}

// clipSetupDetail keeps one line of at most maxSetupNoiseBytes, cut on a rune
// boundary.
func clipSetupDetail(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) <= maxSetupNoiseBytes {
		return text
	}
	cut := maxSetupNoiseBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// firedRules names the active findings' rules, sorted, for a noisy record.
func firedRules(findings []sdk.Finding) string {
	rules := map[string]bool{}
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			rules[finding.RuleID] = true
		}
	}
	names := make([]string, 0, len(rules))
	for name := range rules {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

type setupDetection struct {
	findings []sdk.Finding
	err      error
}

// Legacy state without a setup marker cannot prove a prior incident belongs
// to another revision, so it conservatively retains its incident protection.
func (c *Controller) setupIncidentFree(pendingInstall bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.incidentOpen || c.closureBlocksNextLocked() || c.currentIncidentRequest != nil {
		return false
	}
	if pendingInstall || c.setupEvaluatedRevision == "" {
		return c.incidentDetectedAt.IsZero() && c.lastAcknowledgedIncidentID == "" && len(c.failedClosures) == 0
	}
	return true
}

// Establish health on this same snapshot before interpreting learned findings.
// Caching preserves one execution per detector regardless of registry order.
func (c *Controller) setupHealth(ctx context.Context, snapshot sdk.DetectionContext, detectors []sdk.Detector, candidate *contracts.NamespaceOnboarding) (map[string]setupDetection, bool) {
	if candidate == nil && c.namespaceOnboarding == nil || c.setupEvaluatedRevision == c.config.SourceCommit || len(c.healthDetectorIDs) == 0 || len(detectors) != len(c.detectorList()) {
		return nil, false
	}
	if !c.setupIncidentFree(candidate != nil) || len(c.sourceInventoryFindings) > 0 {
		return nil, false
	}
	results := evaluateDetectors(ctx, detectors, snapshot)
	healthy := true
	for _, id := range c.healthDetectorIDs {
		result, found := results[id]
		if !found || result.err != nil || len(result.findings) != 0 || !detectorConfirmed(c.detectors[id], snapshot) {
			healthy = false
		}
	}
	return results, healthy
}
