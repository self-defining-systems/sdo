package runtime

import (
	"fmt"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
)

// programExplainsFinding requires an authored finding-to-playbook link and the
// exact diagnosed finding. Resource overlap alone is only a candidate binding.
func programExplainsFinding(finding sdk.Finding, id string) bool {
	for _, reference := range finding.Playbooks {
		linked, err := layout.PlaybookID(reference)
		if err == nil && linked == id {
			return true
		}
	}
	return false
}

// mitigationIncident carries the selected programs' original findings, distinct
// from the plan's writes. Empty (non-nil) means no attributed health findings;
// diagnosis must still clear, and global health remains the closure check's job.
func (l *ResponderLoop) mitigationIncident(selected []selectedPlan) IncidentRequest {
	request := l.Request
	request.Findings = []sdk.Finding{}
	for _, finding := range l.Request.Findings {
		targeted := false
		for _, plan := range selected {
			if len(plan.targetFingerprints) > 0 {
				for _, fp := range plan.targetFingerprints {
					if fp == FindingFingerprint(finding) {
						targeted = true
					}
				}
				continue
			}
			for _, match := range l.Request.PlaybookCoverage.GetMatches() {
				if match.GetPlaybookId() != plan.programID || !programBindingsState(match.GetBindings()).Equal(plan.bindings) {
					continue
				}
				for _, fp := range match.GetExplainedFindingFingerprints() {
					if fp == FindingFingerprint(finding) {
						targeted = true
					}
				}
			}
			if programExplainsFinding(finding, plan.programID) && coverageFindingRelated(l.lastDiagnosisView, finding, plan.bindings) {
				targeted = true
			}
		}
		if targeted {
			request.Findings = append(request.Findings, finding)
		}
	}
	if len(request.Findings) == 0 && len(l.Request.Findings) > 0 {
		request.Findings = nil
	} // Unknown target: require the whole sample to clear.
	return request
}

func (l *ResponderLoop) validateFindingTargets(fingerprints []string) error {
	for _, fp := range fingerprints {
		found := false
		for _, finding := range l.Request.Findings {
			if finding.Status == sdk.FindingActive && FindingFingerprint(finding) == fp {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown active target finding %q", fp)
		}
	}
	return nil
}

// Unknown scope requires a clear sample. A known scope compares the original
// target fingerprints, including the responder's freshest post-apply sample.
func targetedFindingsClear(targets, current []sdk.Finding) bool {
	if targets == nil {
		return len(current) == 0
	}
	for _, finding := range current {
		for _, target := range targets {
			if FindingFingerprint(finding) == FindingFingerprint(target) {
				return false
			}
		}
	}
	return true
}
