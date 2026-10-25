package runtime

import (
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
	return request
}
