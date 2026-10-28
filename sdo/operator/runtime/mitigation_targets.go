package runtime

import (
	"fmt"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// programLinksFinding says a finding can bind the program: its resources fill
// the program's roles, or its detector lists the playbook as a binding hint.
// It is the candidate rule of SelectProgramCandidates. A finding is explained
// only when that binding's diagnosis matches (matchedFindingFingerprints);
// detector lists are never required, because health and generated detectors
// list only their own playbook and reflection cannot edit them.
func programLinksFinding(spec program.ProgramSpec, finding sdk.Finding) bool {
	if len(candidateBindings(spec, finding)) > 0 {
		return true
	}
	for _, reference := range finding.Playbooks {
		linked, err := layout.PlaybookID(reference)
		if err == nil && linked == spec.ID {
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
			for _, fp := range matchedFindingFingerprints(l.Request.Findings, l.Request.InitialProgramRuns, plan.programID, plan.bindings) {
				if fp == FindingFingerprint(finding) {
					targeted = true
				}
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
			// A plan chosen without a prior diagnosis run (plan, menu) targets
			// the related findings that can bind its program.
			if l.Programs != nil && coverageFindingRelated(l.lastDiagnosisView, finding, plan.bindings) {
				for _, spec := range l.Programs.List() {
					targeted = targeted || (spec.ID == plan.programID && programLinksFinding(spec, finding))
				}
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
