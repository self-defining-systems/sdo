package runtime

import (
	"context"
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type FastPathRepair struct {
	ProgramID       string           `json:"program_id"`
	ProgramRevision string           `json:"program_revision"`
	Bindings        program.Bindings `json:"bindings"`
	CoveredFindings []sdk.Finding    `json:"covered_findings"`
	VerifiedAt      time.Time        `json:"verified_at"`
}

// The pre-apply observation scope is restart evidence, never a mutation journal.
type fastPathVerificationScope struct {
	Baseline []sdk.Finding `json:"baseline"`
	Covered  []sdk.Finding `json:"covered"`
}

func fastPathScope(findings []sdk.Finding, selected, runs []ProgramRun, related []RelationCoveredFinding) fastPathVerificationScope {
	covered := map[string]bool{}
	for _, run := range runs {
		for _, chosen := range selected {
			if run.ProgramID == chosen.ProgramID && run.Bindings.Equal(chosen.Bindings) && run.Verdict.Result == program.Match {
				covered[run.FindingFingerprint] = true
			}
		}
	}
	for _, relation := range related {
		covered[relation.FindingFingerprint] = true
	}
	scope := fastPathVerificationScope{Baseline: append([]sdk.Finding(nil), findings...)}
	for _, finding := range findings {
		if covered[FindingFingerprint(finding)] {
			scope.Covered = append(scope.Covered, finding)
		}
	}
	return scope
}

type fastPathVerifier struct {
	loop     *ResponderLoop
	selected []selectedPlan
	scope    fastPathVerificationScope
	response *ToolResponse
}

func (v fastPathVerifier) Verify(ctx context.Context) (bool, error) {
	v.loop.verify(ctx, v.selected, v.response)
	return fastPathCauseVerified(v.selected, v.scope, *v.response), nil
}

func fastPathIntroducedFinding(selected []selectedPlan, scope fastPathVerificationScope, response ToolResponse) bool {
	before := map[string]bool{}
	for _, finding := range scope.Baseline {
		before[FindingFingerprint(finding)] = true
	}
	for _, finding := range response.Findings {
		if !before[FindingFingerprint(finding)] && fastPathFindingAffected(selected, scope, finding) {
			return true
		}
	}
	return false
}

// A concurrent peer fault is not evidence against this application. Attribute
// new findings only through named writes, bindings or already covered resources.
func fastPathFindingAffected(selected []selectedPlan, scope fastPathVerificationScope, finding sdk.Finding) bool {
	refs := []sdk.ObjectRef{finding.PrimaryResource}
	refs = append(refs, finding.RelatedResources...)
	for _, ref := range finding.ParameterBindings {
		refs = append(refs, ref)
	}
	affected := make([]sdk.ObjectRef, 0)
	for _, plan := range selected {
		for _, ref := range plan.bindings.Objects {
			affected = append(affected, ref)
		}
		for _, action := range plan.plan.Actions {
			if ref, err := planActionRef(action); err == nil {
				affected = append(affected, ref)
			}
		}
	}
	for _, covered := range scope.Covered {
		affected = append(affected, covered.PrimaryResource)
	}
	for _, ref := range refs {
		for _, target := range affected {
			if ref.Kind != "" && ref.Name != "" && sameResource(ref, target) {
				return true
			}
		}
	}
	return false
}

func fastPathCauseFailed(selected []selectedPlan, scope fastPathVerificationScope, response ToolResponse) bool {
	if fastPathIntroducedFinding(selected, scope, response) {
		return true
	}
	for _, plan := range selected {
		for _, run := range response.ProgramRuns {
			if run.ProgramID == plan.programID && run.Bindings.Equal(plan.bindings) && run.Verdict.Result == program.Match {
				return true
			}
		}
	}
	return false
}

func fastPathCauseVerified(selected []selectedPlan, scope fastPathVerificationScope, response ToolResponse) bool {
	if len(scope.Covered) == 0 || response.Error != "" || !boundPlansNoMatch(selected, response.ProgramRuns) || fastPathIntroducedFinding(selected, scope, response) {
		return false
	}
	observed := map[string]bool{}
	for _, state := range response.DetectorStates {
		observed[state.DetectorID] = state.Status == DetectorEvaluationClear || state.Status == DetectorEvaluationFiring
	}
	active := map[string]bool{}
	for _, finding := range response.Findings {
		active[FindingFingerprint(finding)] = true
	}
	for _, finding := range scope.Covered {
		if !observed[finding.DetectorID] || active[FindingFingerprint(finding)] {
			return false
		}
	}
	return true
}

func (l *ResponderLoop) rememberFastPathRepairs(runs []ProgramRun, scope fastPathVerificationScope) {
	l.fastPathRepairs = nil
	for _, run := range runs {
		l.fastPathRepairs = append(l.fastPathRepairs, FastPathRepair{ProgramID: run.ProgramID, ProgramRevision: run.ProgramRevision, Bindings: cloneProgramBindings(run.Bindings), CoveredFindings: append([]sdk.Finding(nil), scope.Covered...), VerifiedAt: time.Now().UTC()})
	}
}

func fastPathRepairsContract(repairs []FastPathRepair) ([]*contracts.FastPathRepair, error) {
	result := make([]*contracts.FastPathRepair, 0, len(repairs))
	for _, repair := range repairs {
		item := &contracts.FastPathRepair{ProgramId: repair.ProgramID, ProgramRevision: repair.ProgramRevision, Bindings: programRunContract(ProgramRun{Bindings: repair.Bindings}).Bindings, VerifiedAt: optionalTimestamp(repair.VerifiedAt)}
		for _, finding := range repair.CoveredFindings {
			converted, err := findingContract(finding)
			if err != nil {
				return nil, err
			}
			item.CoveredFindings = append(item.CoveredFindings, converted)
		}
		result = append(result, item)
	}
	return result, nil
}

func fastPathRepairsState(repairs []*contracts.FastPathRepair) []FastPathRepair {
	result := make([]FastPathRepair, 0, len(repairs))
	for _, repair := range repairs {
		result = append(result, FastPathRepair{ProgramID: repair.GetProgramId(), ProgramRevision: repair.GetProgramRevision(), Bindings: programBindingsState(repair.GetBindings()), CoveredFindings: findingsState(repair.GetCoveredFindings()), VerifiedAt: controllerTime(repair.GetVerifiedAt())})
	}
	return result
}

// Keep complete historical diagnoses in outcomes, but show the responder only
// each binding's latest observation so a repaired Match cannot invite replay.
func fastPathHandoffRuns(runs []ProgramRun) []*contracts.ProgramRun {
	result := make([]*contracts.ProgramRun, 0)
	seen := map[string]bool{}
	for index := len(runs) - 1; index >= 0; index-- {
		run := runs[index]
		key := matchedViewKey(run.ProgramID, run.Bindings)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, programRunContract(run))
	}
	slices.Reverse(result)
	return result
}

func includeFastPathRepairs(result IncidentResult, repairs []FastPathRepair) IncidentResult {
	for _, repair := range repairs {
		result.AppliedPlaybooks = append(result.AppliedPlaybooks, AppliedPlaybook{PlaybookID: repair.ProgramID, ParameterBindings: repair.Bindings.Objects})
		for _, finding := range repair.CoveredFindings {
			result.ConfirmedRootCauses = append(result.ConfirmedRootCauses, ConfirmedRootCause{Summary: finding.Summary, Resources: []sdk.ObjectRef{finding.PrimaryResource}, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}}, ExplainedDetectors: []string{finding.DetectorID}})
		}
		result.VerificationEvidence = append(result.VerificationEvidence, VerificationEvidence{Name: "fast-path covered findings", Passed: true, Details: "kept repair by " + repair.ProgramID, ObservedAt: repair.VerifiedAt})
	}
	return result
}
