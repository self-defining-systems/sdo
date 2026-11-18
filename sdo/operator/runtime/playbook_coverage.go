package runtime

import (
	"sort"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func cloneCoverage(coverage *contracts.PlaybookCoverage) *contracts.PlaybookCoverage {
	if coverage == nil {
		return nil
	}
	return proto.Clone(coverage).(*contracts.PlaybookCoverage)
}

// buildPlaybookCoverage joins every successful diagnosis to incident findings.
// Counts describe past evidence; they never filter a match or authorize a write.
// The registry supplying runs has already excluded retired playbooks.
func buildPlaybookCoverage(findings []sdk.Finding, runs []ProgramRun, view sdk.DetectionContext, stats map[string]ProgramTrust) *contracts.PlaybookCoverage {
	result := &contracts.PlaybookCoverage{}
	matches := map[string]*contracts.PlaybookMatch{}
	explained := map[string]bool{}
	for _, run := range runs {
		if run.Verdict.Result != program.Match || programRunContract(run).GetError() != "" {
			continue
		}
		key := run.ProgramID + "\x00" + run.ProgramRevision + "\x00" + bindingKey(run.Bindings)
		match := matches[key]
		if match == nil {
			canMitigate := run.CanMitigate != nil && *run.CanMitigate
			match = &contracts.PlaybookMatch{PlaybookId: run.ProgramID, ProgramRevision: run.ProgramRevision, Bindings: programBindingsContract(run.Bindings), CanMitigate: canMitigate, Retrieval: run.Retrieval}
			if canMitigate {
				match.MitigateCommand = mitigateCommand(run.ProgramID, run.Bindings)
			}
			state := stats[run.ProgramID]
			match.VerifiedSuccesses = int32(state.VerifiedSuccesses)
			match.VerifiedFailures = int32(state.Failures)
			matches[key] = match
		}
		covered := map[string]bool{}
		for _, fp := range match.ExplainedFindingFingerprints {
			covered[fp] = true
		}
		for _, finding := range findings {
			if finding.Status != sdk.FindingActive {
				continue
			}
			fp := FindingFingerprint(finding)
			// The run's fingerprint is the finding that bound it; its Match is
			// the one explanation rule, shared with the automatic stage.
			if fp == run.FindingFingerprint && coverageFindingRelated(view, finding, run.Bindings) {
				covered[fp] = true
				explained[fp] = true
			}
		}
		match.ExplainedFindingFingerprints = nil
		for fp := range covered {
			match.ExplainedFindingFingerprints = append(match.ExplainedFindingFingerprints, fp)
		}
		sort.Strings(match.ExplainedFindingFingerprints)
	}
	keys := make([]string, 0, len(matches))
	for key := range matches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Matches = append(result.Matches, matches[key])
	}
	// A playbook SDO tried whose diagnosis was Unknown (or needed judgement) is listed with why: the agent can act
	// on it at once, and an always-Unknown playbook is never silent.
	listed := map[string]bool{}
	for _, run := range runs {
		if run.ProgramID == SourceRestoreID || run.Verdict.Result != program.Unknown && run.Verdict.Result != program.NeedsJudgement {
			continue
		}
		key := run.ProgramID + "\x00" + run.ProgramRevision
		if listed[key] {
			continue
		}
		listed[key] = true
		verdict := playbookVerdictContract(run)
		verdict.UnknownOnRecurrence = int32(stats[run.ProgramID].UnknownOnRecurrence)
		result.Unresolved = append(result.Unresolved, verdict)
	}
	sort.SliceStable(result.Unresolved, func(i, j int) bool { return result.Unresolved[i].PlaybookId < result.Unresolved[j].PlaybookId })
	unexplained := map[string]bool{}
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			fp := FindingFingerprint(finding)
			if !explained[fp] {
				unexplained[fp] = true
			}
		}
	}
	for fp := range unexplained {
		result.UnexplainedFindingFingerprints = append(result.UnexplainedFindingFingerprints, fp)
	}
	sort.Strings(result.UnexplainedFindingFingerprints)
	return result
}

func coverageFindingRelated(view sdk.DetectionContext, finding sdk.Finding, bindings program.Bindings) bool {
	refs := append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...)
	for _, ref := range finding.ParameterBindings {
		refs = append(refs, ref)
	}
	for _, subject := range refs {
		for _, bound := range bindings.Objects {
			if sameResource(subject, bound) || view != nil && coverageDirectlyRelated(view, subject, bound) {
				return true
			}
		}
	}
	return false
}

// diagnosisRank orders verdicts by how much they say: a Match over a judgement call over an Unknown over a NoMatch.
var diagnosisRank = map[string]int{"match": 0, "needs_judgement": 1, "unknown": 2, "no_match": 3}

// stageDiagnoses is every playbook a stage diagnosed, once per playbook and revision (its best verdict), ordered by
// ID: what the stage tried, matched or not. The built-in source restore is not a learned playbook and is left out.
func stageDiagnoses(runs []ProgramRun) []*contracts.PlaybookVerdict {
	best := map[string]*contracts.PlaybookVerdict{}
	for _, run := range runs {
		if run.ProgramID == SourceRestoreID {
			continue
		}
		verdict := playbookVerdictContract(run)
		key := verdict.PlaybookId + "\x00" + verdict.ProgramRevision
		if held := best[key]; held == nil || diagnosisRank[verdict.Result] < diagnosisRank[held.Result] {
			best[key] = verdict
		}
	}
	return sortedDiagnoses(best)
}

// mergeStageDiagnoses folds a later pass's diagnoses into the report's: the better verdict per playbook and revision.
func mergeStageDiagnoses(held, fresh []*contracts.PlaybookVerdict) []*contracts.PlaybookVerdict {
	best := map[string]*contracts.PlaybookVerdict{}
	for _, verdict := range append(append([]*contracts.PlaybookVerdict(nil), held...), fresh...) {
		key := verdict.PlaybookId + "\x00" + verdict.ProgramRevision
		if previous := best[key]; previous == nil || diagnosisRank[verdict.Result] <= diagnosisRank[previous.Result] {
			best[key] = verdict
		}
	}
	return sortedDiagnoses(best)
}

func sortedDiagnoses(best map[string]*contracts.PlaybookVerdict) []*contracts.PlaybookVerdict {
	keys := make([]string, 0, len(best))
	for key := range best {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*contracts.PlaybookVerdict, 0, len(keys))
	for _, key := range keys {
		result = append(result, best[key])
	}
	return result
}

// diagnosisLines states, as facts for the agent's page, what the stage tried: each match with how it was retrieved
// and each Unknown with why. A plain NoMatch is the common case and stays off the page.
func diagnosisLines(stages []*contracts.StageReport) []string {
	var lines []string
	for _, stage := range stages {
		for _, verdict := range stage.GetDiagnoses() {
			switch verdict.GetResult() {
			case "match":
				detail := verdict.GetRetrieval()
				if detail == "" {
					detail = "retrieval unknown"
				}
				lines = append(lines, "tried by sdo: "+verdict.GetPlaybookId()+" -> match ("+detail+")")
			case "unknown", "needs_judgement":
				lines = append(lines, "tried by sdo: "+verdict.GetPlaybookId()+" -> "+verdict.GetResult()+" ("+orQuestion(redactDetectText(verdict.GetReason(), 160))+")")
			}
		}
	}
	return lines
}
