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
			match = &contracts.PlaybookMatch{PlaybookId: run.ProgramID, ProgramRevision: run.ProgramRevision, Bindings: programBindingsContract(run.Bindings), CanMitigate: canMitigate}
			if canMitigate {
				match.MitigationSelection = run.ProgramID
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
			if fp == run.FindingFingerprint || coverageFindingRelated(view, finding, run.Bindings) {
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
