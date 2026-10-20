package runtime

import (
	"context"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const DefaultDiagnosisDeadline = 10 * time.Second
const maxCandidateAssignments = 32

// ProgramRun is one diagnosis for a finding and one concrete binding.
type ProgramRun struct {
	FindingFingerprint string           `json:"finding_fingerprint,omitempty"`
	ProgramID          string           `json:"program_id"`
	Bindings           program.Bindings `json:"bindings"`
	Verdict            program.Verdict  `json:"verdict"`
	Duration           time.Duration    `json:"duration"`
	ProgramRevision    string           `json:"program_revision,omitempty"`
}

// SelectProgramCandidates diagnoses all bindable programs under one deadline.
func SelectProgramCandidates(ctx context.Context, registry program.ProgramRegistry, view program.ReadView, findings []sdk.Finding, deadline time.Duration) []ProgramRun {
	if registry == nil {
		return nil
	}
	if deadline <= 0 {
		deadline = DefaultDiagnosisDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	if live, ok := view.(LiveReadView); ok {
		live.Context = ctx
		view = live
	}
	type candidate struct {
		finding  sdk.Finding
		id       string
		bindings program.Bindings
	}
	var candidates []candidate
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		for _, spec := range registry.List() {
			assignments := candidateBindings(spec, finding)
			if len(assignments) == 0 {
				for _, reference := range finding.Playbooks {
					id, err := memory.PlaybookID(reference)
					if err == nil && id == spec.ID {
						assignments = []program.Bindings{cloneProgramBindings(program.Bindings(finding.ParameterBindings))}
						break
					}
				}
			}
			for _, bindings := range assignments {
				candidates = append(candidates, candidate{finding, spec.ID, bindings})
			}
		}
	}
	results := make(chan ProgramRun, len(candidates))
	for _, item := range candidates {
		go func(candidate candidate) {
			started := time.Now()
			diagnoser, ok := registry.Diagnoser(candidate.id)
			if !ok {
				return
			}
			verdict := program.RunDiagnose(ctx, diagnoser, view, candidate.bindings, deadline)
			results <- ProgramRun{FindingFingerprint: FindingFingerprint(candidate.finding), ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: verdict, Duration: time.Since(started)}
		}(item)
	}
	runs := make([]ProgramRun, 0, len(candidates))
	for range candidates {
		select {
		case run := <-results:
			runs = append(runs, run)
		case <-ctx.Done():
			for _, candidate := range candidates {
				found := false
				for _, run := range runs {
					if run.FindingFingerprint == FindingFingerprint(candidate.finding) && run.ProgramID == candidate.id && sameBindings(run.Bindings, candidate.bindings) {
						found = true
						break
					}
				}
				if !found {
					runs = append(runs, ProgramRun{FindingFingerprint: FindingFingerprint(candidate.finding), ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "diagnosis", Observed: ctx.Err().Error(), Expected: "completed before deadline"}}}, Duration: deadline})
				}
			}
			sortProgramRuns(runs)
			return runs
		}
	}
	sortProgramRuns(runs)
	return runs
}

func candidateBindings(spec program.ProgramSpec, finding sdk.Finding) []program.Bindings {
	refs := append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...)
	for _, ref := range finding.ParameterBindings {
		refs = append(refs, ref)
	}
	unique := make([]sdk.ObjectRef, 0, len(refs))
	seen := map[sdk.ObjectRef]bool{}
	for _, ref := range refs {
		if ref.Kind != "" && !seen[ref] {
			seen[ref] = true
			unique = append(unique, ref)
		}
	}
	var result []program.Bindings
	var bind func(int, program.Bindings)
	bind = func(index int, current program.Bindings) {
		if len(result) >= maxCandidateAssignments {
			return
		}
		if index == len(spec.Params) {
			if program.ValidateBindings(spec, current) == nil {
				result = append(result, current)
			}
			return
		}
		param := spec.Params[index]
		if hint, ok := finding.ParameterBindings[param.Name]; ok && hint.Kind == string(param.Kind) {
			if !bindingUses(current, hint) {
				next := cloneProgramBindings(current)
				next[param.Name] = hint
				bind(index+1, next)
			}
			return
		}
		for _, ref := range unique {
			if ref.Kind != string(param.Kind) || bindingUses(current, ref) {
				continue
			}
			next := cloneProgramBindings(current)
			next[param.Name] = ref
			bind(index+1, next)
		}
		if !param.Required {
			bind(index+1, current)
		}
	}
	bind(0, program.Bindings{})
	return result
}

func bindingUses(bindings program.Bindings, ref sdk.ObjectRef) bool {
	for _, existing := range bindings {
		if existing == ref {
			return true
		}
	}
	return false
}

func cloneProgramBindings(source program.Bindings) program.Bindings {
	result := make(program.Bindings, len(source))
	for key, ref := range source {
		result[key] = ref
	}
	return result
}

func sameBindings(left, right program.Bindings) bool {
	if len(left) != len(right) {
		return false
	}
	for key, ref := range left {
		if right[key] != ref {
			return false
		}
	}
	return true
}

func sortProgramRuns(runs []ProgramRun) {
	priority := map[program.Result]int{program.Match: 0, program.NeedsJudgement: 1, program.Unknown: 2, program.NoMatch: 3}
	sort.Slice(runs, func(i, j int) bool {
		left, right := runs[i], runs[j]
		if priority[left.Verdict.Result] != priority[right.Verdict.Result] {
			return priority[left.Verdict.Result] < priority[right.Verdict.Result]
		}
		if left.ProgramID != right.ProgramID {
			return left.ProgramID < right.ProgramID
		}
		if left.FindingFingerprint != right.FindingFingerprint {
			return left.FindingFingerprint < right.FindingFingerprint
		}
		return bindingKey(left.Bindings) < bindingKey(right.Bindings)
	})
}

func bindingKey(bindings program.Bindings) string {
	keys := make([]string, 0, len(bindings))
	for key := range bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		ref := bindings[key]
		parts = append(parts, key+"="+ref.Kind+"/"+ref.Namespace+"/"+ref.Name)
	}
	return strings.Join(parts, ",")
}
