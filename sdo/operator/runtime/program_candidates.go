package runtime

import (
	"context"
	"encoding/json"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const DefaultDiagnosisDeadline = 10 * time.Second

// GenericDiagnosisWorkers bounds concurrent diagnoses of candidates no
// detector routed (findings that list no playbook).
const GenericDiagnosisWorkers = 8

// FindingNamesPlaybook reports whether the finding's detector routed to the
// playbook by listing it. Only such candidates are detector-routed; the
// automatic stage applies matches from no others.
func FindingNamesPlaybook(finding sdk.Finding, id string) bool {
	for _, reference := range finding.Playbooks {
		if got, err := layout.PlaybookID(reference); err == nil && got == id {
			return true
		}
	}
	return false
}

// ProgramRun is one diagnosis for a concrete finding or hint binding.
type ProgramRun struct {
	CanMitigate        *bool            `json:"can_mitigate,omitempty"`
	FindingFingerprint string           `json:"finding_fingerprint,omitempty"`
	BindingSource      string           `json:"binding_source,omitempty"`
	ProgramID          string           `json:"program_id"`
	Bindings           program.Bindings `json:"bindings"`
	Verdict            program.Verdict  `json:"verdict"`
	Duration           time.Duration    `json:"duration"`
	ProgramRevision    string           `json:"program_revision,omitempty"`
	// MitigateCommand is the command that applies this playbook with these
	// bindings; set on a matching diagnosis that has a fix.
	MitigateCommand string `json:"mitigate_command,omitempty"`
}

// SelectProgramCandidates diagnoses finding-bound programs under one deadline.
// A "hint" BindingSource marks a detector-routed run: the finding lists the
// playbook.
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
		fingerprint string
		source      string
		id          string
		bindings    program.Bindings
		routed      bool
	}
	var candidates []candidate
	seen := map[string]bool{}
	aliases := map[string][]candidate{}
	add := func(id string, bindings program.Bindings, fingerprint, source string, routed bool) {
		key := id + "\x00" + bindingKey(bindings)
		if seen[key] {
			// One detector-routed finding makes the shared diagnosis routed.
			if routed {
				for index := range candidates {
					if candidates[index].id == id && sameBindings(candidates[index].bindings, bindings) {
						candidates[index].routed = true
					}
				}
			}
			if fingerprint != "" {
				aliases[key] = append(aliases[key], candidate{fingerprint: fingerprint, source: source, id: id, bindings: bindings, routed: routed})
			}
			return
		}
		seen[key] = true
		candidates = append(candidates, candidate{fingerprint: fingerprint, source: source, id: id, bindings: bindings, routed: routed})
	}
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		for _, spec := range registry.List() {
			bindings, source, ok := findingProgramBindings(spec, finding)
			if ok {
				add(spec.ID, bindings, FindingFingerprint(finding), source, FindingNamesPlaybook(finding, spec.ID))
			}
		}
	}
	appendAssociations := func(runs []ProgramRun, run ProgramRun) []ProgramRun {
		_, canMitigate := registry.Mitigator(run.ProgramID)
		run.CanMitigate = &canMitigate
		runs = append(runs, run)
		for _, alias := range aliases[run.ProgramID+"\x00"+bindingKey(run.Bindings)] {
			associated := run
			associated.FindingFingerprint = alias.fingerprint
			associated.BindingSource = alias.source
			runs = append(runs, associated)
		}
		return runs
	}
	// Detector-routed candidates (the finding lists the playbook) run first
	// under their own budget, so generic fan-out cannot starve them. Generic
	// candidates then run with bounded concurrency under what remains.
	var routed, generic []candidate
	for _, item := range candidates {
		if item.routed {
			routed = append(routed, item)
		} else {
			generic = append(generic, item)
		}
	}
	runTier := func(ctx context.Context, tier []candidate, workers int) []ProgramRun {
		if len(tier) == 0 {
			return nil
		}
		type result struct {
			index int
			run   ProgramRun
		}
		queue := make(chan int, len(tier))
		for index := range tier {
			queue <- index
		}
		close(queue)
		results := make(chan result, len(tier))
		if workers > len(tier) {
			workers = len(tier)
		}
		for range workers {
			go func() {
				for index := range queue {
					if ctx.Err() != nil {
						return
					}
					candidate := tier[index]
					started := lifecycleio.Now()
					diagnoser, ok := registry.Diagnoser(candidate.id)
					if !ok {
						continue
					}
					verdict := program.RunDiagnose(ctx, diagnoser, view, candidate.bindings, deadline)
					results <- result{index, ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: verdict, Duration: lifecycleio.Since(started)}}
				}
			}()
		}
		var runs []ProgramRun
		finished := make([]bool, len(tier))
		for count := 0; count < len(tier); {
			select {
			case got := <-results:
				finished[got.index] = true
				count++
				runs = appendAssociations(runs, got.run)
			case <-ctx.Done():
				for index, candidate := range tier {
					if !finished[index] {
						runs = appendAssociations(runs, ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "diagnosis", Observed: ctx.Err().Error(), Expected: "completed before deadline"}}}, Duration: deadline})
					}
				}
				return runs
			}
		}
		return runs
	}
	routedCtx, cancelRouted := context.WithTimeout(ctx, deadline/2)
	runs := runTier(routedCtx, routed, len(routed))
	cancelRouted()
	runs = append(runs, runTier(ctx, generic, GenericDiagnosisWorkers)...)
	sortProgramRuns(runs)
	return runs
}

// findingProgramBindings is the shared candidate/target boundary. Optional
// object roles still need a finding resource; scalar defaults and an empty map
// do not make an unrelated finding a diagnosis candidate. Explicit authored
// hints remain visible even when incomplete, but carry only declared roles.
func findingProgramBindings(spec program.ProgramSpec, finding sdk.Finding) (program.Bindings, string, bool) {
	bound, err := program.BindFinding(spec, finding)
	hasObjectRoles := false
	for _, param := range spec.Params {
		hasObjectRoles = hasObjectRoles || param.Kind != ""
	}
	if err == nil && (!hasObjectRoles || len(bound.Objects) > 0) {
		source := "finding"
		if FindingNamesPlaybook(finding, spec.ID) {
			source = "hint"
		}
		for name, ref := range bound.Objects {
			if finding.ParameterBindings[name] == ref && ref.Kind != "" {
				source = "hint"
			}
		}
		return bound, source, true
	}
	if FindingNamesPlaybook(finding, spec.ID) {
		return cloneProgramBindings(bound), "hint", true
	}
	return program.Bindings{}, "", false
}

func cloneProgramBindings(source program.Bindings) program.Bindings {
	result := source.Clone()
	if result.Objects == nil {
		result.Objects = program.ObjectBindings{}
	}
	return result
}
func sameBindings(left, right program.Bindings) bool { return left.Equal(right) }

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
	keys := make([]string, 0, len(bindings.Objects))
	for key := range bindings.Objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		ref := bindings.Objects[key]
		parts = append(parts, key+"="+ref.Kind+"/"+ref.Namespace+"/"+ref.Name)
	}
	key := strings.Join(parts, ",")
	if len(bindings.Values) > 0 {
		raw, _ := json.Marshal(bindings.Values)
		key += "|values=" + string(raw)
	}
	return key
}

// matchedFindingFingerprints joins a successful live diagnosis to the active
// findings that supplied its bindings. It never reconstructs bindings from
// authored explanation links, recorded corpus labels, or proposed writes.
func matchedFindingFingerprints(findings []sdk.Finding, runs []ProgramRun, id string, bindings program.Bindings) []string {
	matched := map[string]bool{}
	for _, run := range runs {
		if run.ProgramID == id && run.Verdict.Result == program.Match && run.Bindings.Equal(bindings) {
			matched[run.FindingFingerprint] = true
		}
	}
	var fingerprints []string
	seen := map[string]bool{}
	for _, finding := range findings {
		fp := FindingFingerprint(finding)
		if finding.Status == sdk.FindingActive && matched[fp] && !seen[fp] {
			fingerprints = append(fingerprints, fp)
			seen[fp] = true
		}
	}
	sort.Strings(fingerprints)
	return fingerprints
}
