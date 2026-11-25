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
	"slices"
)

const DefaultDiagnosisDeadline = 10 * time.Second

// ProgramRun is one diagnosis for a concrete finding or hint binding.
type ProgramRun struct {
	CanMitigate        *bool  `json:"can_mitigate,omitempty"`
	FindingFingerprint string `json:"finding_fingerprint,omitempty"`
	BindingSource      string `json:"binding_source,omitempty"`
	// Retrieval is how the playbook came to be tried (Retrieval* below).
	Retrieval       string           `json:"retrieval,omitempty"`
	ProgramID       string           `json:"program_id"`
	Bindings        program.Bindings `json:"bindings"`
	Verdict         program.Verdict  `json:"verdict"`
	Duration        time.Duration    `json:"duration"`
	AskedAt         time.Time        `json:"-"`
	ProgramRevision string           `json:"program_revision,omitempty"`
	// MitigateCommand is the command that applies this playbook with these
	// bindings; set on a matching diagnosis that has a fix.
	MitigateCommand string `json:"mitigate_command,omitempty"`
	// suspectRank orders credited bindings by where their suspect first
	// appears in the findings; it is never persisted.
	suspectRank int
}

// Binding sources a candidate producer may emit. Each must be accepted by the
// binding_source rule on contracts.ProgramRun (program.proto); the conformance
// test checks every value in BindingSources against it.
const (
	// BindingSourceFinding binds through the resources a finding names.
	BindingSourceFinding = "finding"
	// BindingSourceHint binds through a finding's authored parameter hint or playbook reference.
	BindingSourceHint = "hint"
	// BindingSourceSuspect binds the built-in source-restore candidate by the
	// suspect component on a failing path.
	BindingSourceSuspect = "suspect"
	// BindingSourceCredited marks a run bound through a playbook's credited
	// (symptom, component) pair rather than through a finding's resources.
	BindingSourceCredited = "credited"
)

// Retrieval paths: how a playbook came to be tried on a finding. Attribution only; none of them filters or orders.
const (
	// RetrievalDetector: a detector finding names the playbook.
	RetrievalDetector = "detector"
	// RetrievalRoleLessScan: a playbook with no object role is tried on every finding, named or not.
	RetrievalRoleLessScan = "role_less_scan"
	// RetrievalRoleBound: the playbook's object roles bound from a finding's resources.
	RetrievalRoleBound = "role_bound"
	// RetrievalCredited: bound through a credited (symptom, component) pair.
	RetrievalCredited = "credited"
	// RetrievalSuspect: the built-in source-restore candidate for a suspect component.
	RetrievalSuspect = "suspect"
)

// RetrievalPaths lists every value a candidate producer can emit.
var RetrievalPaths = []string{RetrievalDetector, RetrievalRoleLessScan, RetrievalRoleBound, RetrievalCredited, RetrievalSuspect}

// findingRetrieval attributes the diagnosis of spec on finding: a detector that names the playbook comes first.
func findingRetrieval(spec program.ProgramSpec, finding sdk.Finding) string {
	for _, reference := range finding.Playbooks {
		if id, err := layout.PlaybookID(reference); err == nil && id == spec.ID {
			return RetrievalDetector
		}
	}
	for _, param := range spec.Params {
		if param.Kind != "" {
			return RetrievalRoleBound
		}
	}
	return RetrievalRoleLessScan
}

// BindingSources lists every value a candidate producer can emit.
var BindingSources = []string{BindingSourceFinding, BindingSourceHint, BindingSourceSuspect, BindingSourceCredited}

// KeyedMatching binds playbooks by the pairs they were credited for.
type KeyedMatching struct {
	// Credits are the pairs each playbook earned. A playbook without any keeps
	// the finding binding.
	Credits ProgramCredits
	// OpeningKeys are the alerts the incident opened on: the symptom side of a pair.
	OpeningKeys []string
	// Stage is the automatic stage: a playbook with credited pairs binds only
	// through them, and of several credited Matches only the first in suspect
	// order is kept. The responder's own diagnosis (Stage false) also sees the
	// finding-bound candidates, so a playbook that matches a suspect it was not
	// yet credited for can still be offered.
	Stage bool
	// Restore are the built-in source-restore candidates: suspects on every
	// failing path, bound by component name. They are offered like any playbook's
	// candidates; whether the stage applies one is the loop's BuiltinRestoreAuto.
	Restore   []sourceRestoreCandidate
	Attempted map[string]bool
}

func (k KeyedMatching) active() bool { return len(k.Credits) > 0 }

// SelectProgramCandidates diagnoses finding-bound programs under one deadline.
func SelectProgramCandidates(ctx context.Context, registry program.ProgramRegistry, view program.ReadView, findings []sdk.Finding, deadline time.Duration) []ProgramRun {
	return SelectKeyedProgramCandidates(ctx, registry, view, findings, deadline, KeyedMatching{})
}

// SelectKeyedProgramCandidates is SelectProgramCandidates plus credited-pair
// binding: for each pair whose symptom the incident opened on, a suspect (a
// resource a finding names) equal to the pair's component binds the playbook's
// component role, provided every other parameter binds too.
func SelectKeyedProgramCandidates(ctx context.Context, registry program.ProgramRegistry, view program.ReadView, findings []sdk.Finding, deadline time.Duration, keyed KeyedMatching) []ProgramRun {
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
		retrieval   string
		id          string
		bindings    program.Bindings
		rank        int
	}
	var candidates []candidate
	seen := map[string]bool{}
	aliases := map[string][]candidate{}
	position := map[string]int{}
	add := func(id string, bindings program.Bindings, fingerprint, source, retrieval string, rank int) {
		key := stageAttemptKey(id, bindings)
		if keyed.Attempted[key] {
			return
		}
		if seen[key] {
			// A finding that names the playbook is why it was tried, whichever finding got there first.
			if retrieval == RetrievalDetector {
				candidates[position[key]].retrieval = retrieval
			}
			if fingerprint != "" {
				aliases[key] = append(aliases[key], candidate{fingerprint: fingerprint, source: source, retrieval: retrieval, id: id, bindings: bindings, rank: rank})
			}
			return
		}
		seen[key] = true
		position[key] = len(candidates)
		candidates = append(candidates, candidate{fingerprint: fingerprint, source: source, retrieval: retrieval, id: id, bindings: bindings, rank: rank})
	}
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		for _, spec := range registry.List() {
			if spec.ID == SourceRestoreID || keyed.Stage && keyed.Credits.Has(spec.ID) {
				continue
			}
			bindings, source, ok := findingProgramBindings(spec, finding)
			if ok {
				add(spec.ID, bindings, FindingFingerprint(finding), source, findingRetrieval(spec, finding), 0)
			}
		}
	}
	for _, restore := range keyed.Restore {
		if _, ok := registry.Diagnoser(SourceRestoreID); ok {
			add(SourceRestoreID, restore.bindings, restore.fingerprint, BindingSourceSuspect, RetrievalSuspect, restore.rank)
		}
	}
	if keyed.active() {
		for _, spec := range registry.List() {
			if spec.ID == SourceRestoreID {
				continue
			}
			for _, credited := range creditedBindings(spec, keyed, findings) {
				add(spec.ID, credited.bindings, credited.fingerprint, BindingSourceCredited, RetrievalCredited, credited.rank)
			}
		}
	}
	results := make(chan ProgramRun, len(candidates))
	appendAssociations := func(runs []ProgramRun, run ProgramRun) []ProgramRun {
		_, canMitigate := registry.Mitigator(run.ProgramID)
		run.CanMitigate = &canMitigate
		runs = append(runs, run)
		for _, alias := range aliases[run.ProgramID+"\x00"+bindingKey(run.Bindings)] {
			associated := run
			associated.FindingFingerprint = alias.fingerprint
			associated.BindingSource = alias.source
			associated.Retrieval = alias.retrieval
			runs = append(runs, associated)
		}
		return runs
	}
	for _, item := range candidates {
		go func(candidate candidate) {
			started := lifecycleio.Now()
			diagnoser, ok := registry.Diagnoser(candidate.id)
			if !ok {
				return
			}
			verdict := program.RunDiagnose(ctx, diagnoser, view, candidate.bindings, deadline)
			results <- ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, Retrieval: candidate.retrieval, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: verdict, Duration: lifecycleio.Since(started), AskedAt: started, suspectRank: candidate.rank}
		}(item)
	}
	runs := make([]ProgramRun, 0, len(candidates))
	for range candidates {
		select {
		case run := <-results:
			runs = appendAssociations(runs, run)
		case <-ctx.Done():
			for _, candidate := range candidates {
				found := false
				for _, run := range runs {
					if run.ProgramID == candidate.id && sameBindings(run.Bindings, candidate.bindings) {
						found = true
						break
					}
				}
				if !found {
					runs = appendAssociations(runs, ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, Retrieval: candidate.retrieval, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "diagnosis", Observed: ctx.Err().Error(), Expected: "completed before deadline"}}}, Duration: deadline, suspectRank: candidate.rank})
				}
			}
			sortProgramRuns(runs)
			return keyedStageRuns(runs, keyed)
		}
	}
	sortProgramRuns(runs)
	return keyedStageRuns(runs, keyed)
}

type creditedBinding struct {
	bindings    program.Bindings
	fingerprint string
	rank        int
}

// creditedBindings binds the spec's component role to each suspect, in the
// order the findings name them, whose (symptom, component) pair the playbook was
// credited for. Every other parameter must bind as well: a pair never leaves a
// required role unbound. The finding that named the suspect is the run's
// finding, so coverage explains it.
func creditedBindings(spec program.ProgramSpec, keyed KeyedMatching, findings []sdk.Finding) []creditedBinding {
	var result []creditedBinding
	if !keyed.Credits.Has(spec.ID) {
		return nil
	}
	rank := 0
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		for _, suspect := range append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...) {
			if suspect.Name == "" {
				continue
			}
			rank++
			for _, symptom := range keyed.OpeningKeys {
				if !keyed.Credits.Credited(spec.ID, symptom, suspect) {
					continue
				}
				bindings := program.Bindings{Objects: program.ObjectBindings{ComponentRole: suspect}}
				if program.ValidateBindings(spec, bindings) != nil {
					break
				}
				result = append(result, creditedBinding{bindings: bindings, fingerprint: FindingFingerprint(finding), rank: rank})
				break
			}
		}
	}
	return result
}

// keyedStageRuns keeps, for each playbook with credited pairs, only the first
// Match in suspect order: one playbook application per stage pass. Diagnoses
// that did not Match stay as evidence.
func keyedStageRuns(runs []ProgramRun, keyed KeyedMatching) []ProgramRun {
	if !keyed.Stage || !keyed.active() {
		return runs
	}
	first := map[string]int{}
	for _, run := range runs {
		if run.BindingSource != BindingSourceCredited || run.Verdict.Result != program.Match {
			continue
		}
		if rank, ok := first[run.ProgramID]; !ok || run.suspectRank < rank {
			first[run.ProgramID] = run.suspectRank
		}
	}
	return slices.DeleteFunc(runs, func(run ProgramRun) bool {
		rank, ok := first[run.ProgramID]
		return ok && run.BindingSource == BindingSourceCredited && run.Verdict.Result == program.Match && run.suspectRank != rank
	})
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
	named := findingNamesPlaybook(finding, spec.ID)
	if err == nil && (!hasObjectRoles || len(bound.Objects) > 0) {
		source := BindingSourceFinding
		if named {
			source = BindingSourceHint
		}
		for name, ref := range bound.Objects {
			if finding.ParameterBindings[name] == ref && ref.Kind != "" {
				source = BindingSourceHint
			}
		}
		return bound, source, true
	}
	if named {
		return cloneProgramBindings(bound), BindingSourceHint, true
	}
	return program.Bindings{}, "", false
}

// findingNamesPlaybook reports whether the finding lists the playbook: the
// detector that produced it says this playbook is the one for its cause.
func findingNamesPlaybook(finding sdk.Finding, id string) bool {
	for _, reference := range finding.Playbooks {
		if named, err := layout.PlaybookID(reference); err == nil && named == id {
			return true
		}
	}
	return false
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
