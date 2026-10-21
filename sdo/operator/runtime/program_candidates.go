package runtime

import (
	"context"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const DefaultDiagnosisDeadline = 10 * time.Second
const maxCandidateAssignments = 32
const DefaultSweepLimit = 64

type CandidateOptions struct {
	Deadline   time.Duration
	SweepLimit int
	Affected   []sdk.ObjectRef
}

// ProgramRun is one diagnosis for a concrete finding, hint, or sweep binding.
type ProgramRun struct {
	FindingFingerprint string           `json:"finding_fingerprint,omitempty"`
	BindingSource      string           `json:"binding_source,omitempty"`
	ProgramID          string           `json:"program_id"`
	Bindings           program.Bindings `json:"bindings"`
	Verdict            program.Verdict  `json:"verdict"`
	Duration           time.Duration    `json:"duration"`
	ProgramRevision    string           `json:"program_revision,omitempty"`
}

// SelectProgramCandidates diagnoses all bindable programs under one deadline.
func SelectProgramCandidates(ctx context.Context, registry program.ProgramRegistry, view program.ReadView, findings []sdk.Finding, deadline time.Duration) []ProgramRun {
	return SelectProgramCandidatesWithOptions(ctx, registry, view, findings, CandidateOptions{Deadline: deadline})
}

// SelectProgramCandidatesWithOptions diagnoses finding and namespace-sweep bindings.
func SelectProgramCandidatesWithOptions(ctx context.Context, registry program.ProgramRegistry, view program.ReadView, findings []sdk.Finding, options CandidateOptions) []ProgramRun {
	if registry == nil {
		return nil
	}
	deadline := options.Deadline
	if deadline <= 0 {
		deadline = DefaultDiagnosisDeadline
	}
	limit := options.SweepLimit
	if limit <= 0 {
		limit = DefaultSweepLimit
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
	}
	var candidates []candidate
	seen := map[string]bool{}
	add := func(id string, bindings program.Bindings, fingerprint, source string) {
		if len(options.Affected) > 0 {
			matched := false
			for _, ref := range bindings {
				for _, affected := range options.Affected {
					if ref.Kind == affected.Kind && ref.Namespace == affected.Namespace && ref.Name == affected.Name {
						matched = true
					}
				}
			}
			if !matched {
				return
			}
		}
		key := id + "\x00" + bindingKey(bindings)
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, candidate{fingerprint: fingerprint, source: source, id: id, bindings: bindings})
	}
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		for _, spec := range registry.List() {
			assignments := candidateBindings(spec, finding)
			hintOnly := false
			if len(assignments) == 0 {
				for _, reference := range finding.Playbooks {
					id, err := memory.PlaybookID(reference)
					if err == nil && id == spec.ID {
						assignments = []program.Bindings{cloneProgramBindings(program.Bindings(finding.ParameterBindings))}
						hintOnly = true
						break
					}
				}
			}
			for _, bindings := range assignments {
				source := "finding"
				if hintOnly {
					source = "hint"
				} else {
					for name, ref := range bindings {
						if finding.ParameterBindings[name] == ref && ref.Kind != "" {
							source = "hint"
						}
					}
				}
				add(spec.ID, bindings, FindingFingerprint(finding), source)
			}
		}
	}
	for _, spec := range registry.List() {
		if len(spec.Params) == 0 {
			continue
		}
		refs := make([][]sdk.ObjectRef, len(spec.Params))
		valid := true
		for index, param := range spec.Params {
			if !param.Required || param.Kind == "" {
				valid = false
				break
			}
			refs[index] = sweepRefs(view, param.Kind)
			if len(refs[index]) == 0 {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		count := 0
		var bind func(int, program.Bindings)
		bind = func(index int, current program.Bindings) {
			if count >= limit || ctx.Err() != nil {
				return
			}
			if index == len(spec.Params) {
				before := len(candidates)
				add(spec.ID, current, "", "sweep")
				if len(candidates) > before {
					count++
				}
				return
			}
			for _, ref := range refs[index] {
				if count >= limit || ctx.Err() != nil {
					break
				}
				if bindingUses(current, ref) {
					continue
				}
				next := cloneProgramBindings(current)
				next[spec.Params[index].Name] = ref
				bind(index+1, next)
			}
		}
		bind(0, program.Bindings{})
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
			results <- ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: verdict, Duration: time.Since(started)}
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
					if run.ProgramID == candidate.id && sameBindings(run.Bindings, candidate.bindings) {
						found = true
						break
					}
				}
				if !found {
					runs = append(runs, ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "diagnosis", Observed: ctx.Err().Error(), Expected: "completed before deadline"}}}, Duration: deadline})
				}
			}
			sortProgramRuns(runs)
			return runs
		}
	}
	sortProgramRuns(runs)
	return runs
}

func sweepRefs(view program.ReadView, kind program.Kind) []sdk.ObjectRef {
	if view == nil || view.Namespace() == "" {
		return nil
	}
	var objects []metav1.Object
	apiVersion := "v1"
	switch kind {
	case "Service":
		items := view.Services()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "ConfigMap":
		items := view.ConfigMaps()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Pod":
		items := view.Pods()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Endpoints":
		items := view.Endpoints()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Event":
		items := view.Events()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Deployment":
		apiVersion = "apps/v1"
		items := view.Deployments()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "ReplicaSet":
		apiVersion = "apps/v1"
		items := view.ReplicaSets()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "EndpointSlice":
		apiVersion = "discovery.k8s.io/v1"
		items := view.EndpointSlices()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "NetworkPolicy":
		apiVersion = "networking.k8s.io/v1"
		items := view.NetworkPolicies()
		for i := range items {
			objects = append(objects, &items[i])
		}
	default:
		return nil
	}
	refs := make([]sdk.ObjectRef, 0, len(objects))
	for _, object := range objects {
		if object.GetNamespace() == view.Namespace() && object.GetName() != "" {
			refs = append(refs, sdk.ObjectRefFrom(string(kind), apiVersion, object))
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	return refs
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
