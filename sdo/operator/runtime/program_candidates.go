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

<<<<<<< HEAD
type CandidateOptions struct {
	// Clock measures diagnosis duration; nil uses the production clock.
	Clock      func() time.Time
	Deadline   time.Duration
	SweepLimit int
	Affected   []sdk.ObjectRef
	// SweepCursors stores the next mixed-radix assignment per program.
	SweepCursors map[string]string
}

// ProgramRun is one diagnosis for a concrete finding, hint, or sweep binding.
=======
// ProgramRun is one diagnosis for a concrete finding or hint binding.
>>>>>>> origin/vic-lsh/dev/sdo-improve
type ProgramRun struct {
	CanMitigate        *bool            `json:"can_mitigate,omitempty"`
	FindingFingerprint string           `json:"finding_fingerprint,omitempty"`
	BindingSource      string           `json:"binding_source,omitempty"`
	ProgramID          string           `json:"program_id"`
	Bindings           program.Bindings `json:"bindings"`
	Verdict            program.Verdict  `json:"verdict"`
	Duration           time.Duration    `json:"duration"`
	ProgramRevision    string           `json:"program_revision,omitempty"`
}

// SelectProgramCandidates diagnoses finding-bound programs under one deadline.
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
	}
	var candidates []candidate
	seen := map[string]bool{}
	aliases := map[string][]candidate{}
	add := func(id string, bindings program.Bindings, fingerprint, source string) {
		key := id + "\x00" + bindingKey(bindings)
		if seen[key] {
			if fingerprint != "" {
				aliases[key] = append(aliases[key], candidate{fingerprint: fingerprint, source: source, id: id, bindings: bindings})
			}
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
			bindings, source, ok := findingProgramBindings(spec, finding)
			if ok {
				add(spec.ID, bindings, FindingFingerprint(finding), source)
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
			runs = append(runs, associated)
		}
		return runs
	}
	for _, item := range candidates {
		go func(candidate candidate) {
			clock := options.Clock
			if clock == nil {
				clock = lifecycleio.Now
			}
			started := clock()
			diagnoser, ok := registry.Diagnoser(candidate.id)
			if !ok {
				return
			}
			verdict := program.RunDiagnose(ctx, diagnoser, view, candidate.bindings, deadline)
			results <- ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: verdict, Duration: clock().Sub(started)}
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
					runs = appendAssociations(runs, ProgramRun{FindingFingerprint: candidate.fingerprint, BindingSource: candidate.source, ProgramID: candidate.id, Bindings: candidate.bindings, Verdict: program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "diagnosis", Observed: ctx.Err().Error(), Expected: "completed before deadline"}}}, Duration: deadline})
				}
			}
			sortProgramRuns(runs)
			return runs
		}
	}
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
<<<<<<< HEAD
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
	case "StatefulSet":
		apiVersion = "apps/v1"
		items := view.StatefulSets()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "DaemonSet":
		apiVersion = "apps/v1"
		items := view.DaemonSets()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Job":
		apiVersion = "batch/v1"
		items := view.Jobs()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "CronJob":
		apiVersion = "batch/v1"
		items := view.CronJobs()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "PersistentVolumeClaim":
		apiVersion = "v1"
		items := view.PersistentVolumeClaims()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "ServiceAccount":
		apiVersion = "v1"
		items := view.ServiceAccounts()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "HorizontalPodAutoscaler":
		apiVersion = "autoscaling/v2"
		items := view.HorizontalPodAutoscalers()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "PodDisruptionBudget":
		apiVersion = "policy/v1"
		items := view.PodDisruptionBudgets()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Ingress":
		apiVersion = "networking.k8s.io/v1"
		items := view.Ingresses()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Node":
		apiVersion = "v1"
		items := view.Nodes()
		for i := range items {
			objects = append(objects, &items[i])
		}
	case "Secret":
		for _, item := range view.Secrets() {
			metadata := item.ObjectMeta()
			objects = append(objects, &metadata)
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
		if (object.GetNamespace() == view.Namespace() || kind == "Node" && object.GetNamespace() == "") && object.GetName() != "" {
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
=======
	if err == nil && (!hasObjectRoles || len(bound.Objects) > 0) {
		source := "finding"
		for name, ref := range bound.Objects {
			if finding.ParameterBindings[name] == ref && ref.Kind != "" {
				source = "hint"
>>>>>>> origin/vic-lsh/dev/sdo-improve
			}
		}
		return bound, source, true
	}
	for _, reference := range finding.Playbooks {
		id, err := layout.PlaybookID(reference)
		if err == nil && id == spec.ID {
			return cloneProgramBindings(bound), "hint", true
		}
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
