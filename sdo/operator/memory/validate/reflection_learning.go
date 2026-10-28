package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// ReflectionCoverageGaps notes each verified cause that reflection left without
// executable learning: no replayable program, or a program no finding links.
// The link is the automatic stage's binding rule (findingBindsCase). A detector
// that lists the playbook without binding its roles does not link it. Health
// and generated detectors need no edit to reach a learned program.
// These cause-level notes remain informational. ReflectionRepairDiagnostics
// separately makes RuleReflectionProgramRequired blocking for each recorded
// hand repair; detector coverage and replay are still advisory. Memory that
// cannot be inspected yields no notes: the structural layers report why.
func ReflectionCoverageGaps(root string, outcome *contracts.OutcomeRecord, decision *contracts.ReflectionDecision) (gaps []Diagnostic) {
	if decision.GetLearningDecision() == "no_change" && decision.GetNoChangeReason() == "no_session" && outcome.GetReflectionSkipReason() == "no_session" {
		return nil
	}
	causes := VerifiedReflectionCauses(outcome)
	if len(causes) == 0 {
		return nil
	}
	moduleData, _ := os.ReadFile(filepath.Join(root, layout.DiagnosticsGoModFile))
	module := ""
	for _, line := range strings.Split(string(moduleData), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[0] == "module" {
			module = parts[1]
			break
		}
	}
	specs, err := programvalidation.InspectMemory(filepath.Join(root, layout.Dir), module)
	if err != nil {
		return nil
	}
	for _, cause := range causes {
		hasProgram, hasDetector := false, false
		for _, spec := range specs {
			if spec.Mitigator == "" {
				continue
			}
			for _, id := range spec.Cases {
				c, err := sdktest.LoadCorpusCase(filepath.Join(root, layout.CorpusDir, id))
				if err != nil {
					return nil
				}
				if !slices.Contains(c.RootCauseProgramIDs, spec.ID) || (!bindingsCoverRepairs(outcome, c.Bindings.Objects, cause.GetResources()) || !UndetectedCaseMatches(outcome, c, cause)) {
					continue
				}
				hasProgram = true
				hasDetector = hasDetector || findingBindsCase(outcome, c.Bindings.Objects)
			}
		}
		resources := make([]string, 0, len(cause.GetResources()))
		for _, r := range cause.GetResources() {
			resources = append(resources, fmt.Sprintf("%s %s/%s", r.GetKind(), r.GetNamespace(), r.GetName()))
		}
		switch {
		case !hasProgram:
			gaps = append(gaps, Diagnostic{Rule: RuleReflectionProgramRequired, Message: fmt.Sprintf("verified cause %d (%s) has no replayable diagnose and mitigation program", cause.GetCauseIndex(), strings.Join(resources, ", "))})
		case !hasDetector:
			gaps = append(gaps, Diagnostic{Rule: RuleReflectionDetectorRequired, Message: fmt.Sprintf("verified cause %d (%s) has a program, but no finding binds its roles", cause.GetCauseIndex(), strings.Join(resources, ", "))})
		}
	}
	return gaps
}

// DeclineNoRecordedRepair is the one typed decline reason. It records a verified
// cause that has no recorded repair transition on its own resources.
const DeclineNoRecordedRepair = "no_recorded_repair"

// Only repaired targets need parameters. Symptom resources remain in the cause
// and replay snapshot, with independent opening/clearance evidence checked by
// VerifiedReflectionCauses; they need not be written or exposed as extra roles.
func bindingsCoverRepairs(outcome *contracts.OutcomeRecord, bindings map[string]sdk.ObjectRef, resources []*contracts.ObjectRef) bool {
	repaired, _, valid := causeRepairs(outcome, resources)
	return valid && bindingsCoverCause(bindings, repaired)
}

func bindingsCoverCause(bindings map[string]sdk.ObjectRef, resources []*contracts.ObjectRef) bool {
	if len(resources) == 0 {
		return false
	}
	for _, r := range resources {
		found := false
		for _, b := range bindings {
			found = found || (b.Kind == r.GetKind() && b.Namespace == r.GetNamespace() && b.Name == r.GetName())
		}
		if !found {
			return false
		}
	}
	return true
}

// findingBindsCase reports whether the automatic stage can bind the recorded
// case from one incident finding: program.BindFinding, the stage's binding
// rule, fills every role the case binds with that same object. A listed
// playbook without these bindings is diagnosed unbound and never runs.
func findingBindsCase(outcome *contracts.OutcomeRecord, bindings map[string]sdk.ObjectRef) bool {
	if len(bindings) == 0 {
		return false
	}
	spec := program.ProgramSpec{ID: "recorded-case"}
	for role, ref := range bindings {
		spec.Params = append(spec.Params, program.ParamSpec{Name: role, Kind: program.Kind(ref.Kind), Required: true})
	}
	sort.Slice(spec.Params, func(i, j int) bool { return spec.Params[i].Name < spec.Params[j].Name })
	for _, finding := range outcome.GetFindings() {
		bound, err := program.BindFinding(spec, findingForBinding(finding))
		if err != nil {
			continue
		}
		all := true
		for role, want := range bindings {
			got := bound.Objects[role]
			all = all && got.Kind == want.Kind && got.Namespace == want.Namespace && got.Name == want.Name
		}
		if all {
			return true
		}
	}
	return false
}

func findingForBinding(finding *contracts.Finding) sdk.Finding {
	ref := func(r *contracts.ObjectRef) sdk.ObjectRef {
		return sdk.ObjectRef{APIVersion: r.GetApiVersion(), Kind: r.GetKind(), Namespace: r.GetNamespace(), Name: r.GetName()}
	}
	result := sdk.Finding{PrimaryResource: ref(finding.GetPrimaryResource())}
	for _, related := range finding.GetRelatedResources() {
		result.RelatedResources = append(result.RelatedResources, ref(related))
	}
	for role, bound := range finding.GetParameterBindings() {
		if result.ParameterBindings == nil {
			result.ParameterBindings = map[string]sdk.ObjectRef{}
		}
		result.ParameterBindings[role] = ref(bound)
	}
	return result
}
