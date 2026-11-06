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
	declared := declaredPlaybooks(root)
	for _, cause := range causes {
		hasProgram, hasDetector := false, false
		var programs, unlisted []string
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
				bound, scoped := findingBindsCase(outcome, declared, spec.ID, c.Bindings.Objects)
				hasDetector = hasDetector || bound
				if !slices.Contains(programs, spec.ID) {
					programs = append(programs, spec.ID)
				}
				for _, detector := range scoped {
					if !slices.Contains(unlisted, detector+" -> "+spec.ID) {
						unlisted = append(unlisted, detector+" -> "+spec.ID)
					}
				}
			}
		}
		resources := make([]string, 0, len(cause.GetResources()))
		for _, r := range cause.GetResources() {
			resources = append(resources, fmt.Sprintf("%s %s/%s", r.GetKind(), r.GetNamespace(), r.GetName()))
		}
		switch {
		case !hasProgram:
			gaps = append(gaps, Diagnostic{Rule: RuleReflectionProgramRequired, Message: fmt.Sprintf("verified cause %d (%s) has no replayable diagnose and mitigation program", cause.GetCauseIndex(), strings.Join(resources, ", "))})
		case !hasDetector && len(unlisted) > 0:
			sort.Strings(unlisted)
			gaps = append(gaps, Diagnostic{Rule: RuleReflectionDetectorRequired,
				Message: fmt.Sprintf("verified cause %d (%s) has programs %s, but no finding binds their roles: a finding's named bindings bind only the playbooks it lists, and detector -> playbook %s would bind if listed", cause.GetCauseIndex(), strings.Join(resources, ", "), strings.Join(programs, ", "), strings.Join(unlisted, ", ")),
				Hint:    "add the playbook to that incident detector's possiblePlaybooks and Spec Playbooks (and to any Playbooks its findings set); a reused playbook must be listed by the detector whose binding it needs"})
		case !hasDetector:
			gaps = append(gaps, Diagnostic{Rule: RuleReflectionDetectorRequired,
				Message: fmt.Sprintf("verified cause %d (%s) has programs %s, but no finding binds their roles", cause.GetCauseIndex(), strings.Join(resources, ", "), strings.Join(programs, ", ")),
				Hint:    "add a cause-specific incident detector (the cause's failure text, log pattern or state read): bind each required role in its ParameterBindings and list the playbook in its possiblePlaybooks"})
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

// findingBindsCase reports whether the automatic stage binds program id's
// recorded case from one incident finding: program.BindFinding, the stage's
// binding rule, run with the program's own ID, fills every role the case binds
// with that same object. A finding lists the playbooks it recorded plus those
// its detector declares in this tree, so a detector edited to list the program
// counts. scoped names the incident detectors whose finding would bind the
// case if it listed the program.
func findingBindsCase(outcome *contracts.OutcomeRecord, declared map[string][]string, id string, bindings map[string]sdk.ObjectRef) (bound bool, scoped []string) {
	if len(bindings) == 0 {
		return false, nil
	}
	spec := program.ProgramSpec{ID: id}
	for role, ref := range bindings {
		spec.Params = append(spec.Params, program.ParamSpec{Name: role, Kind: program.Kind(ref.Kind), Required: true})
	}
	sort.Slice(spec.Params, func(i, j int) bool { return spec.Params[i].Name < spec.Params[j].Name })
	binds := func(finding sdk.Finding) bool {
		got, err := program.BindFinding(spec, finding)
		if err != nil {
			return false
		}
		for role, want := range bindings {
			if g := got.Objects[role]; g.Kind != want.Kind || g.Namespace != want.Namespace || g.Name != want.Name {
				return false
			}
		}
		return true
	}
	for _, recorded := range outcome.GetFindings() {
		finding := findingForBinding(recorded, declared[recorded.GetDetectorId()])
		if binds(finding) {
			return true, nil
		}
		// Only an incident detector is the responder's to edit.
		_, incident := declared[recorded.GetDetectorId()]
		finding.Playbooks = append(finding.Playbooks, id)
		if incident && binds(finding) && !slices.Contains(scoped, recorded.GetDetectorId()) {
			scoped = append(scoped, recorded.GetDetectorId())
		}
	}
	return false, scoped
}

// declaredPlaybooks maps each incident detector in the tree to the playbooks
// its manifest declares. Unreadable detectors declare nothing: the structural
// layers report them.
func declaredPlaybooks(root string) map[string][]string {
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		return nil
	}
	declared := map[string][]string{}
	for _, detector := range detectors {
		if detector.Class == layout.ClassIncident {
			declared[detector.Manifest.ID] = detector.Manifest.PossiblePlaybooks
		}
	}
	return declared
}

func findingForBinding(finding *contracts.Finding, declared []string) sdk.Finding {
	ref := func(r *contracts.ObjectRef) sdk.ObjectRef {
		return sdk.ObjectRef{APIVersion: r.GetApiVersion(), Kind: r.GetKind(), Namespace: r.GetNamespace(), Name: r.GetName()}
	}
	result := sdk.Finding{DetectorID: finding.GetDetectorId(), PrimaryResource: ref(finding.GetPrimaryResource())}
	for _, related := range finding.GetRelatedResources() {
		result.RelatedResources = append(result.RelatedResources, ref(related))
	}
	for role, bound := range finding.GetParameterBindings() {
		if result.ParameterBindings == nil {
			result.ParameterBindings = map[string]sdk.ObjectRef{}
		}
		result.ParameterBindings[role] = ref(bound)
	}
	result.Playbooks = append([]string(nil), finding.GetPlaybookIds()...)
	for _, id := range declared {
		if !slices.Contains(result.Playbooks, id) {
			result.Playbooks = append(result.Playbooks, id)
		}
	}
	return result
}
