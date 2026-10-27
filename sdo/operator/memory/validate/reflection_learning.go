package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// ReflectionLearningDiagnostics requires executable learning for each verified
// cause. Corpus bindings associate parameterized programs with cause resources;
// detector references are many-to-many. Compilation and repair replay follow.
func ReflectionLearningDiagnostics(root string, outcome *contracts.OutcomeRecord, decision *contracts.ReflectionDecision) ([]Diagnostic, error) {
	if decision.GetLearningDecision() == "no_change" && decision.GetNoChangeReason() == "no_session" && outcome.GetReflectionSkipReason() == "no_session" {
		return nil, nil
	}
	causes := VerifiedReflectionCauses(outcome)
	var pending []*contracts.ReflectionCauseWork
	for _, cause := range causes {
		declined := false
		for _, gap := range decision.GetSdkGaps() {
			declined = declined || strings.TrimSpace(gap.GetReason()) != "" && gap.GetPrimitive() != "" && gap.GetFaultClass() != "" && gap.GetScratchScript() != "" && sameCauseResources(cause.GetResources(), gap.GetResources())
		}
		if !declined {
			pending = append(pending, cause)
		}
	}
	causes = pending
	if len(causes) == 0 {
		return nil, nil
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
		return []Diagnostic{{Rule: RuleReflectionProgramRequired, Message: err.Error()}}, nil
	}
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		return nil, err
	}
	linked := map[string]bool{}
	var incidentDetectors []string
	for _, d := range detectors {
		for _, id := range d.Manifest.PossiblePlaybooks {
			linked[id] = true
		}
		if d.Class == layout.ClassIncident {
			incidentDetectors = append(incidentDetectors, d.Manifest.ID)
		}
	}
	var ds []Diagnostic
	for _, cause := range causes {
		hasProgram, hasDetector := false, false
		var programs []string
		for _, spec := range specs {
			if spec.Mitigator == "" {
				continue
			}
			for _, id := range spec.Cases {
				c, err := sdktest.LoadCorpusCase(filepath.Join(root, layout.CorpusDir, id))
				if err != nil {
					return nil, err
				}
				if !slices.Contains(c.RootCauseProgramIDs, spec.ID) || (!bindingsCoverRepairs(outcome, c.Bindings.Objects, cause.GetResources()) || !UndetectedCaseMatches(outcome, c, cause)) {
					continue
				}
				hasProgram = true
				hasDetector = hasDetector || linked[spec.ID]
				if !slices.Contains(programs, spec.ID) {
					programs = append(programs, spec.ID)
				}
			}
		}
		if !hasProgram && decision.GetDeclineReason() == DeclineNoRecordedRepair && !hasOwnTransition(outcome, cause) {
			// The cause's own resources have no recorded repair write, so no
			// program can bind or replay it. It is recorded, not demanded.
			continue
		}
		if !hasProgram {
			ds = append(ds, Diagnostic{Rule: RuleReflectionProgramRequired, Message: fmt.Sprintf("verified cause %d requires a diagnose program, mitigation program, README and a recorded-observation replay fixture, or a typed SDK gap naming these cause resources; a cause with no recorded repair write on its own resources is recorded with decline_reason=no_recorded_repair", cause.GetCauseIndex())})
		} else if !hasDetector {
			ds = append(ds, detectorRequired(cause, programs, incidentDetectors))
		}
	}
	return ds, nil
}

// DeclineNoRecordedRepair is the one typed decline reason. It records a verified
// cause that has no recorded repair transition on its own resources.
const DeclineNoRecordedRepair = "no_recorded_repair"

// hasOwnTransition reports a successful repair write to one of the cause's own
// resources, the only transition a program can bind and replay.
func hasOwnTransition(outcome *contracts.OutcomeRecord, cause *contracts.ReflectionCauseWork) bool {
	repaired, _, valid := causeRepairs(outcome, cause.GetResources())
	return !valid || len(repaired) > 0
}

// detectorRequired names what to link and where. The check accepts any
// detector that lists the program; reflection can edit only responder-owned
// incident detectors, so that is where a new link goes.
func detectorRequired(cause *contracts.ReflectionCauseWork, programs, incidentDetectors []string) Diagnostic {
	resources := make([]string, 0, len(cause.GetResources()))
	for _, r := range cause.GetResources() {
		resources = append(resources, fmt.Sprintf("%s %s/%s", r.GetKind(), r.GetNamespace(), r.GetName()))
	}
	existing := "none yet"
	if len(incidentDetectors) > 0 {
		existing = strings.Join(incidentDetectors, ", ")
	}
	return Diagnostic{
		Rule: RuleReflectionDetectorRequired,
		Path: layout.DetectorDir(layout.ClassIncident, ""),
		Message: fmt.Sprintf("verified cause %d (%s) has executable program %s, but no detector lists it in possiblePlaybooks",
			cause.GetCauseIndex(), strings.Join(resources, ", "), strings.Join(programs, ", ")),
		Hint: fmt.Sprintf("list the program in possiblePlaybooks of a responder-owned incident detector that observes these resources (existing: %s) or add a new incident detector; "+
			"reflection cannot edit health-owned or built-in detectors such as drift.unexplained, so they cannot newly link a program", existing),
	}
}

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
func sameCauseResources(a, b []*contracts.ObjectRef) bool {
	if len(a) == 0 || len(a) != len(b) {
		return false
	}
	bound := map[string]sdk.ObjectRef{}
	for i, r := range b {
		bound[fmt.Sprint(i)] = sdk.ObjectRef{Kind: r.GetKind(), Namespace: r.GetNamespace(), Name: r.GetName()}
	}
	return bindingsCoverCause(bound, a)
}
