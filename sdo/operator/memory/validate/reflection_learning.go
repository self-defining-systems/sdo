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
	for _, d := range detectors {
		for _, id := range d.Manifest.PossiblePlaybooks {
			linked[id] = true
		}
	}
	var ds []Diagnostic
	for _, cause := range causes {
		hasProgram, hasDetector := false, false
		for _, spec := range specs {
			if spec.Mitigator == "" {
				continue
			}
			for _, id := range spec.Cases {
				c, err := sdktest.LoadCorpusCase(filepath.Join(root, layout.CorpusDir, id))
				if err != nil {
					return nil, err
				}
				if !slices.Contains(c.RootCauseProgramIDs, spec.ID) || !bindingsCoverCause(c.Bindings.Objects, cause.GetResources()) {
					continue
				}
				hasProgram = true
				hasDetector = hasDetector || linked[spec.ID]
			}
		}
		if !hasProgram {
			ds = append(ds, Diagnostic{Rule: RuleReflectionProgramRequired, Message: fmt.Sprintf("verified cause %d requires a diagnose program, mitigation program, README and a recorded-observation replay fixture, or a typed SDK gap naming these cause resources", cause.GetCauseIndex())})
		} else if !hasDetector {
			ds = append(ds, Diagnostic{Rule: RuleReflectionDetectorRequired, Message: fmt.Sprintf("verified cause %d requires a detector referencing its executable playbook; detectors may reference several playbooks", cause.GetCauseIndex())})
		}
	}
	return ds, nil
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
