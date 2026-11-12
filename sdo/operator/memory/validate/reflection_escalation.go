package validate

import (
	"os"
	"path/filepath"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
)

// stageEscalatedResult is StageMitigation.result of an automatic-stage attempt
// that escalated on a matched out-of-reach Diagnose.
const stageEscalatedResult = "escalated"

// escalationLearning reports whether an outcome is an escalation with a
// confirmed out-of-reach cause that the responder confirmed itself: no automatic-stage attempt escalated it, so no
// accepted playbook recognizes the fault yet.
func escalationLearning(outcome *contracts.OutcomeRecord) bool {
	if outcome.GetClassification() != outcomeclass.Escalated || !outcomeclass.Escalation(outcome.GetConfirmedRootCauses(), outcome.GetFindings(), outcome.GetRepairActions()) {
		return false
	}
	for _, stage := range outcome.GetPriorStages() {
		if slices.ContainsFunc(stage.GetMitigations(), func(m *contracts.StageMitigation) bool { return m.GetResult() == stageEscalatedResult }) {
			return false
		}
	}
	return true
}

// diagnoseOnlyChanges reports whether the reflection changed at least one
// playbook and every playbook it changed is Diagnose-only (no Mitigate): the
// only learning an escalated closure grants, since nothing was repaired.
func diagnoseOnlyChanges(root string, paths []string) bool {
	changed := map[string]bool{}
	for _, p := range paths {
		kind, id, ok := layout.Classify(p)
		id, _, _ = strings.Cut(id, "/")
		if ok && (kind == layout.KindPlaybook || kind == layout.KindPlaybookProgram || kind == layout.KindPlaybookScript) {
			changed[id] = true
		}
	}
	if len(changed) == 0 {
		return false
	}
	moduleData, _ := os.ReadFile(filepath.Join(root, layout.DiagnosticsGoModFile))
	module := ""
	for _, line := range strings.Split(string(moduleData), "\n") {
		if fields := strings.Fields(line); len(fields) == 2 && fields[0] == "module" {
			module = fields[1]
			break
		}
	}
	specs, err := programvalidation.InspectMemoryForAdoption(filepath.Join(root, layout.Dir), module)
	if err != nil {
		return false
	}
	seen := map[string]bool{}
	for _, spec := range specs {
		if !changed[spec.ID] {
			continue
		}
		if spec.Mitigator != "" {
			return false
		}
		seen[spec.ID] = true
	}
	return len(seen) > 0
}

// EscalationReflectionDiagnostics is the reflection check of an escalated
// closure the responder confirmed. Returning no_change leaves the fault to an
// agent next time, so it names the work: a Diagnose-only playbook whose
// verdict carries the confirmed cause_class and reach out-of-reach. Such a
// playbook is the one update the closure grants (it repairs nothing), so the
// caller exempts it from reflection.repair-required.
func EscalationReflectionDiagnostics(root string, outcome *contracts.OutcomeRecord, decision *contracts.ReflectionDecision, paths []string) (diagnostics []Diagnostic, diagnoseOnlyUpdate bool) {
	if !escalationLearning(outcome) || decision == nil {
		return nil, false
	}
	if decision.GetLearningDecision() == "updated" {
		return nil, diagnoseOnlyChanges(root, paths)
	}
	return []Diagnostic{{
		Rule:    RuleReflectionEscalationDiagnose,
		Message: "this closure escalated a confirmed out-of-reach cause that no accepted playbook recognizes yet",
		Hint:    "save a Diagnose-only playbook (no mitigate.go) whose Match verdict sets the cause's CauseClass and Reach out-of-reach from the evidence that localized it; SDO then escalates a repeat without an agent",
	}}, false
}
