package validate

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// ReflectionRepairDiagnostics binds the fast no-change path to controller
// receipts, not the model's explanation. Reported changed IDs are checked
// against the real diff by ReflectionDiagnostics. README-only learning remains
// valid; executable coverage and replay are separate, informational checks.
func ReflectionRepairDiagnostics(outcome *contracts.OutcomeRecord, decision *contracts.ReflectionDecision, paths []string) []Diagnostic {
	if decision == nil || (outcome.GetReflectionSkipReason() == "no_session" && decision.GetLearningDecision() == "no_change" && decision.GetNoChangeReason() == "no_session") {
		return nil
	}
	changed := map[string]bool{}
	if decision.GetLearningDecision() == "updated" {
		for _, p := range paths {
			kind, id, ok := layout.Classify(p)
			id, _, _ = strings.Cut(id, "/")
			if ok && (kind == layout.KindPlaybook || kind == layout.KindPlaybookProgram || kind == layout.KindPlaybookScript) && slices.Contains(decision.GetPlaybookIds(), id) {
				changed[id] = true
			}
		}
	}
	applications := append([]*contracts.PlanApplication(nil), outcome.GetPlanApplications()...)
	stageHandled := map[string]map[string]bool{}
	stale := map[string]string{}
	for _, stage := range outcome.GetPriorStages() {
		for _, m := range stage.GetMitigations() {
			if m.GetResult() == "skipped" {
				continue
			}
			app := m.GetApplyReceipt()
			if app != nil {
				applications = append(applications, app)
				if stageHandled[app.GetApplicationId()] == nil {
					stageHandled[app.GetApplicationId()] = map[string]bool{}
				}
				stageHandled[app.GetApplicationId()][m.GetPlaybookId()] = true
			}
			if m.GetResult() != "applied_cleared" || len(m.GetRemainingFindingFingerprints()) > 0 || m.GetError() != "" {
				stale[m.GetPlaybookId()] = "stage result " + m.GetResult()
			}
		}
	}
	var hand []*contracts.RepairActionReceipt
	for _, a := range outcome.GetRepairActions() {
		if !a.GetSuccess() {
			continue
		}
		fromPlaybook := false
		for _, app := range applications {
			if len(app.GetProgramIds()) > 0 && !app.GetDryRun() && applicationOwnsRepair(app, a) {
				fromPlaybook = true
				break
			}
		}
		if !fromPlaybook {
			hand = append(hand, a)
		}
	}
	for _, app := range applications {
		if app.GetDryRun() {
			continue
		}
		for _, id := range app.GetProgramIds() {
			if !stageHandled[app.GetApplicationId()][id] && (!app.GetApplied() || app.GetError() != "" || app.GetRolledBack() || app.GetRollbackError() != "" || !app.GetVerificationCompleted() || !app.GetVerificationPassed()) {
				stale[id] = "application failed or did not verify its findings"
			}
			for _, a := range hand {
				if app.GetCompletedAt() == nil || a.GetStartedAt() == nil || a.GetStartedAt().AsTime().Before(app.GetCompletedAt().AsTime()) {
					continue
				}
				if repairOverlapsPlaybook(outcome, app, id, a) {
					stale[id] = "needed later hand repair " + a.GetActionId()
				}
			}
		}
	}
	// Prior deterministic stages precede responder repairs even when a legacy
	// stage omitted its application receipt. Its own bindings still identify
	// the playbook that needed a later repair.
	for _, stage := range outcome.GetPriorStages() {
		for _, m := range stage.GetMitigations() {
			if stage.GetStage() != "deterministic" || m.GetResult() == "skipped" || m.GetApplyReceipt() != nil {
				continue
			}
			for _, a := range hand {
				if repairOverlapsPlaybook(outcome, nil, m.GetPlaybookId(), a) {
					stale[m.GetPlaybookId()] = "needed later hand repair " + a.GetActionId()
				}
			}
		}
	}
	remaining := map[string]bool{}
	for _, evaluation := range outcome.GetFinalHealthDetectorState() {
		if evaluation.GetStatus() == "firing" || evaluation.GetStatus() == "pending" {
			for _, fingerprint := range evaluation.GetFingerprints() {
				remaining[fingerprint] = true
			}
		}
	}
	applied := map[string]bool{}
	for _, id := range outcome.GetAppliedPlaybooks() {
		applied[id] = true
	}
	for _, app := range applications {
		for _, id := range app.GetProgramIds() {
			applied[id] = true
		}
	}
	for _, match := range outcome.GetPlaybookCoverage().GetMatches() {
		if applied[match.GetPlaybookId()] && slices.ContainsFunc(match.GetExplainedFindingFingerprints(), func(fingerprint string) bool { return remaining[fingerprint] }) {
			stale[match.GetPlaybookId()] = "its findings remain unresolved in the final controller observation"
		}
	}
	var ds []Diagnostic
	for _, a := range hand {
		if len(changed) > 0 || repairHasSDKGap(a, decision.GetSdkGaps()) {
			continue
		}
		ds = append(ds, Diagnostic{Rule: RuleReflectionUncoveredRepair, Message: fmt.Sprintf("repair %q (%s) is not covered by an updated playbook or scoped SDK gap", a.GetActionId(), a.GetTarget()), Hint: "save the useful repair in a changed playbook and return its ID, or name the missing capability in a typed SDK gap scoped to this repair"})
	}
	ids := make([]string, 0, len(stale))
	for id := range stale {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if changed[id] {
			continue
		}
		ds = append(ds, Diagnostic{Rule: RuleReflectionStalePlaybook, Message: fmt.Sprintf("applied playbook %q must change: %s", id, stale[id]), Hint: "update this playbook, or retire it with a reason, and include its ID in the updated result; a peer edit or SDK gap does not fix stale memory"})
	}
	return ds
}

// Match the controller's receipt identities, including per-script receipts and
// duplicate suffixes. A bare program-plan kind never proves playbook ownership:
// direct hand repairs also run through the ordinary plan executor.
func applicationOwnsRepair(app *contracts.PlanApplication, a *contracts.RepairActionReceipt) bool {
	id := app.GetApplicationId()
	if id == "" {
		return false
	}
	switch a.GetKind() {
	case "sdo-apply", "sdo-mitigate":
		return a.GetActionId() == id
	case "program-plan", "run-script":
		base := "sdo-program-" + id
		if a.GetActionId() == base {
			return true
		}
		suffix, ok := strings.CutPrefix(a.GetActionId(), base+"-")
		if !ok {
			return false
		}
		suffix = strings.TrimPrefix(suffix, "script-")
		for _, part := range strings.Split(suffix, "-") {
			n, err := strconv.Atoi(part)
			if err != nil || n < 1 {
				return false
			}
		}
		return suffix != ""
	}
	return false
}

func repairHasSDKGap(a *contracts.RepairActionReceipt, gaps []*contracts.SdkGap) bool {
	for _, gap := range gaps {
		if len(a.GetResources()) == 0 {
			if a.GetActionId() != "" && strings.Contains(gap.GetReason(), a.GetActionId()) {
				return true
			}
			continue
		}
		covered := true
		for _, ref := range a.GetResources() {
			covered = covered && slices.ContainsFunc(gap.GetResources(), func(other *contracts.ObjectRef) bool { return equalCauseResource(ref, other) })
		}
		if covered {
			return true
		}
	}
	return false
}

func repairOverlapsPlaybook(o *contracts.OutcomeRecord, app *contracts.PlanApplication, id string, a *contracts.RepairActionReceipt) bool {
	var resources []*contracts.ObjectRef
	// A batched application may contain unrelated playbooks. Its aggregate
	// receipt cannot attribute a peer's later repair to every member.
	if len(app.GetProgramIds()) == 1 {
		for _, receipt := range o.GetRepairActions() {
			if applicationOwnsRepair(app, receipt) {
				resources = append(resources, receipt.GetResources()...)
			}
		}
	}
	for _, match := range o.GetPlaybookCoverage().GetMatches() {
		if match.GetPlaybookId() == id {
			for _, b := range match.GetBindings() {
				if b.GetObject() != nil {
					resources = append(resources, b.GetObject())
				}
			}
		}
	}
	for _, stage := range o.GetPriorStages() {
		for _, m := range stage.GetMitigations() {
			if m.GetPlaybookId() == id {
				for _, b := range m.GetBindings() {
					if b.GetObject() != nil {
						resources = append(resources, b.GetObject())
					}
				}
			}
		}
	}
	for _, ref := range resources {
		if slices.ContainsFunc(a.GetResources(), func(other *contracts.ObjectRef) bool { return equalCauseResource(ref, other) }) {
			return true
		}
		for _, owner := range a.GetOwners() {
			if slices.ContainsFunc(owner.GetOwnerChain(), func(other *contracts.ObjectRef) bool { return equalCauseResource(ref, other) }) {
				return true
			}
		}
	}
	return false
}
