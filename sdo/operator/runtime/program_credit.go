package runtime

import (
	"slices"
	"sort"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

// ComponentRole is the one role a learned playbook's credited key is bound
// through: the workload the playbook fixed.
const ComponentRole = "component"

// CreditKey is one (symptom, component) pair a playbook was credited for: the
// alert an incident opened on and the component the playbook's component role
// bound when the playbook's own application cleared it.
type CreditKey struct {
	Symptom   string
	Component sdk.ObjectRef
}

// ProgramCredits maps a playbook ID to the pairs it earned. It is derived from
// durable outcomes at load time and never stored, so no later write to a
// responder-owned file is needed and a playbook gains a pair only through a
// credited success.
type ProgramCredits map[string][]CreditKey

// Has reports whether the playbook earned any pair. A playbook without pairs
// keeps the finding binding.
func (c ProgramCredits) Has(id string) bool { return len(c[id]) > 0 }

// Credited reports whether (symptom, component) is in the playbook's key.
func (c ProgramCredits) Credited(id, symptom string, component sdk.ObjectRef) bool {
	return slices.ContainsFunc(c[id], func(key CreditKey) bool {
		return key.Symptom == symptom && sameCreditComponent(key.Component, component)
	})
}

func sameCreditComponent(a, b sdk.ObjectRef) bool {
	return a.Kind == b.Kind && a.Namespace == b.Namespace && a.Name == b.Name
}

// ProgramCreditIndex derives credited pairs from outcomes. Only a `success`
// closure earns credit, and only for a playbook application that was the final
// write before Recovered: a recovery in which another change came last is
// coincidence, so it never widens a key. A pair needs the component role bound,
// so a playbook that predates the role has no pairs until an incident credits it.
func ProgramCreditIndex(outcomes []*contracts.OutcomeRecord) ProgramCredits {
	credits := ProgramCredits{}
	for _, outcome := range outcomes {
		if outcome.GetClassification() != "success" {
			continue
		}
		for _, application := range finalApplications(outcome) {
			if len(application.GetProgramIds()) != 1 || !outcomeAllowsAutomaticCredit(outcome, application.GetProgramIds()[0]) {
				// A batched application cannot attribute recovery to one playbook.
				continue
			}
			id := application.GetProgramIds()[0]
			if id == SourceRestoreID {
				// Built in, never learned: no pair is credited for it.
				continue
			}
			component, ok := applicationComponent(outcome, application, id)
			if !ok {
				continue
			}
			for _, symptom := range outcome.GetRecoveryVerification().GetOpeningKeys() {
				if !credits.Credited(id, symptom, component) {
					credits[id] = append(credits[id], CreditKey{Symptom: symptom, Component: component})
				}
			}
		}
	}
	for id := range credits {
		sort.Slice(credits[id], func(i, j int) bool {
			a, b := credits[id][i], credits[id][j]
			if a.Symptom != b.Symptom {
				return a.Symptom < b.Symptom
			}
			return bindingKeyRef(a.Component) < bindingKeyRef(b.Component)
		})
	}
	return credits
}

func bindingKeyRef(ref sdk.ObjectRef) string { return ref.Kind + "/" + ref.Namespace + "/" + ref.Name }

// finalApplications are the applications that may earn a credited pair: the
// final application and, when it is the automatic stage's, every other stage
// application of that report that verified with no later write other than the
// stage's own. Each stage application is credited on its own settlement: its
// own Diagnose, never the batch it shared.
func finalApplications(outcome *contracts.OutcomeRecord) []*contracts.PlanApplication {
	last := finalApplication(outcome)
	if last == nil {
		return nil
	}
	stage := map[string]bool{}
	for _, attempt := range deterministicStageReport(outcome.GetPriorStages()).GetMitigations() {
		if id := attempt.GetApplyReceipt().GetApplicationId(); id != "" {
			stage[id] = true
		}
	}
	result := []*contracts.PlanApplication{last}
	if !stage[last.GetApplicationId()] {
		return result
	}
	var stageApps []*contracts.PlanApplication
	for _, application := range outcome.GetPlanApplications() {
		if stage[application.GetApplicationId()] {
			stageApps = append(stageApps, application)
		}
	}
	for _, application := range stageApps {
		if application == last || !verifiedApplication(application) {
			continue
		}
		if !laterWrites(outcome, application, func(receipt *contracts.RepairActionReceipt) bool {
			return slices.ContainsFunc(stageApps, func(own *contracts.PlanApplication) bool { return validate.ApplicationOwnsRepair(own, receipt) })
		}) {
			result = append(result, application)
		}
	}
	return result
}

func verifiedApplication(application *contracts.PlanApplication) bool {
	return !application.GetDryRun() && application.GetApplied() && application.GetCompletedAt() != nil &&
		application.GetError() == "" && !application.GetRolledBack() && application.GetRollbackError() == "" &&
		application.GetVerificationCompleted() && application.GetVerificationPassed() &&
		application.GetSettlementRejectionReason() == contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED
}

// laterWrites reports a recorded write that own does not claim and that
// started after application completed.
func laterWrites(outcome *contracts.OutcomeRecord, application *contracts.PlanApplication, own func(*contracts.RepairActionReceipt) bool) bool {
	for _, receipt := range outcome.GetRepairActions() {
		// A failed receipt with a reason wrote nothing that could have recovered users.
		if receipt.GetStartedAt() == nil || (!receipt.GetSuccess() && receipt.GetFailureReason() != "") || own(receipt) {
			continue
		}
		if receipt.GetStartedAt().AsTime().After(application.GetCompletedAt().AsTime()) {
			return true
		}
	}
	return false
}

// finalApplication is the last applied plan application, when it verified and
// no write that is not its own started after it completed.
func finalApplication(outcome *contracts.OutcomeRecord) *contracts.PlanApplication {
	var last *contracts.PlanApplication
	for _, application := range outcome.GetPlanApplications() {
		if application.GetDryRun() || !application.GetApplied() || application.GetCompletedAt() == nil {
			continue
		}
		if last == nil || application.GetCompletedAt().AsTime().After(last.GetCompletedAt().AsTime()) {
			last = application
		}
	}
	if last == nil || !verifiedApplication(last) || laterWrites(outcome, last, func(receipt *contracts.RepairActionReceipt) bool {
		return validate.ApplicationOwnsRepair(last, receipt)
	}) {
		return nil
	}
	return last
}

// applicationComponent is the object the application's playbook bound to its
// component role: from the stage attempt that carries the application, else
// from the one binding the incident's coverage recorded for the playbook.
func applicationComponent(outcome *contracts.OutcomeRecord, application *contracts.PlanApplication, id string) (sdk.ObjectRef, bool) {
	var bindings [][]*contracts.Binding
	for _, stage := range outcome.GetPriorStages() {
		for _, mitigation := range stage.GetMitigations() {
			if mitigation.GetPlaybookId() == id && mitigation.GetApplyReceipt().GetApplicationId() == application.GetApplicationId() {
				bindings = append(bindings, mitigation.GetBindings())
			}
		}
	}
	if len(bindings) == 0 {
		for _, match := range outcome.GetPlaybookCoverage().GetMatches() {
			if match.GetPlaybookId() == id {
				bindings = append(bindings, match.GetBindings())
			}
		}
	}
	if len(bindings) != 1 {
		return sdk.ObjectRef{}, false
	}
	for _, binding := range bindings[0] {
		if binding.GetName() == ComponentRole && binding.GetObject().GetName() != "" {
			ref := binding.GetObject()
			return sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()}, true
		}
	}
	return sdk.ObjectRef{}, false
}

// outcomeCaseContext is what Go writes into a corpus case's labels: the alerts
// the incident opened on and the resources its findings named.
func outcomeCaseContext(outcome *contracts.OutcomeRecord) memory.CaseContext {
	result := memory.CaseContext{OpeningKeys: slices.Clone(outcome.GetRecoveryVerification().GetOpeningKeys())}
	add := func(ref *contracts.ObjectRef) {
		if ref.GetName() == "" {
			return
		}
		suspect := sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()}
		if !slices.ContainsFunc(result.Suspects, func(seen sdk.ObjectRef) bool { return sameCreditComponent(seen, suspect) }) {
			result.Suspects = append(result.Suspects, suspect)
		}
	}
	for _, finding := range outcome.GetFindings() {
		add(finding.GetPrimaryResource())
		for _, related := range finding.GetRelatedResources() {
			add(related)
		}
	}
	return result
}

// automaticCreditApplications are the stage applications eligible for
// attribution under the controller-owned stage protocol. A handoff at the
// deadline, a reported change before Recovered, an unsettled rollout or a
// write that is not the stage's makes attribution ambiguous, and then none is
// eligible. With several, each is credited only on its own evidence: recovery
// settlement requires its own fresh post-apply Diagnose to return NoMatch, so
// a plan that only shared a clearing batch earns nothing.
func automaticCreditApplications(report *contracts.StageReport, applications []*contracts.PlanApplication, changes []*contracts.AgentChange, recovered time.Time) map[string]bool {
	// An early handoff leaves attribution to the agent's changes below: an alert
	// set that cleared without one was the program's repair, shown late.
	exit := report.GetRecoveryWait().GetExitReason()
	early := len(report.GetHandoffKeys()) > 0 && exit == contracts.RecoveryWait_EXIT_REASON_UNCOVERED
	if recovered.IsZero() || !early && (report.GetHandoffSelected() || exit != contracts.RecoveryWait_EXIT_REASON_CLEARED) {
		return nil
	}
	for _, change := range changes {
		if !change.GetAt().IsValid() || !change.GetAt().AsTime().After(recovered) {
			return nil
		}
	}
	ids := map[string]bool{}
	for _, attempt := range report.GetMitigations() {
		app := attempt.GetApplyReceipt()
		if stageReceiptWrote(app) && !app.GetRolledBack() {
			if app.GetApplicationId() == "" || attempt.GetRolloutUnsettledAtDeadline() {
				return nil
			}
			ids[app.GetApplicationId()] = true
		}
	}
	if len(ids) == 0 {
		return nil
	}
	for _, app := range applications {
		if stageReceiptWrote(app) && !app.GetRolledBack() && !ids[app.GetApplicationId()] {
			return nil
		}
	}
	return ids
}

func outcomeAllowsAutomaticCredit(outcome *contracts.OutcomeRecord, playbook string) bool {
	report := deterministicStageReport(outcome.GetPriorStages())
	if report.GetApplyCompletedAt() == nil {
		// Historical receipts retain their original interpretation.
		return true
	}
	automatic := slices.ContainsFunc(report.GetMitigations(), func(attempt *contracts.StageMitigation) bool {
		return attempt.GetPlaybookId() == playbook && stageReceiptWrote(attempt.GetApplyReceipt())
	})
	if !automatic {
		return true
	}
	if outcome.GetClassification() != "success" || !outcome.GetRecoveryVerification().GetWritesSettled() {
		return false
	}
	eligible := automaticCreditApplications(report, outcome.GetPlanApplications(), outcome.GetAgentChanges(), controllerTime(outcome.GetTimestamps().GetVerifiedAt()))
	return slices.ContainsFunc(report.GetMitigations(), func(attempt *contracts.StageMitigation) bool {
		return attempt.GetPlaybookId() == playbook && eligible[attempt.GetApplyReceipt().GetApplicationId()]
	})
}
