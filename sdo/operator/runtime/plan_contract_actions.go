package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"sdo.dev/operator/sdk/program"
)

// applyContractActions applies the actions that write nothing: an Escalate
// plan records its report only, and trailing ProposeSourceChange actions
// record their resolved proposals after the plan's writes complete. handled is
// false for a plan with neither.
func (e PlanExecutor) applyContractActions(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (handled bool, result PlanApplicationResult, err error) {
	_, escalation := program.EscalationOf(plan)
	first := len(plan.Actions)
	for i, action := range plan.Actions {
		if _, ok := program.SourceChangeAction(action); ok {
			first = i
			break
		}
	}
	if !escalation && first == len(plan.Actions) {
		return false, PlanApplicationResult{}, nil
	}
	result = PlanApplicationResult{Plan: plan, DryRun: dryRun, ActionReports: newPlanReports(plan)}
	if err := plan.Validate(); err != nil {
		return true, result, fmt.Errorf("validate plan: %w", err)
	}
	if escalation {
		// Nothing is written: the deterministic stage or the responder closes
		// the incident as escalated from the matched out-of-reach diagnosis.
		if !dryRun {
			result.ActionsApplied = append(result.ActionsApplied, plan.Actions...)
			result.ActionReports[0].Status = "applied"
		}
		return true, result, nil
	}
	in := e.valueInputs(ctx, incident)
	proposals := make([]program.SourceChangeProposal, 0, len(plan.Actions)-first)
	reports := make([]PlanActionReport, 0, len(plan.Actions)-first)
	for i, action := range plan.Actions[first:] {
		change, _ := program.SourceChangeAction(action)
		if incident.Namespace != "" && change.Ref.Namespace != incident.Namespace {
			return true, result, fmt.Errorf("proposed source change of %s/%s is outside incident namespace %q", change.Ref.Namespace, change.Ref.Name, incident.Namespace)
		}
		if e.Source != nil {
			if _, err := acceptedIncidentManifest(ctx, e.Source, incident.SourceCommit, incident.Namespace, change.Ref); err != nil {
				return true, result, fmt.Errorf("proposed source change of %s %s: the object is not in accepted source: %w", change.Ref.Kind, change.Ref.Name, err)
			}
		}
		value, provenance, err := program.ResolveValue(change.Value, in)
		if err != nil {
			return true, result, fmt.Errorf("proposed source change of %s %s: %w", change.Ref.Kind, change.Ref.Name, err)
		}
		proposal := program.SourceChangeProposal{Ref: change.Ref, Path: change.Path, Value: value, Reason: change.Reason}
		proposals = append(proposals, proposal)
		reports = append(reports, PlanActionReport{ActionIndex: first + i, Status: "applied", Values: []program.ValueProvenance{provenance}, SourceChange: &proposals[len(proposals)-1]})
	}
	if first > 0 {
		writes := program.Plan{Actions: plan.Actions[:first], Rationale: plan.Rationale}
		result, err = e.Apply(ctx, incident, writes, dryRun)
		result.Plan = plan
		if err != nil || dryRun {
			for i := first; i < len(plan.Actions); i++ {
				result.ActionReports = append(result.ActionReports, PlanActionReport{ActionIndex: i, Status: "prepared"})
			}
			if err != nil {
				for i := first; i < len(plan.Actions); i++ {
					result.ActionReports[i].Status = "refused"
				}
			}
			return true, result, err
		}
		result.ActionReports = append(result.ActionReports[:first], reports...)
	} else {
		if dryRun {
			return true, result, nil
		}
		result.ActionReports = reports
	}
	if !dryRun {
		result.ActionsApplied = append(result.ActionsApplied, plan.Actions[first:]...)
	}
	return true, result, nil
}

// outcomeSourceProposals lists the applied ProposeSourceChange records of an
// outcome's plan applications, in order.
func outcomeSourceProposals(record *contracts.OutcomeRecord) []program.SourceChangeProposal {
	var out []program.SourceChangeProposal
	for _, application := range record.GetPlanApplications() {
		if !application.GetVerificationPassed() && application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH {
			continue
		}
		for _, report := range application.GetActionReports() {
			if report.GetStatus() != "applied" || len(report.GetSourceChangeJson()) == 0 {
				continue
			}
			var proposal program.SourceChangeProposal
			if json.Unmarshal(report.GetSourceChangeJson(), &proposal) == nil {
				out = append(out, proposal)
			}
		}
	}
	return out
}
