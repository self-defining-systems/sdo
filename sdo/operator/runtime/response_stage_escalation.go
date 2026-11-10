package runtime

import (
	"context"
	"fmt"
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// stageEscalated is a matched out-of-reach diagnosis the stage escalated: it
// wrote nothing and handed its findings to closure, which records the
// incident escalated while they still fire.
const stageEscalated = "escalated"

// stageEscalation decides whether a matched diagnosis escalates. Only a
// verdict whose Reach is out-of-reach does: a Diagnose-only playbook escalates
// on it, and a playbook with a mitigation escalates when Mitigate returns an
// Escalate plan. Any other plan proceeds through ordinary planning.
func stageEscalation(ctx context.Context, mitigator program.Mitigator, canMitigate bool, view program.ReadView, bindings program.Bindings, verdict program.Verdict, deadline time.Duration) (program.Escalate, bool) {
	if verdict.Result != program.Match || verdict.Reach != program.ReachOutOfReach || verdict.Validate() != nil {
		return program.Escalate{}, false
	}
	if !canMitigate {
		escalation := program.Escalate{Reason: fmt.Sprintf("Diagnose matched an out-of-reach %s cause", verdict.CauseClass)}
		for _, evidence := range verdict.Evidence {
			if evidence.Resource.Kind != "" && !slices.Contains(escalation.Resources, evidence.Resource) {
				escalation.Resources = append(escalation.Resources, evidence.Resource)
			}
		}
		return escalation, true
	}
	plan, err := BuildMitigationPlan(ctx, mitigator, view, bindings, deadline)
	if err != nil || plan.Validate() != nil {
		return program.Escalate{}, false
	}
	return program.EscalationOf(plan)
}


// escalateStageAttempt records the escalation and removes its targeted
// findings from the stage's remaining work, so no later stage repairs a cause
// out of the namespace's reach. The findings stay in the incident request:
// closure still observes them.
func escalateStageAttempt(state *IncidentState, attempt *contracts.StageMitigation, match stageCandidate, bindings program.Bindings, verdict program.Verdict, escalation program.Escalate) {
	attempt.Result = stageEscalated
	attempt.SkipReason = ""
	attempt.EscalationReason = escalation.Reason
	attempt.EscalatedResources = objectRefsContract(escalation.Resources)
	attempt.Escalation = programRunContract(ProgramRun{ProgramID: attempt.GetPlaybookId(), ProgramRevision: attempt.GetProgramRevision(), Bindings: bindings, Verdict: verdict})
	targets := map[string]bool{}
	for _, fp := range match.targets {
		targets[fp] = true
	}
	remaining := make([]sdk.Finding, 0, len(state.Findings))
	for _, finding := range state.Findings {
		if !targets[FindingFingerprint(finding)] {
			remaining = append(remaining, finding)
		}
	}
	state.Findings = remaining
	state.refreshUnexplained()
	recordStageFindingProgress(attempt, match.targets, state.Findings)
}

// escalatedCause is the confirmed out-of-reach cause of an escalated attempt:
// the resources its verdict and Escalate named plus the resources of the
// opening findings it explained, with those findings as detector evidence.
func escalatedCause(attempt *contracts.StageMitigation, findings []sdk.Finding) (ConfirmedRootCause, bool) {
	if attempt.GetResult() != stageEscalated || attempt.GetEscalation() == nil {
		return ConfirmedRootCause{}, false
	}
	run := programRunState(attempt.GetEscalation())
	if run.Verdict.Reach != program.ReachOutOfReach {
		return ConfirmedRootCause{}, false
	}
	cause := ConfirmedRootCause{Summary: attempt.GetEscalationReason(), CauseClass: run.Verdict.CauseClass, Reach: run.Verdict.Reach}
	add := func(ref sdk.ObjectRef) {
		if ref.Kind != "" && !slices.Contains(cause.Resources, ref) {
			cause.Resources = append(cause.Resources, ref)
		}
	}
	for _, ref := range objectRefsState(attempt.GetEscalatedResources()) {
		add(ref)
	}
	explained := map[string]bool{}
	for _, fp := range attempt.GetClearedFindingFingerprints() {
		explained[fp] = true
	}
	for _, finding := range findings {
		if !explained[FindingFingerprint(finding)] {
			continue
		}
		add(finding.PrimaryResource)
		mergeEarlyCauseEvidence(&cause, ConfirmedRootCause{ExplainedDetectors: []string{finding.DetectorID}, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}}})
	}
	for _, evidence := range run.Verdict.Evidence {
		if evidence.Resource.Kind != "" {
			cause.Evidence = append(cause.Evidence, RootCauseEvidence{Kind: "live-observation", Source: attempt.GetPlaybookId(), Observation: evidence.Fact + ": " + evidence.Observed})
		}
	}
	return cause, len(cause.Resources) > 0 && len(cause.ExplainedDetectors) > 0
}
