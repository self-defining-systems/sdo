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

// stageOutOfReachResult is a matched out-of-reach diagnosis: the stage wrote
// nothing for it and hands the incident to the agent at once, with the verdict
// in its report as facts. SDO never escalates.
const stageOutOfReachResult = "out_of_reach"

// stageOutOfReach decides whether a matched diagnosis is out of reach. Only a
// verdict whose Reach is out-of-reach is: a Diagnose-only playbook on it, and
// a playbook with a mitigation whose Mitigate returns an Escalate plan (the
// SDK name of "hand this to the agent"). Any other plan proceeds through
// ordinary planning.
func stageOutOfReach(ctx context.Context, mitigator program.Mitigator, canMitigate bool, view program.ReadView, bindings program.Bindings, verdict program.Verdict, deadline time.Duration) (program.Escalate, bool, error) {
	if !verdict.CanEscalate() {
		return program.Escalate{}, false, nil
	}
	if !canMitigate {
		escalation := program.Escalate{Reason: fmt.Sprintf("Diagnose matched an out-of-reach %s cause", verdict.CauseClass)}
		for _, evidence := range verdict.Evidence {
			if evidence.Resource.Kind != "" && !slices.Contains(escalation.Resources, evidence.Resource) {
				escalation.Resources = append(escalation.Resources, evidence.Resource)
			}
		}
		return escalation, true, nil
	}
	plan, err := BuildMitigationPlan(ctx, mitigator, view, bindings, deadline)
	if err != nil {
		return program.Escalate{}, false, err
	}
	if err := plan.Validate(); err != nil {
		return program.Escalate{}, false, &NonExecutablePlanError{Cause: err}
	}
	escalation, ok := program.EscalationOf(plan)
	return escalation, ok, nil
}

// stageOutOfReachIn reports whether any automatic-stage attempt matched an
// out-of-reach diagnosis.
func stageOutOfReachIn(stages []*contracts.StageReport) bool {
	return slices.ContainsFunc(stages, func(report *contracts.StageReport) bool {
		return slices.ContainsFunc(report.GetMitigations(), func(attempt *contracts.StageMitigation) bool { return attempt.GetResult() == stageOutOfReachResult })
	})
}

// recordOutOfReachAttempt records the out-of-reach diagnosis and removes its
// targeted findings from the stage's remaining work, so no later program
// repairs a cause out of the namespace's reach. The findings stay in the
// incident request: the agent works them and closure observes them.
func recordOutOfReachAttempt(state *IncidentState, attempt *contracts.StageMitigation, match stageCandidate, bindings program.Bindings, verdict program.Verdict, escalation program.Escalate) {
	attempt.Result = stageOutOfReachResult
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

// outOfReachCause is the confirmed out-of-reach cause of an escalated attempt:
// the resources its verdict and Escalate named plus the resources of the
// opening findings it explained, with those findings as detector evidence.
func outOfReachCause(attempt *contracts.StageMitigation, findings []sdk.Finding) (ConfirmedRootCause, bool) {
	if attempt.GetResult() != stageOutOfReachResult || attempt.GetEscalation() == nil {
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
