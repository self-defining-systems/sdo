package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/planwire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// finalApplicationVerified reports durable proof that an application's final
// effect is confirmed: an exact final-field match of the objects it wrote, or
// an observed transition. The transition basis requires a non-object effect
// (see planwire.NonObjectEffect) whose every action recorded its own success.
func finalApplicationVerified(app *contracts.PlanApplication) bool {
	if app.GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
		return false
	}
	if app.GetFinalEquivalenceVerified() {
		return true
	}
	return app.GetFinalVerificationBasis() == "observed-transition" && nonObjectEffectsSucceeded(app.GetPlan().GetActions(), app.GetActionReports())
}

// nonObjectEffectsSucceeded reports whether every action with a non-object
// effect recorded its own success (exit status, ensured observation, verified
// catalog read-back, completed Job or script) and at least one of them ran.
// The other half of the proof, the program's diagnosis turning from MATCH (the
// plan's precondition) to NO_MATCH after the action, is the settlement's.
func nonObjectEffectsSucceeded(actions []*contracts.Action, reports []*contracts.ActionReport) bool {
	ran := false
	for index, action := range actions {
		if !planwire.NonObjectEffect(action) {
			continue
		}
		if index >= len(reports) {
			return false
		}
		effected, ok := nonObjectActionSucceeded(action, reports[index])
		if !ok {
			return false
		}
		ran = ran || effected
	}
	return ran
}

// nonObjectActionSucceeded checks one action's own success evidence; effected
// is false for an ensure whose observation already held, which ran nothing.
func nonObjectActionSucceeded(action *contracts.Action, report *contracts.ActionReport) (effected, ok bool) {
	if report.GetStatus() != "applied" || report.GetFailureReason() != "" {
		return false, false
	}
	expect := int32(0)
	switch kind := action.GetKind().(type) {
	case *contracts.Action_RunScript:
		var evidence ScriptExecutionResult
		if report.ExitCode == nil || report.GetExitCode() != 0 || json.Unmarshal(report.GetScriptResultJson(), &evidence) != nil || evidence.JobUID == "" || evidence.ScriptDigest != program.ScriptDigest(kind.RunScript.GetScript()) || evidence.TimedOut {
			return false, false
		}
		return true, true
	case *contracts.Action_Exec:
		return true, report.ExitCode != nil && report.GetExitCode() == 0
	case *contracts.Action_EnsureDatabaseUser:
		// The executor reports applied only after a direct catalog read-back.
		return true, true
	case *contracts.Action_StoreBackup, *contracts.Action_RepairData, *contracts.Action_RestoreBackup, *contracts.Action_InvalidateDerived:
		// The executor reports applied only after the store's own end marker.
		return true, true
	case *contracts.Action_RunJob:
		return true, len(report.GetJobStatusJson()) > 0
	case *contracts.Action_EnsureExec:
		expect = kind.EnsureExec.GetExpect()
	case *contracts.Action_EnsureHttp:
		expect = kind.EnsureHttp.GetExpect()
	default:
		return false, false
	}
	if len(report.GetExternalEffectJson()) == 0 {
		return false, report.ExitCode == nil // observed at Expect already: nothing ran
	}
	var effect program.ExternalEffect
	if json.Unmarshal(report.GetExternalEffectJson(), &effect) != nil {
		return false, false
	}
	return true, effect.ExitCode == 0 && effect.Before != expect && effect.After == expect
}

func applicationActionResources(app *contracts.PlanApplication, index int, action *contracts.Action) ([]sdk.ObjectRef, error) {
	if action.GetRunScript() != nil {
		if index >= len(app.GetActionReports()) {
			return nil, fmt.Errorf("script action report unavailable")
		}
		var evidence ScriptExecutionResult
		if err := json.Unmarshal(app.GetActionReports()[index].GetScriptResultJson(), &evidence); err != nil {
			return nil, err
		}
		return scriptRepairResources(PlanActionReport{Script: &evidence}), nil
	}
	ref, err := planActionRef(planActionState(action))
	if err != nil {
		return nil, err
	}
	return []sdk.ObjectRef{ref}, nil
}

// finalTransitionApplication verifies a non-object application's recorded
// invocation bindings again on fresh state. It never projects opaque effects
// or invents post-state.
func (l *ResponderLoop) finalTransitionApplication(ctx context.Context, app RecordedPlanApplication, view program.ReadView, accepted ...selectedPlan) bool {
	if !app.Result.Applied || !app.Result.VerificationCompleted || !app.Result.VerificationPassed || app.Result.DryRun || app.RolledBack || app.Error != "" || app.RollbackError != "" || len(app.ProgramIDs) == 0 || l.Programs == nil {
		return false
	}
	wire, err := app.Result.Proto()
	if err != nil {
		return false
	}
	wire.ApplicationId = app.ID
	wire.FinalVerificationBasis = "observed-transition"
	if !finalApplicationVerified(wire) {
		return false
	}
	outcome := &contracts.OutcomeRecord{}
	for _, call := range l.calls {
		outcome.ProgramToolCalls = append(outcome.ProgramToolCalls, &contracts.ProgramToolCall{Command: call.Command, RequestJson: call.RequestJSON, ResponseJson: call.ResponseJSON})
	}
	for _, id := range app.ProgramIDs {
		diagnoser, ok := l.Programs.Diagnoser(id)
		if !ok {
			return false
		}
		bindings := appliedPlanBindings(outcome, wire, id)
		if len(bindings) == 0 {
			for _, plan := range accepted {
				if plan.programID == id && plan.revision != "" && plan.revision == l.programRevision(id) {
					bindings = append(bindings, plan.bindings)
				}
			}
		}
		if len(bindings) == 0 {
			return false
		}
		for _, bound := range bindings {
			if verdict := program.RunDiagnose(ctx, diagnoser, view, bound, l.deadline()); verdict.Result != program.NoMatch {
				return false
			}
		}
	}
	var targets []sdk.Finding
	fingerprints := map[string]bool{}
	for _, report := range app.Result.ActionReports {
		if report.Script != nil {
			for _, fp := range report.Script.TargetFindings {
				fingerprints[fp] = true
			}
		}
	}
	for _, finding := range l.Request.Findings {
		if fingerprints[FindingFingerprint(finding)] {
			targets = append(targets, finding)
		}
	}
	verifier := scopedMitigationVerifier(l.Executor.Verifier, app.Result.Plan, targets)
	if verifier == nil {
		return false
	}
	clear, err := verifier.Verify(ctx)
	return err == nil && clear
}

// recordScriptInvocation normalizes the real menu invocation into the existing
// plan/application evidence contract. These are controller-owned provenance
// records of the executed script, not simulated actions or model tool claims.
func (l *ResponderLoop) recordScriptInvocation(app RecordedPlanApplication, id string, bindings program.Bindings, verdict program.Verdict) {
	planID := app.ID + "-script-invocation"
	planRequest, _ := json.Marshal(ToolRequest{Command: "plan", ProgramID: id, Bindings: bindings})
	planResponse, _ := json.Marshal(ToolResponse{Command: "plan", PlanID: planID})
	applyRequest, _ := json.Marshal(ToolRequest{Command: "apply", PlanIDs: []string{planID}})
	run := ProgramRun{ProgramID: id, Bindings: bindings, Verdict: verdict}
	applyResponse, _ := json.Marshal(ToolResponse{Command: "apply", ApplicationID: app.ID, ProgramRuns: []ProgramRun{run}})
	l.calls = append(l.calls, ToolCall{Command: "plan", At: app.StartedAt, RequestJSON: string(planRequest), ResponseJSON: string(planResponse), Summary: "controller-recorded script menu invocation"}, ToolCall{Command: "apply", At: app.CompletedAt, RequestJSON: string(applyRequest), ResponseJSON: string(applyResponse), Summary: "controller-recorded script execution and bound diagnosis"})
}
