package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func finalApplicationVerified(app *contracts.PlanApplication) bool {
	if app.GetFinalEquivalenceVerified() {
		return true
	}
	if app.GetFinalVerificationBasis() != "observed-transition" {
		return false
	}
	scripted := false
	for index, action := range app.GetPlan().GetActions() {
		script := action.GetRunScript()
		if script == nil {
			continue
		}
		scripted = true
		if index >= len(app.GetActionReports()) {
			return false
		}
		report := app.GetActionReports()[index]
		if report.GetStatus() != "applied" || report.ExitCode == nil || report.GetExitCode() != 0 {
			return false
		}
		var evidence ScriptExecutionResult
		if json.Unmarshal(report.GetScriptResultJson(), &evidence) != nil || evidence.JobUID == "" || evidence.ScriptDigest != program.ScriptDigest(script.GetScript()) || evidence.TimedOut {
			return false
		}
	}
	return scripted
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

// finalScriptApplication verifies the recorded invocation's own bindings again
// on fresh state. It never projects opaque script effects or invents post-state.
func (l *ResponderLoop) finalScriptApplication(ctx context.Context, app RecordedPlanApplication, view program.ReadView) bool {
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
