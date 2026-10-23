package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// Session identity and reports are presentation state, never mutation authority.
func (l *ResponderLoop) openDirectMenuSession(response *ToolResponse) {
	if l.menuSession == nil {
		l.menuSession = &contracts.MitigationSession{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, DispatchEpoch: 1, OriginalDeadline: timestamppb.New(l.Request.ResponseDeadline)}
	}
	response.Session = proto.Clone(l.menuSession).(*contracts.MitigationSession)
	response.Summary = "responder session"
}

func (l *ResponderLoop) listMenu(ctx context.Context, request ToolRequest, response *ToolResponse) {
	if l.MenuConfig == nil {
		response.Error = "mitigation menu is disabled"
		return
	}
	l.openDirectMenuSession(response)
	entries := rawMenuActions()
	if l.Programs != nil {
		for _, spec := range l.Programs.List() {
			if request.ProgramID != "" && request.ProgramID != spec.ID {
				continue
			}
			if _, ok := l.Programs.Mitigator(spec.ID); !ok {
				continue
			}
			bindings := []program.Bindings{}
			if len(request.Bindings) > 0 {
				bindings = append(bindings, request.Bindings)
			} else {
				for _, finding := range l.Request.Findings {
					bindings = append(bindings, candidateBindings(spec, finding)...)
				}
			}
			if len(bindings) == 0 {
				bindings = append(bindings, program.Bindings{})
			}
			for _, binding := range bindings {
				id := "program-" + menuDigest([]any{spec.ID, binding})
				entries[id] = menuEntry{spec: spec, programID: spec.ID, bindings: cloneProgramBindings(binding)}
			}
		}
	}
	for _, plan := range l.MenuConfig.Plans {
		p := plan.Plan
		entries[plan.ID] = menuEntry{programID: plan.ProgramID, bindings: cloneProgramBindings(plan.Bindings), plan: &p}
	}
	menu := &contracts.MitigationMenu{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, DispatchEpoch: 1, SourceCommit: l.Request.SourceCommit, ScopeDigest: "direct", PolicyDigest: "direct", RoundLimit: uint32(max(0, min(5, l.MaxBatches)-l.batches)), RemainingBatches: uint32(max(0, min(5, l.MaxBatches)-l.batches)), OriginalDeadline: timestamppb.New(l.Request.ResponseDeadline)}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	response.MenuParameters = map[string][]MenuParameter{}
	for _, id := range ids {
		entry := entries[id]
		if entry.action != nil {
			response.MenuParameters[id] = menuActionParameters(entry.action)
		} else {
			response.MenuParameters[id] = menuProgramParameters(entry.spec)
		}
		menu.Options = append(menu.Options, &contracts.MitigationOption{OptionId: id, ProgramId: entry.programID, ProgramRevision: l.Request.SourceCommit, DependencyRevision: l.Request.SourceCommit, Effects: []contracts.MitigationOption_Effect{contracts.MitigationOption_EFFECT_SDK_PLAN_ACTION}, MaxActionsPerRound: 32, Bindings: menuBindings(entry.bindings)})
	}
	menu.MenuId = "menu-" + menuDigest(menu)
	l.menu = menu
	l.menuEntries = entries
	response.Menu = proto.Clone(menu).(*contracts.MitigationMenu)
	response.Summary = fmt.Sprintf("%d mitigation options", len(menu.Options))
}

// Invoke the ordinary plan executor, under its existing credentials. Selection
// metadata is descriptive; no ledger, fencing, provenance or menu preflight runs.
func (l *ResponderLoop) mitigateMenu(ctx context.Context, selection *contracts.MitigationSelection, parameters map[string]string, response *ToolResponse) {
	if l.MenuConfig == nil {
		response.Error = "mitigation menu is disabled"
		return
	}
	if selection == nil {
		response.Error = "mitigate requires an option"
		return
	}
	entry, ok := l.menuEntries[selection.OptionId]
	if !ok {
		response.Error = "unknown mitigation option"
		return
	}
	attempt := fmt.Sprintf("mitigation-%s-%d", l.sessionID, len(l.menuReports)+1)
	report := &contracts.MitigationReport{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, RequestId: selection.RequestId, AttemptId: attempt, AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_UNRESOLVED}
	rounds := selection.RoundLimit
	if rounds == 0 {
		rounds = 1
	}
	if rounds > 5 {
		rounds = 5
	}
	if rounds > uint32(max(0, min(5, l.MaxBatches)-l.batches)) {
		response.Error = "response loop budget exhausted"
		return
	}
	for round := uint32(1); round <= rounds; round++ {
		rr := &contracts.MitigationRoundReport{RoundIndex: round, RoundId: fmt.Sprintf("%s-r%d", attempt, round)}
		report.Rounds = append(report.Rounds, rr)
		l.batches++
		plan := program.Plan{}
		var err error
		if entry.action != nil {
			plan, err = menuRawPlan(entry.action, parameters)
		} else if entry.plan != nil {
			plan = *entry.plan
		} else {
			mitigator, found := l.Programs.Mitigator(entry.programID)
			if !found {
				report.Status = contracts.MitigationReport_STATUS_EXECUTION_ERROR
				report.FailedPhase = "planning"
				report.ReasonCode = "mitigator_unavailable"
				break
			}
			view, viewErr := l.snapshotView(ctx)
			if viewErr != nil {
				err = viewErr
			} else {
				plan, err = l.runMitigator(ctx, entry.programID, mitigator, view, entry.bindings)
			}
		}
		if err != nil {
			report.Status = contracts.MitigationReport_STATUS_EXECUTION_ERROR
			report.FailedPhase = "planning"
			report.ReasonCode = "direct_plan_failed"
			rr.Verification = menuErrorVerdict(err)
			break
		}
		started := time.Now().UTC()
		result, applyErr := l.Executor.Apply(ctx, l.Request, plan, false)
		completed := time.Now().UTC()
		for index, patch := range result.Patches {
			receipt := menuActionReceipt(menuActionID(attempt, round, index+1), patch.Ref)
			receipt.State = contracts.MitigationActionReceipt_STATE_NOT_APPLIED
			if index < len(result.ActionsApplied) {
				receipt.State = contracts.MitigationActionReceipt_STATE_APPLIED
			}
			if result.PendingAction != nil && index == len(result.ActionsApplied) {
				receipt.State = contracts.MitigationActionReceipt_STATE_UNKNOWN
			}
			if result.RolledBack {
				receipt.CompensationState = contracts.MitigationActionReceipt_STATE_APPLIED
			}
			receipt.ActionKind = patch.Operation
			if receipt.ActionKind == "" {
				receipt.ActionKind = "sdk-plan-patch"
			}
			receipt.ChangedFields = menuChangedFields(patch.Patch)
			if patch.After != nil {
				receipt.Uid = string(patch.After.GetUID())
				receipt.ResourceVersion = patch.After.GetResourceVersion()
			}
			rr.Actions = append(rr.Actions, receipt)
			if receipt.State == contracts.MitigationActionReceipt_STATE_APPLIED {
				l.menuRepairs = append(l.menuRepairs, RepairActionReceipt{ActionID: receipt.ActionId, Kind: "mitigation-menu", Target: patch.Ref.Kind + "/" + patch.Ref.Name, Resources: []sdk.ObjectRef{patch.Ref}, Summary: "responder executed SDK plan action", Details: fmt.Sprintf("attempt %s round %d action %d via ordinary PlanExecutor", attempt, round, index+1), Success: !result.RolledBack, StartedAt: started, CompletedAt: completed})
			}
		}
		rr.IndependentlyHealthy = result.VerificationPassed
		rr.OriginalFindingsClear = result.VerificationPassed
		rr.Verification = verdictContract(program.Verdict{Result: program.Unknown})
		if result.VerificationPassed {
			rr.Verification = verdictContract(program.Verdict{Result: program.NoMatch})
			report.Status = contracts.MitigationReport_STATUS_SUCCESS
			l.findings = nil
			break
		}
		report.FailedPhase = "verification"
		report.ReasonCode = "direct_verification_unresolved"
		if applyErr != nil {
			rr.Verification = menuErrorVerdict(applyErr)
			if !result.Applied {
				report.Status = contracts.MitigationReport_STATUS_EXECUTION_ERROR
				report.FailedPhase = "forward"
				report.ReasonCode = "direct_apply_failed"
			}
			break
		}
	}
	report.RemainingBatches = uint32(max(0, min(5, l.MaxBatches)-l.batches))
	l.menuReports = append(l.menuReports, cloneSelectionReport(report))
	response.Report = report
	response.Summary = "mitigation " + report.Status.String()
}

func menuErrorVerdict(err error) *contracts.Verdict {
	return verdictContract(program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "error", Observed: boundedToolText(err.Error())}}})
}
