package runtime

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/types"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	response.SelectionSchema = menuMessageSchema((&contracts.MitigationSelection{}).ProtoReflect().Descriptor())
	response.SelectionSetSchema = menuMessageSchema((&contracts.MitigationSelectionSet{}).ProtoReflect().Descriptor())
	optionSchema := response.SelectionSetSchema.Properties["options"].Items
	optionSchema.Properties["parameters"].AdditionalProperties = &MenuSchema{AnyOf: []*MenuSchema{{Type: "string"}, {Type: "number"}, {Type: "boolean"}, {Type: "object", AdditionalProperties: true}, {Type: "array", Items: &MenuSchema{}}}, Description: "Shapes are published per option in parameters[].schema."}
	entries := rawMenuActions()
	if capabilities, ok := l.Executor.Client.(interface{ unavailableMenuActions() map[string]bool }); ok {
		for action, unavailable := range capabilities.unavailableMenuActions() {
			if unavailable {
				delete(entries, action)
			}
		}
	}
	if l.MenuConfig.Unexplained != nil {
		assessment, err := l.MenuConfig.Unexplained(ctx)
		if err != nil {
			response.Error = err.Error()
			return
		}
		response.UnexplainedRemovalBlockers = assessment.Blockers
		for _, removal := range assessment.Removals {
			selected := removal
			plan := program.Plan{Actions: []program.Action{program.DeleteObject{Ref: removal.Ref, Reason: "remove current source-absent unexplained object"}}}
			entries["remove-unexplained-"+menuDigest(removal)] = menuEntry{programID: "remove-unexplained", bindings: program.Bindings{Objects: program.ObjectBindings{"resource": removal.Ref}}, plan: &plan, removal: &selected}
		}
	} else {
		for _, finding := range l.Request.Findings {
			if finding.DetectorID == "drift.unexplained" {
				response.UnexplainedRemovalBlockers = append(response.UnexplainedRemovalBlockers, UnexplainedRemovalBlocker{FindingFingerprint: FindingFingerprint(finding), Ref: finding.PrimaryResource, Reason: RemovalUnavailable})
			}
		}
	}
	if l.Programs != nil {
		for _, spec := range l.Programs.List() {
			if request.ProgramID != "" && request.ProgramID != spec.ID {
				continue
			}
			if _, ok := l.Programs.Mitigator(spec.ID); !ok {
				continue
			}
			bindings := []program.Bindings{}
			if request.ProgramID != "" && len(request.Parameters) > 0 {
				bound, err := menuBindProgram(menuEntry{spec: spec, bindings: request.Bindings}, request.Parameters)
				if err == nil {
					err = program.ValidateBindings(spec, bound)
				}
				if err != nil {
					response.Error = fmt.Sprintf("program parameters: %v", err)
					return
				}
				bindings = append(bindings, bound)
			} else if len(request.Bindings.Objects)+len(request.Bindings.Values) > 0 {
				bindings = append(bindings, request.Bindings)
			} else {
				for _, finding := range l.Request.Findings {
					bindings = append(bindings, candidateBindings(spec, finding)...)
				}
				// Namespace-sweep matches can bind resources absent from findings.
				// Offer the exact option identified by incident-open coverage.
				for _, match := range l.Request.PlaybookCoverage.GetMatches() {
					if match.GetPlaybookId() == spec.ID && match.GetCanMitigate() {
						bindings = append(bindings, programBindingsState(match.GetBindings()))
					}
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
	if registry, ok := l.Programs.(*workingProgramRegistry); ok {
		for id, message := range registry.failures {
			if request.ProgramID != "" && request.ProgramID != id {
				continue
			}
			reason := strings.ReplaceAll(message, "\n", " ")
			if len(reason) > 256 {
				reason = reason[:200] + "; fix checkout source and retry"
			}
			entries["program-"+menuDigest([]any{id, program.Bindings{}})] = menuEntry{programID: id, spec: program.ProgramSpec{ID: id}, compileError: reason}
		}
	}
	for _, plan := range l.MenuConfig.Plans {
		p := plan.Plan
		entries[plan.ID] = menuEntry{programID: plan.ProgramID, bindings: cloneProgramBindings(plan.Bindings), plan: &p}
	}
	menu := &contracts.MitigationMenu{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, DispatchEpoch: 1, SourceCommit: l.Request.SourceCommit, ScopeDigest: "direct", PolicyDigest: "direct", RoundLimit: uint32(max(1, min(5, l.MaxBatches)-l.batches)), RemainingBatches: uint32(max(0, min(5, l.MaxBatches)-l.batches)), OriginalDeadline: timestamppb.New(l.Request.ResponseDeadline)}
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
		menu.Options = append(menu.Options, &contracts.MitigationOption{DisabledReason: entry.compileError, OptionId: id, ProgramId: entry.programID, ProgramRevision: l.programRevision(entry.programID), DependencyRevision: l.programRevision(entry.programID), Effects: []contracts.MitigationOption_Effect{contracts.MitigationOption_EFFECT_SDK_PLAN_ACTION}, MaxActionsPerRound: 32, Bindings: menuBindings(entry.bindings)})
	}
	menu.MenuId = "menu-" + menuDigest(menu)
	l.menu = menu
	l.menuEntries.advertise(entries)
	response.Menu = proto.Clone(menu).(*contracts.MitigationMenu)
	response.Summary = fmt.Sprintf("%d mitigation options; apply several independent options in one call; overlapping object write sets are refused before writes", len(menu.Options))
}

// Invoke the ordinary plan executor, under its existing credentials. Selection
// metadata is descriptive; no ledger, fencing, provenance or menu preflight runs.
func (l *ResponderLoop) mitigateMenu(ctx context.Context, selection *contracts.MitigationSelection, parameters map[string]string, targetFingerprints []string, response *ToolResponse) {
	l.executeMenuOption(ctx, selection, parameters, targetFingerprints, response, true)
}

func (l *ResponderLoop) executeMenuOption(ctx context.Context, selection *contracts.MitigationSelection, parameters map[string]string, targetFingerprints []string, response *ToolResponse, consumeRound bool) {
	if l.MenuConfig == nil {
		response.Error = "mitigation menu is disabled"
		return
	}
	if selection == nil {
		response.Error = "mitigate requires an option"
		return
	}
	entry, ok := l.menuEntries.lookup(selection.OptionId)
	if !ok {
		response.Error = "unknown mitigation option"
		return
	}
	l.executeMenuEntry(ctx, entry, selection, parameters, targetFingerprints, response, consumeRound)
}

// Prepared batch entries are local execution state, never catalog mutations.
func (l *ResponderLoop) executeMenuEntry(ctx context.Context, entry menuEntry, selection *contracts.MitigationSelection, parameters map[string]string, targetFingerprints []string, response *ToolResponse, consumeRound bool) {
	if entry.action == nil && entry.plan == nil {
		if response.ProgramRevisions == nil {
			response.ProgramRevisions = map[string]string{}
		}
		response.ProgramRevisions[entry.programID] = l.programRevision(entry.programID)
	}
	bindings, bindingErr := menuBindProgram(entry, parameters)
	attempt := fmt.Sprintf("mitigation-%s-%d", l.sessionID, len(l.menuReports)+1)
	requestID := selection.RequestId
	if requestID == "" {
		requestID = attempt
	}
	report := &contracts.MitigationReport{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, RequestId: requestID, AttemptId: attempt, AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_UNRESOLVED, OptionId: selection.OptionId}
	rounds := selection.RoundLimit
	if rounds == 0 {
		rounds = 1
	}
	if rounds > 5 {
		rounds = 5
	}
	if consumeRound && rounds > uint32(max(0, min(5, l.MaxBatches)-l.batches)) {
		response.Error = "response loop budget exhausted"
		return
	}
	var followUp *program.Plan
	var planned []program.Plan
	for round := uint32(1); round <= rounds; round++ {
		rr := &contracts.MitigationRoundReport{RoundIndex: round, RoundId: fmt.Sprintf("%s-r%d", attempt, round)}
		report.Rounds = append(report.Rounds, rr)
		var plan program.Plan
		var err error
		reason := "direct_plan_failed"
		if followUp != nil {
			// A follow-up continues the same selection: no responder batch or
			// Match precondition, but the ordinary executor, receipts, checks
			// and other applications' held reservations.
			plan, followUp = *followUp, nil
			refs, refErr := l.menuPlanWriteRefs(plan)
			var conflict ToolResponse
			if refErr != nil && l.heldPendingWrites() > 0 {
				err, reason = refErr, "write_set_unknown"
			} else if l.pendingConflictExcept(refs, &conflict, attempt) {
				err, reason = errors.New(conflict.Error), "pending_write_conflict"
			}
		} else {
			if consumeRound {
				l.batches++
			}
			// Preserve the ordinary incident counter before a potentially lost reply.
			_ = l.persistToolState(ctx)
			plan, err = l.prepareMenuPlan(ctx, entry, parameters, bindings)
			if bindingErr != nil {
				err = bindingErr
			}
			if err == nil && consumeRound {
				err = l.validateMenuPreconditions(ctx, entry, bindings)
			}
		}
		if err != nil {
			report.Status = contracts.MitigationReport_STATUS_EXECUTION_ERROR
			report.FailedPhase = "planning"
			report.ReasonCode = reason
			if errors.Is(err, program.ErrReplicaSetScaleRefused) {
				report.ReasonCode = "replicaset_scale_refused"
			}
			rr.Verification = verdictContract(program.Verdict{Result: program.Unknown})
			response.MenuExecution = append(response.MenuExecution, MenuRoundExecution{OptionID: selection.OptionId, ProgramID: entry.programID, RoundIndex: round, Error: boundedToolText(err.Error())})
			break
		}
		applicationID := fmt.Sprintf("%s-r%d", attempt, round)
		description := MenuRoundExecution{OptionID: selection.OptionId, ProgramID: entry.programID, RoundIndex: round, ApplicationID: applicationID, Plan: &contracts.Plan{Rationale: plan.Rationale}}
		for _, action := range plan.Actions {
			converted, convertErr := planActionProto(action)
			if convertErr == nil {
				description.Plan.Actions = append(description.Plan.Actions, converted)
			}
		}
		planned = append(planned, plan)
		executor := l.Executor
		executor.ApplicationID = applicationID
		executor.PollRollouts = true
		if entry.removal != nil {
			targetFingerprints = []string{entry.removal.FindingFingerprint}
			executor.DeleteObject = func(ctx context.Context, ref sdk.ObjectRef, uid types.UID, rv string) error {
				if l.MenuConfig.RemoveUnexplained == nil {
					return fmt.Errorf("unexplained removal executor is unavailable")
				}
				return l.MenuConfig.RemoveUnexplained(ctx, *entry.removal, executor.Client.(PlanLifecycleClient), ref, uid, rv)
			}
		}
		started := time.Now().UTC()
		result, applyErr := executor.Apply(ctx, l.mitigationIncident([]selectedPlan{{programID: entry.programID, bindings: bindings, targetFingerprints: targetFingerprints}}), plan, false)
		completed := time.Now().UTC()

		if result.Applied || result.PendingAction != nil {
			l.applications[applicationID] = result
			response.ApplicationID = applicationID
			report.ApplicationId = applicationID
			if result.RolloutPending != nil {
				l.reservePending(applicationID, plan, attempt, entry.programID, bindings, targetFingerprints)
			}
		}
		description.ActionResults = result.ActionReports
		description.ChangesRemain = result.Applied && !result.RolledBack
		description.RolledBack = result.RolledBack
		description.Pending = planApplicationPending(result)
		response.RolloutPending = result.RolloutPending
		if applyErr != nil {
			description.Error = boundedToolText(applyErr.Error())
		}
		response.MenuExecution = append(response.MenuExecution, description)
		for index, patch := range result.Patches {
			receipt := menuActionReceipt(menuActionID(attempt, round, index+1), patch.Ref)
			receipt.State = contracts.MitigationActionReceipt_STATE_NOT_APPLIED
			if index < len(result.ActionsApplied) {
				receipt.State = contracts.MitigationActionReceipt_STATE_APPLIED
			}
			if index < len(result.ActionReports) && result.ActionReports[index].Status != "applied" {
				receipt.State = contracts.MitigationActionReceipt_STATE_NOT_APPLIED
			}
			if result.PendingAction != nil && index == len(result.ActionsApplied) {
				receipt.State = contracts.MitigationActionReceipt_STATE_UNKNOWN
			}
			if index < len(result.ActionReports) && result.ActionReports[index].Status == "unknown" {
				receipt.State = contracts.MitigationActionReceipt_STATE_UNKNOWN
			}
			if result.RolledBack && receipt.State == contracts.MitigationActionReceipt_STATE_APPLIED {
				receipt.CompensationState = contracts.MitigationActionReceipt_STATE_APPLIED
			}
			receipt.ActionKind = patch.Operation
			if receipt.ActionKind == "" {
				receipt.ActionKind = "sdk-plan-patch"
			}
			receipt.ChangedFields = menuChangedFields(patch.Patch)
			if patch.Before != nil {
				receipt.Uid = string(patch.Before.GetUID())
				receipt.ResourceVersion = patch.Before.GetResourceVersion()
			}
			if patch.After != nil {
				receipt.Uid = string(patch.After.GetUID())
				receipt.ResourceVersion = patch.After.GetResourceVersion()
			}
			rr.Actions = append(rr.Actions, receipt)
			if receipt.State == contracts.MitigationActionReceipt_STATE_APPLIED {
				resources := []sdk.ObjectRef{patch.Ref}
				if index < len(result.ActionReports) {
					if scriptRefs := scriptRepairResources(result.ActionReports[index]); len(scriptRefs) > 0 {
						resources = scriptRefs
					}
				}
				l.menuRepairs = append(l.menuRepairs, RepairActionReceipt{ActionID: receipt.ActionId, Kind: "mitigation-menu", Target: patch.Ref.Kind + "/" + patch.Ref.Name, Resources: resources, Summary: "responder executed SDK plan action", Details: fmt.Sprintf("attempt %s round %d action %d via ordinary PlanExecutor", attempt, round, index+1), Success: !result.RolledBack, StartedAt: started, CompletedAt: completed})
			}
		}
		l.observeMitigationHealth(result, response)
		targetVerdict := program.Verdict{Result: program.Unknown}
		if result.MitigationHealth != nil && entry.programID != "" && result.Applied && result.RolloutPending == nil && l.Programs != nil {
			if diagnoser, ok := l.Programs.Diagnoser(entry.programID); ok {
				view, viewErr := l.snapshotView(ctx)
				if viewErr == nil {
					targetVerdict = program.RunDiagnose(ctx, diagnoser, view, bindings, l.deadline())
				}
				run := ProgramRun{ProgramRevision: l.programRevision(entry.programID), ProgramID: entry.programID, Bindings: bindings, Verdict: targetVerdict}
				response.ProgramRuns = append(response.ProgramRuns, run)
				l.runs = append(l.runs, run)
				result.VerificationPassed = result.VerificationPassed && targetVerdict.Result == program.NoMatch
			}
		}
		response.TargetedHealthy = result.VerificationPassed
		rr.IndependentlyHealthy = response.Healthy
		if result.MitigationHealth == nil {
			rr.IndependentlyHealthy = result.VerificationPassed
		}
		rr.Verification = verdictContract(targetVerdict)
		// Preserve program/raw-action attribution in the durable outcome, even
		// when the larger menu response is truncated in the tool-call journal.
		application := RecordedPlanApplication{Pending: planApplicationPending(result), ID: applicationID, ProgramIDs: []string{entry.programID}, Result: result, StartedAt: started, CompletedAt: completed, RolledBack: result.RolledBack, RollbackError: result.RollbackError}
		if applyErr != nil {
			application.Error = applyErr.Error()
		}
		l.applicationsLog = append(l.applicationsLog, application)
		if hasAppliedScript(plan.Actions) && entry.action == nil {
			l.recordScriptInvocation(application, entry.programID, bindings, targetVerdict)
		}
		if result.Applied {
			l.applications[applicationID] = result
		}
		if result.VerificationPassed {
			rr.Verification = verdictContract(program.Verdict{Result: program.NoMatch})
			report.Status = contracts.MitigationReport_STATUS_SUCCESS
			report.FailedPhase = ""
			report.ReasonCode = ""
			break
		}
		if result.RolloutPending != nil {
			report.Status = contracts.MitigationReport_STATUS_UNRESOLVED
			report.FailedPhase = "rollout"
			report.ReasonCode = "rollout_pending"
			next, ok, followErr := l.programFollowUp(ctx, entry, bindings, planned)
			if ok {
				followUp = &next
				rounds++
				continue
			}
			if followErr != nil {
				response.MenuExecution = append(response.MenuExecution, MenuRoundExecution{OptionID: selection.OptionId, ProgramID: entry.programID, RoundIndex: round + 1, Error: boundedToolText("follow-up: " + followErr.Error())})
			}
			break
		}
		report.FailedPhase = "verification"
		report.ReasonCode = "direct_verification_unresolved"
		if applyErr != nil {
			rr.Verification = verdictContract(program.Verdict{Result: program.Unknown})
			if !result.Applied {
				report.Status = contracts.MitigationReport_STATUS_EXECUTION_ERROR
				report.FailedPhase = "forward"
				report.ReasonCode = "direct_apply_failed"
				for _, action := range result.ActionReports {
					if action.FailureReason != "" {
						report.ReasonCode = action.FailureReason
						break
					}
				}
			}
			break
		}
	}
	if len(planned) > 1 && len(l.pendingWrites) > 0 {
		// Earlier rounds' reservations settle once a follow-up unblocked them.
		l.settlePending(ctx)
	}
	report.RemainingBatches = uint32(max(0, min(5, l.MaxBatches)-l.batches))
	l.menuReports = append(l.menuReports, cloneSelectionReport(report))
	if l.menuExecutions == nil {
		l.menuExecutions = map[string][]MenuRoundExecution{}
	}
	l.menuExecutions[attempt] = cloneMenuExecution(response.MenuExecution)
	response.Report = report
	response.Summary = "mitigation " + report.Status.String()
}

func (l *ResponderLoop) prepareMenuPlan(ctx context.Context, entry menuEntry, parameters map[string]string, bindings program.Bindings) (program.Plan, error) {
	plan := program.Plan{}
	var err error
	if entry.action != nil {
		inputs := program.ValueInputs{Spec: program.ProgramSpec{ID: string(entry.action.Message().Name())}}
		observed := map[sdk.ObjectRef]*unstructured.Unstructured{}
		inputs.Observed = func(ref sdk.ObjectRef) (map[string]any, error) {
			if err := program.FromObserved(ref, "metadata.name").Validate(); err != nil {
				return nil, err
			}
			if object := observed[ref]; object != nil {
				return object.DeepCopy().Object, nil
			}
			object, err := l.Executor.Client.Get(ctx, ref)
			if err != nil {
				return nil, err
			}
			if object == nil {
				return nil, fmt.Errorf("observed value object unavailable")
			}
			observed[ref] = object.DeepCopy()
			return object.DeepCopy().Object, nil
		}
		plan, err = menuRawPlan(entry.action, parameters, &inputs)
		if err == nil {
			plan = program.BindPlanValues(plan, inputs)
		}
	} else if entry.plan != nil {
		plan = *entry.plan
	} else {
		mitigator, found := l.Programs.Mitigator(entry.programID)
		if !found {
			err = fmt.Errorf("mitigator %s is unavailable", entry.programID)
		} else {
			view, viewErr := l.snapshotView(ctx)
			if viewErr != nil {
				err = viewErr
			} else {
				plan, err = l.runMitigator(ctx, entry.programID, mitigator, view, bindings)
			}
		}
	}
	return plan, err
}

func (l *ResponderLoop) validateMenuPreconditions(ctx context.Context, entry menuEntry, bindings program.Bindings) error {
	if entry.action != nil || entry.plan != nil {
		return nil
	}
	diagnoser, found := l.Programs.Diagnoser(entry.programID)
	if !found {
		return fmt.Errorf("diagnoser %s is unavailable", entry.programID)
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return err
	}
	verdict := program.RunDiagnose(ctx, diagnoser, view, bindings, l.deadline())
	if verdict.Result != program.Match {
		return fmt.Errorf("program %s no longer matches live state: %s", entry.programID, verdict.Result)
	}
	return nil
}
