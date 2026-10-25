package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type preparedMenuOption struct {
	selection *contracts.MitigationOptionSelection
	entry     menuEntry
	plan      program.Plan
}

func (l *ResponderLoop) refuseSelection(response *ToolResponse, reason, detail string) {
	response.Error = detail
	response.Report = &contracts.MitigationReport{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, AttemptId: fmt.Sprintf("mitigation-%s-refused-%d", l.sessionID, len(l.calls)), RequestId: "refused", AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_REFUSED, FailedPhase: "acceptance", ReasonCode: reason, RemainingBatches: uint32(max(0, l.batchLimit()-l.batches))}
}

// Prepare every option and compare full object identities before the first write.
// Execution remains with the ordinary executor, including its per-option guards.
func (l *ResponderLoop) mitigateSelection(ctx context.Context, request ToolRequest, response *ToolResponse) {
	set := request.SelectionSet
	if set == nil && len(l.pendingWrites) == 0 {
		l.mitigateMenu(ctx, request.Selection, request.Parameters, request.FindingFingerprints, response)
		return
	}
	if set != nil && request.Selection != nil {
		l.refuseSelection(response, "invalid_selection", "choose selection or selection_set")
		return
	}
	if set == nil {
		if request.Selection == nil {
			l.refuseSelection(response, "invalid_selection", "mitigate requires an option")
			return
		}
		set = &contracts.MitigationSelectionSet{Version: 1, Options: []*contracts.MitigationOptionSelection{{OptionId: request.Selection.OptionId, Bindings: request.Selection.Bindings, Parameters: request.Parameters, FindingFingerprints: request.FindingFingerprints}}}
	}
	if err := wire.Validate(set); err != nil {
		l.refuseSelection(response, "invalid_selection", "selection_set requires version 1 and 1..32 options")
		return
	}
	if l.batches >= l.batchLimit() {
		response.Error = "response loop budget exhausted"
		return
	}
	if l.menuEntries == nil {
		listing := ToolResponse{}
		l.listMenu(ctx, ToolRequest{Command: "menu"}, &listing)
		if listing.Error != "" {
			response.Error = listing.Error
			return
		}
	}
	prepared := make([]preparedMenuOption, 0, len(set.Options))
	owners := map[string]string{}
	for index, selected := range set.Options {
		if selected == nil {
			l.refuseSelection(response, "invalid_selection", "nil option")
			return
		}
		entry, ok := l.menuEntries[selected.OptionId]
		if !ok {
			l.refuseSelection(response, "invalid_selection", "unknown mitigation option: "+selected.OptionId)
			return
		}
		if err := l.validateFindingTargets(selected.FindingFingerprints); err != nil {
			l.refuseSelection(response, "invalid_selection", err.Error())
			return
		}
		if len(selected.Bindings) > 0 {
			entry.bindings = programBindingsState(selected.Bindings)
		}
		bindings, err := menuBindProgram(entry, selected.Parameters)
		if err != nil {
			l.refuseSelection(response, "direct_plan_failed", err.Error())
			return
		}
		plan, err := l.prepareMenuPlan(ctx, entry, selected.Parameters, bindings)
		if err != nil {
			l.refuseSelection(response, "direct_plan_failed", err.Error())
			return
		}
		if err = plan.Validate(); err != nil {
			l.refuseSelection(response, "direct_plan_failed", err.Error())
			return
		}
		refs, err := l.menuPlanWriteRefs(plan)
		if err != nil {
			l.refuseSelection(response, "write_set_unknown", err.Error())
			return
		}
		for _, ref := range refs {
			key := writeObjectKey(ref)
			owner := fmt.Sprintf("option %d (%s)", index+1, selected.OptionId)
			if previous, ok := owners[key]; ok && previous != owner {
				l.refuseSelection(response, "write_set_conflict", fmt.Sprintf("%s conflicts with %s on %s", owner, previous, key))
				return
			}
			owners[key] = owner
			for application, pendingWrite := range l.pendingWrites {
				for _, pending := range pendingWrite.Refs {
					if writeObjectKey(pending) == key {
						l.refuseSelection(response, "pending_write_conflict", fmt.Sprintf("%s conflicts with pending application %s on %s", owner, application, key))
						return
					}
				}
			}
		}
		entry.bindings = bindings
		entry.plan = &plan
		entry.action = nil
		prepared = append(prepared, preparedMenuOption{selected, entry, plan})
	}
	// Dry-run the real executor's policy and structural checks for every option.
	for _, item := range prepared {
		if _, err := l.Executor.Apply(ctx, l.mitigationIncident([]selectedPlan{{programID: item.entry.programID, bindings: item.entry.bindings, targetFingerprints: item.selection.FindingFingerprints}}), item.plan, true); err != nil {
			l.refuseSelection(response, "direct_plan_failed", err.Error())
			return
		}
	}
	l.batches++
	if err := l.persistToolState(ctx); err != nil {
		response.Error = err.Error()
		return
	}
	combined := &contracts.MitigationReport{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, RequestId: fmt.Sprintf("batch-%d", len(l.calls)), AttemptId: fmt.Sprintf("batch-%s-%d", l.sessionID, l.batches), AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_SUCCESS, RemainingBatches: uint32(max(0, l.batchLimit()-l.batches)), Rounds: []*contracts.MitigationRoundReport{{RoundIndex: 1, RoundId: fmt.Sprintf("batch-%d", l.batches)}}}
	for _, item := range prepared {
		original := l.menuEntries[item.selection.OptionId]
		l.menuEntries[item.selection.OptionId] = item.entry
		part := ToolResponse{Command: "mitigate"}
		l.executeMenuOption(ctx, &contracts.MitigationSelection{OptionId: item.selection.OptionId, RoundLimit: 1}, nil, item.selection.FindingFingerprints, &part, false)
		l.menuEntries[item.selection.OptionId] = original
		if part.Report != nil {
			response.OptionReports = append(response.OptionReports, part.Report)
			if part.Report.Status != contracts.MitigationReport_STATUS_SUCCESS {
				combined.Status = part.Report.Status
				combined.ReasonCode = part.Report.ReasonCode
				combined.FailedPhase = part.Report.FailedPhase
			}
			for _, round := range part.Report.Rounds {
				combined.Rounds[0].Actions = append(combined.Rounds[0].Actions, round.Actions...)
			}
		}
		response.MenuExecution = append(response.MenuExecution, part.MenuExecution...)
		response.ProgramRuns = append(response.ProgramRuns, part.ProgramRuns...)
		if part.ApplicationID != "" {
			response.ApplicationID = part.ApplicationID
		}
		if part.RolloutPending != nil {
			response.RolloutPending = part.RolloutPending
		}
		response.Healthy = part.Healthy
		response.TargetedHealthy = combined.Status == contracts.MitigationReport_STATUS_SUCCESS
		response.MitigationHealth = part.MitigationHealth
		if part.Error != "" {
			response.Error = part.Error
		}
	}
	if len(prepared) == 1 && request.SelectionSet == nil {
		response.Report = response.OptionReports[0]
	} else {
		response.Report = combined
		l.menuReports = append(l.menuReports, proto.Clone(combined).(*contracts.MitigationReport))
		l.menuExecutions[combined.AttemptId] = cloneMenuExecution(response.MenuExecution)
	}
	if response.RolloutPending != nil {
		response.Healthy = false
		response.TargetedHealthy = false
	}
	response.Summary = fmt.Sprintf("%d mitigation options: %s", len(prepared), combined.Status.String())
}

func writeObjectKey(ref sdk.ObjectRef) string { return ref.Kind + "/" + ref.Namespace + "/" + ref.Name }

type pendingMitigation struct {
	Refs                []sdk.ObjectRef
	AttemptID           string
	ProgramID           string
	Bindings            program.Bindings
	FindingFingerprints []string
}

func (l *ResponderLoop) reservePending(application string, plan program.Plan, attempt, programID string, bindings program.Bindings, fingerprints []string) {
	if l.pendingWrites == nil {
		l.pendingWrites = map[string]pendingMitigation{}
	}
	refs, _ := l.menuPlanWriteRefs(plan)
	sort.Slice(refs, func(i, j int) bool { return writeObjectKey(refs[i]) < writeObjectKey(refs[j]) })
	l.pendingWrites[application] = pendingMitigation{Refs: refs, AttemptID: attempt, ProgramID: programID, Bindings: cloneProgramBindings(bindings), FindingFingerprints: append([]string(nil), fingerprints...)}
}

// Reservations are observation state, never restored write or rollback authority.
func (l *ResponderLoop) settlePending(ctx context.Context) {
	executor := l.Executor
	executor.PollRollouts = true
	for application, pending := range l.pendingWrites {
		if _, err := executor.waitForRollouts(ctx, l.Request, pending.Refs); err != nil {
			continue
		}
		delete(l.pendingWrites, application)
		result, hasResult := l.applications[application]
		if !hasResult {
			continue
		} // Restart cannot recreate process-local verification authority.
		result.RolloutPending = nil
		result.VerificationCompleted = true
		verifier := scopedMitigationVerifier(executor.Verifier, result.Plan, l.mitigationIncident([]selectedPlan{{programID: pending.ProgramID, bindings: pending.Bindings, targetFingerprints: pending.FindingFingerprints}}).Findings)
		if verifier != nil {
			result.VerificationPassed, _ = verifier.Verify(ctx)
			result.MitigationHealth = verifier.observation
		} else if executor.Verifier != nil {
			result.VerificationPassed, _ = executor.Verifier.Verify(ctx)
		}
		if l.Programs != nil {
			if diagnoser, ok := l.Programs.Diagnoser(pending.ProgramID); ok {
				view, err := l.snapshotView(ctx)
				result.VerificationPassed = result.VerificationPassed && err == nil
				if err == nil {
					verdict := program.RunDiagnose(ctx, diagnoser, view, pending.Bindings, l.deadline())
					result.VerificationPassed = result.VerificationPassed && verdict.Result == program.NoMatch
				}
			}
		}
		l.applications[application] = result
		for i := range l.applicationsLog {
			if l.applicationsLog[i].ID == application {
				l.applicationsLog[i].Result = result
				l.applicationsLog[i].Error = ""
				l.applicationsLog[i].CompletedAt = time.Now().UTC()
			}
		}
		for _, report := range l.menuReports {
			if report.AttemptId != pending.AttemptID {
				continue
			}
			report.FailedPhase = "verification"
			report.ReasonCode = "direct_verification_unresolved"
			if result.VerificationPassed {
				report.Status = contracts.MitigationReport_STATUS_SUCCESS
				report.FailedPhase = ""
				report.ReasonCode = ""
			}
		}
		for attempt, executions := range l.menuExecutions {
			for i := range executions {
				if executions[i].ApplicationID == application {
					executions[i].Pending = false
					executions[i].Error = ""
				}
			}
			l.menuExecutions[attempt] = executions
		}
	}
}

func (l *ResponderLoop) pendingConflict(refs []sdk.ObjectRef, response *ToolResponse) bool {
	for _, ref := range refs {
		for application, pending := range l.pendingWrites {
			for _, target := range pending.Refs {
				if writeObjectKey(ref) == writeObjectKey(target) {
					l.refuseSelection(response, "pending_write_conflict", fmt.Sprintf("%s conflicts with pending application %s", writeObjectKey(ref), application))
					return true
				}
			}
		}
	}
	return false
}

// Raw API requests identify effects by their canonical path, not by a caller's
// optional Ref. Collection creates must name their object in the request body.
func (l *ResponderLoop) menuPlanWriteRefs(plan program.Plan) ([]sdk.ObjectRef, error) {
	var refs []sdk.ObjectRef
	for _, action := range plan.Actions {
		if _, ok := generalAction(action).(program.WaitFor); ok {
			continue
		}
		if request, ok := generalAction(action).(program.KubernetesRequest); ok {
			method := strings.ToUpper(request.Method)
			if method == "GET" || method == "HEAD" {
				continue
			}
			path := request.Path
			parts := strings.Split(strings.Trim(path, "/"), "/")
			offset := 2
			if len(parts) > 0 && parts[0] == "apis" {
				offset = 3
			}
			if len(parts) > offset+2 && parts[offset] == "namespaces" {
				offset += 2
			}
			if method == "POST" && len(parts) == offset+1 {
				var object struct {
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
				}
				if json.Unmarshal(request.Body, &object) != nil || object.Metadata.Name == "" {
					return nil, fmt.Errorf("cannot identify collection create write target")
				}
				path = strings.TrimSuffix(path, "/") + "/" + object.Metadata.Name
			} else if len(parts) == offset+3 {
				path = "/" + strings.Join(parts[:len(parts)-1], "/")
			}
			probe := request
			probe.Method = "DELETE"
			probe.Path = path
			probe.Ref = sdk.ObjectRef{}
			probe.Body = nil
			ref, err := l.Executor.resolveDeleteTarget(probe)
			if err != nil {
				return nil, fmt.Errorf("identify API write target: %w", err)
			}
			if request.Ref != (sdk.ObjectRef{}) && writeObjectKey(request.Ref) != writeObjectKey(ref) {
				return nil, fmt.Errorf("API Ref conflicts with path write target")
			}
			refs = append(refs, ref)
			continue
		}
		ref, err := planActionRef(action)
		if err != nil {
			return nil, err
		}
		if ref.Kind == "" || ref.Name == "" {
			return nil, fmt.Errorf("write target must name an object")
		}
		refs = append(refs, ref)
	}
	return refs, nil
}
