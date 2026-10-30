package runtime

import (
	"context"
	"encoding/json"
	"errors"
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
	response.Report = &contracts.MitigationReport{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, AttemptId: fmt.Sprintf("mitigation-%s-refused-%d", l.sessionID, len(l.calls)), RequestId: "refused", AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_REFUSED, FailedPhase: "acceptance", ReasonCode: reason, RemainingBatches: uint32(l.loopsRemaining())}
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
	if l.loopsRemaining() == 0 {
		response.Error = "response loop budget exhausted"
		return
	}
	if l.menuEntries.empty() {
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
		entry, ok := l.menuEntries.lookup(selected.OptionId)
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
			l.refuseSelection(response, mitigationPlanningReason(err, "direct_plan_failed"), err.Error())
			return
		}
		plan, err := l.prepareMenuPlan(ctx, entry, selected.Parameters, bindings)
		if err != nil {
			l.refuseSelection(response, mitigationPlanningReason(err, "direct_plan_failed"), err.Error())
			return
		}
		refs, err := l.menuPlanWriteRefs(plan)
		for _, ref := range refs {
			if ref.Kind == "*" && len(set.Options) > 1 {
				l.refuseSelection(response, "write_set_conflict", "script write scope requires one option per application")
				return
			}
		}
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
		}
		if l.pendingConflict(ctx, refs, response) {
			return
		}
		// Preserve reservation ownership diagnostics even when an earlier
		// pending write has already cleared the program's live diagnosis.
		if err := l.validateMenuPreconditions(ctx, entry, bindings); err != nil {
			l.refuseSelection(response, mitigationPlanningReason(err, "direct_plan_failed"), err.Error())
			return
		}

		entry.bindings = bindings
		entry.plan = &plan
		entry.action = nil
		prepared = append(prepared, preparedMenuOption{selected, entry, plan})
	}
	// Resolve the entire batch write set before structural validation so a raw
	// API request cannot hide an overlapping object behind a validation error.
	// Dry-run the real executor's policy and structural checks for every option.
	for _, item := range prepared {
		if err := item.plan.Validate(); err != nil {
			l.refuseSelection(response, "direct_plan_failed", err.Error())
			return
		}
		if _, err := l.Executor.Apply(ctx, l.mitigationIncident([]selectedPlan{{programID: item.entry.programID, bindings: item.entry.bindings, targetFingerprints: item.selection.FindingFingerprints}}), item.plan, true); err != nil {
			l.refuseSelection(response, mitigationPlanningReason(err, "direct_plan_failed"), err.Error())
			return
		}
	}
	l.spendLoop()
	if err := l.persistToolState(ctx); err != nil {
		response.Error = err.Error()
		return
	}
	combined := &contracts.MitigationReport{Version: 1, IncidentId: l.Request.IncidentID, SessionId: l.sessionID, RequestId: fmt.Sprintf("batch-%d", len(l.calls)), AttemptId: fmt.Sprintf("batch-%s-%d", l.sessionID, len(l.menuReports)+1), AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_SUCCESS, RemainingBatches: uint32(l.loopsRemaining()), Rounds: []*contracts.MitigationRoundReport{{RoundIndex: 1, RoundId: fmt.Sprintf("batch-%d", len(l.menuReports)+1)}}}
	for _, item := range prepared {
		part := ToolResponse{Command: "mitigate"}
		l.executeMenuEntry(ctx, item.entry, &contracts.MitigationSelection{OptionId: item.selection.OptionId, RoundLimit: 1}, nil, item.selection.FindingFingerprints, &part, false)
		if part.Report != nil {
			response.OptionReports = append(response.OptionReports, part.Report)
			mergeMitigationStatus(combined, part.Report)
			for _, round := range part.Report.Rounds {
				combined.Rounds[0].Actions = append(combined.Rounds[0].Actions, round.Actions...)
			}
		}
		response.MenuExecution = append(response.MenuExecution, part.MenuExecution...)
		response.ProgramRuns = append(response.ProgramRuns, part.ProgramRuns...)
		if len(part.ProgramRevisions) > 0 {
			if response.ProgramRevisions == nil {
				response.ProgramRevisions = map[string]string{}
			}
			for id, revision := range part.ProgramRevisions {
				response.ProgramRevisions[id] = revision
			}
		}
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
	ProgramRevision string
	diagnoser       program.Diagnoser
	Refs            []sdk.ObjectRef
	// WriteRefs separates exclusive desired-state writes from rollout observation.
	// Nil preserves conservative scope for older checkpoints.
	WriteRefs           *[]sdk.ObjectRef
	AttemptID           string
	ProgramID           string
	Bindings            program.Bindings
	FindingFingerprints []string
	ReservedAt          time.Time
	// Released reservations no longer hold their writes but are still observed;
	// Superseded ones saw a later write and earn no late credit.
	Released   bool
	Superseded bool
	// Reason is the typed reason a reservation was released before its rollout
	// settled; empty means the stall bound released it.
	Reason string
}

func (l *ResponderLoop) reservePending(application string, result PlanApplicationResult, attempt, programID string, bindings program.Bindings, fingerprints []string) {
	if l.pendingWrites == nil {
		l.pendingWrites = map[string]pendingMitigation{}
	}
	refs, err := l.menuPlanWriteRefs(result.Plan)
	if err != nil {
		refs = planCaptureRefs(result.Plan)
	}
	exclusive := l.applicationWriteRefs(result)
	sort.Slice(refs, func(i, j int) bool { return writeObjectKey(refs[i]) < writeObjectKey(refs[j]) })
	var diagnoser program.Diagnoser
	revision := ""
	if programID != "" && l.Programs != nil {
		diagnoser, _ = l.Programs.Diagnoser(programID)
		revision = l.programRevision(programID)
	}
	if programID != "" && revision != "" && diagnoser != nil && len(l.applicationBindings[application]) == 0 {
		if l.applicationBindings == nil {
			l.applicationBindings = map[string][]recoveryBinding{}
		}
		l.applicationBindings[application] = []recoveryBinding{{ProgramID: programID, ProgramRevision: revision, Bindings: cloneProgramBindings(bindings), FindingFingerprints: append([]string(nil), fingerprints...)}}
	}
	l.pendingWrites[application] = pendingMitigation{diagnoser: diagnoser, ProgramRevision: revision, Refs: refs, WriteRefs: exclusive, AttemptID: attempt, ProgramID: programID, Bindings: cloneProgramBindings(bindings), FindingFingerprints: append([]string(nil), fingerprints...), ReservedAt: time.Now().UTC()}
}

// applicationWriteRefs is the shared desired-state scope for reservation and
// recovery attribution. Requeues and confirmed unchanged writes only observe.
func (l *ResponderLoop) applicationWriteRefs(result PlanApplicationResult) *[]sdk.ObjectRef {
	writes := program.Plan{}
	for _, action := range result.ActionsApplied {
		// A requeue changes no desired state. Every prerequisite repair may
		// retrigger the controller, even while its earlier rollout is pending.
		if isAdmissionRetry(action) {
			continue
		}
		ref, _ := planActionRef(action)
		unchanged := false
		for _, patch := range result.Patches {
			if patch.Ref == ref && patch.Operation == "" && patch.Before != nil && patch.After != nil {
				// API bookkeeping can advance on an unchanged write. Use the
				// same configuration boundary as the executor's readback.
				unchanged = true
				for _, field := range planPatchFields(patch) {
					before, beforeOK := planConfigurationField(patch.Before, field)
					after, afterOK := planConfigurationField(patch.After, field)
					if beforeOK != afterOK || !planValuesEqual(before, after) {
						unchanged = false
						break
					}
				}
				if !unchanged {
					break
				}
			}
		}
		if !unchanged {
			writes.Actions = append(writes.Actions, action)
		}
	}
	if result.PendingAction != nil && *result.PendingAction >= 0 && *result.PendingAction < len(result.Plan.Actions) {
		writes.Actions = append(writes.Actions, result.Plan.Actions[*result.PendingAction])
	}
	writeRefs, writeErr := l.menuPlanWriteRefs(writes)
	var exclusive *[]sdk.ObjectRef
	if writeErr == nil {
		if writeRefs == nil {
			writeRefs = []sdk.ObjectRef{}
		}
		exclusive = &writeRefs
	}
	return exclusive
}

// Reservations are observation state, never restored write or rollback authority.
func (l *ResponderLoop) settlePending(ctx context.Context) {
	executor := l.Executor
	if executor.Rollouts == nil {
		return
	}
	executor.PollRollouts = true
	for application, pending := range l.pendingWrites {
		if len(pending.Refs) == 0 {
			continue
		}
		if observed, err := executor.waitForRollouts(ctx, l.Request, pending.Refs); err != nil {
			stall := ""
			var rollout *RolloutPendingError
			if errors.As(err, &rollout) && rollout.Stalled {
				stall = observed.GetReason()
			}
			l.releaseStalledPending(ctx, application, pending, stall)
			continue
		}
		result, hasResult := l.applications[application]
		if hasResult {
			// Rollout status alone does not prove accepted-source convergence (old
			// ReplicaSets and terminating Pods may still be present).
			if err := executor.observeSourceResiduals(ctx, l.Request, &result); err != nil {
				l.applications[application] = result
				l.releaseStalledPending(ctx, application, pending, "")
				continue
			}
		}
		delete(l.pendingWrites, application)
		if !hasResult {
			l.updatePendingReport(application, pending.AttemptID, false, "verification_unavailable")
			continue // Restart cannot recreate process-local verification authority.
		}
		if pending.Superseded {
			// A write after the release may have finished this rollout.
			result.VerificationCompleted = true
			result.VerificationPassed = false
			l.applications[application] = result
			reason := "rollout_stalled"
			if pending.Reason != "" {
				reason = pending.Reason
			}
			l.updatePendingReport(application, pending.AttemptID, false, reason)
			continue
		}
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
			diagnoser, ok := pending.diagnoser, pending.diagnoser != nil
			if !ok && (pending.ProgramRevision == "" || pending.ProgramRevision == l.programRevision(pending.ProgramID)) {
				diagnoser, ok = l.Programs.Diagnoser(pending.ProgramID)
			}
			if ok {
				view, err := l.snapshotView(ctx)
				result.VerificationPassed = result.VerificationPassed && err == nil
				if err == nil {
					verdict := program.RunDiagnose(ctx, diagnoser, view, pending.Bindings, l.deadline())
					l.runs = append(l.runs, ProgramRun{ProgramRevision: pending.ProgramRevision, ProgramID: pending.ProgramID, Bindings: cloneProgramBindings(pending.Bindings), Verdict: verdict})
					result.VerificationPassed = result.VerificationPassed && verdict.Result == program.NoMatch
				}
			}
		}
		l.applications[application] = result
		for i := range l.applicationsLog {
			if l.applicationsLog[i].ID == application {
				l.applicationsLog[i].Result = result
				l.applicationsLog[i].Pending = planApplicationPending(result)
				l.applicationsLog[i].Error = ""
			}
		}
		l.updatePendingReport(application, pending.AttemptID, result.VerificationPassed, "direct_verification_unresolved")
	}
}

// A pending application that does not converge within its bound releases its
// writes to the responder with typed reason rollout_stalled. Its writes stay
// applied and unverified, and observation continues: a rollout that completes
// later settles through the ordinary verification, so a slow healthy rollout
// keeps its success. A rollout observed to be unable to progress without
// another actor (stall names its blocker) releases after the stall grace
// instead: waiting longer cannot clear it.
func (l *ResponderLoop) releaseStalledPending(ctx context.Context, application string, pending pendingMitigation, stall string) {
	if pending.Released {
		return
	}
	if pending.ReservedAt.IsZero() {
		// Restored reservations start their bound at restore time.
		pending.ReservedAt = time.Now().UTC()
		l.pendingWrites[application] = pending
		return
	}
	bound := l.PendingStallTimeout
	if bound <= 0 {
		bound = DefaultPendingApplicationStall
	}
	if stall != "" {
		bound = min(bound, rolloutStallGrace)
	}
	if time.Since(pending.ReservedAt) < bound {
		return
	}
	pending.Released = true
	l.pendingWrites[application] = pending
	const reason = "rollout_stalled"
	if result, ok := l.applications[application]; ok {
		_ = l.Executor.reportSourceResiduals(ctx, l.Request, &result)
		result.RolloutPending = nil
		l.applications[application] = result
		for i := range l.applicationsLog {
			if l.applicationsLog[i].ID == application {
				l.applicationsLog[i].Result = result
				l.applicationsLog[i].Pending = planApplicationPending(result)
				l.applicationsLog[i].Error = reason + ": rollout did not converge within " + bound.String() + "; writes remain applied and unverified"
				if stall != "" {
					l.applicationsLog[i].Error = reason + ": " + boundedRolloutText(stall, 512) + "; writes remain applied and unverified"
				}
			}
		}
	}
	l.updatePendingReport(application, pending.AttemptID, false, reason)
	for _, report := range l.menuReports {
		if report.AttemptId == pending.AttemptID {
			report.FailedPhase = "rollout"
		}
	}
}

// Observation targets include idempotent retries; only writes need exclusion.
func (p pendingMitigation) exclusiveRefs() []sdk.ObjectRef {
	if p.WriteRefs != nil {
		return *p.WriteRefs
	}
	return p.Refs
}

func (l *ResponderLoop) pendingOverlaps(ref sdk.ObjectRef, pending pendingMitigation) bool {
	for _, target := range pending.exclusiveRefs() {
		if ref.Kind == "*" || target.Kind == "*" || writeObjectKey(ref) == writeObjectKey(target) {
			return true
		}
	}
	return false
}

// failIneffectivePending ends a held program application whose writes landed
// but whose own diagnosis still matches while a finding it targeted is still
// active: its plan did not address the fault, so waiting for its rollout
// cannot help. It fails with diagnosis_persists and releases its writes so an
// independent repair can run; the repair then supersedes it (no late credit).
// Without a diagnoser or a fresh diagnosis there is no such evidence and the
// reservation keeps holding.
func (l *ResponderLoop) failIneffectivePending(ctx context.Context, application string, pending pendingMitigation) bool {
	if pending.ProgramID == "" || l.Programs == nil {
		return false
	}
	if !l.targetedFindingActive(pending.FindingFingerprints) {
		return false
	}
	diagnoser, ok := pending.diagnoser, pending.diagnoser != nil
	if !ok && (pending.ProgramRevision == "" || pending.ProgramRevision == l.programRevision(pending.ProgramID)) {
		diagnoser, ok = l.Programs.Diagnoser(pending.ProgramID)
	}
	if !ok {
		return false
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return false
	}
	verdict := program.RunDiagnose(ctx, diagnoser, view, pending.Bindings, l.deadline())
	l.runs = append(l.runs, ProgramRun{ProgramRevision: pending.ProgramRevision, ProgramID: pending.ProgramID, Bindings: cloneProgramBindings(pending.Bindings), Verdict: verdict})
	if verdict.Result != program.Match {
		return false
	}
	const reason = "diagnosis_persists"
	pending.Released = true
	pending.Reason = reason
	l.pendingWrites[application] = pending
	if result, ok := l.applications[application]; ok {
		result.RolloutPending = nil
		result.VerificationCompleted = true
		result.VerificationPassed = false
		l.applications[application] = result
		for i := range l.applicationsLog {
			if l.applicationsLog[i].ID == application {
				l.applicationsLog[i].Result = result
				l.applicationsLog[i].Pending = planApplicationPending(result)
				l.applicationsLog[i].Error = reason + ": " + pending.ProgramID + " still matches after its writes; writes remain applied"
			}
		}
	}
	l.updatePendingReport(application, pending.AttemptID, false, reason)
	return true
}

// targetedFindingActive is true when no targets were named or one is still active.
func (l *ResponderLoop) targetedFindingActive(fingerprints []string) bool {
	if len(fingerprints) == 0 {
		return true
	}
	for _, finding := range l.Request.Findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		for _, fingerprint := range fingerprints {
			if FindingFingerprint(finding) == fingerprint {
				return true
			}
		}
	}
	return false
}

// heldPendingWrites counts reservations that still hold their writes.
func (l *ResponderLoop) heldPendingWrites() int {
	held := 0
	for _, pending := range l.pendingWrites {
		if !pending.Released {
			held++
		}
	}
	return held
}

func (l *ResponderLoop) pendingConflict(ctx context.Context, refs []sdk.ObjectRef, response *ToolResponse) bool {
	return l.pendingConflictExcept(ctx, refs, response, "")
}

// A held reservation refuses overlapping writes unless it is shown not to have
// addressed its fault (failIneffectivePending). A released one is superseded
// by any later write that is not refused: the write may be what let its
// rollout finish (an owned ReplicaSet or Pod, a claim, a node), so the stalled
// application earns no late credit. Reservations of attempt itself are skipped.
func (l *ResponderLoop) pendingConflictExcept(ctx context.Context, refs []sdk.ObjectRef, response *ToolResponse, attempt string) bool {
	for _, ref := range refs {
		for application, pending := range l.pendingWrites {
			if pending.Released || (attempt != "" && pending.AttemptID == attempt) {
				continue
			}
			if l.pendingOverlaps(ref, pending) && l.failIneffectivePending(ctx, application, pending) {
				continue
			}
			if len(pending.Refs) == 0 && pending.WriteRefs == nil {
				l.refuseSelection(response, "pending_write_conflict", "pending application "+application+" has unknown write scope")
				return true
			}
			for _, target := range pending.exclusiveRefs() {
				if ref.Kind == "*" || target.Kind == "*" || writeObjectKey(ref) == writeObjectKey(target) {
					l.refuseSelection(response, "pending_write_conflict", fmt.Sprintf("%s conflicts with pending application %s", writeObjectKey(ref), application))
					return true
				}
			}
		}
	}
	for application, pending := range l.pendingWrites {
		if pending.Released && (attempt == "" || pending.AttemptID != attempt) {
			for _, ref := range refs {
				if ref.Kind == "*" || l.pendingOverlaps(ref, pending) {
					pending.Superseded = true
					break
				}
			}
			l.pendingWrites[application] = pending
		}
	}
	return false
}

// Raw API requests identify effects by their canonical path, not by a caller's
// optional Ref. Collection creates must name their object in the request body.
func (l *ResponderLoop) menuPlanWriteRefs(plan program.Plan) ([]sdk.ObjectRef, error) {
	var refs []sdk.ObjectRef
	for _, action := range plan.Actions {
		if _, ok := concreteExternalAction(action).(program.RunScript); ok {
			// An opaque write set conflicts with every pending/batched write.
			refs = append(refs, sdk.ObjectRef{Kind: "*", Namespace: l.Request.Namespace, Name: "*"})
			continue
		}
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

func (l *ResponderLoop) updatePendingReport(application, attempt string, passed bool, reason string) {
	for _, report := range l.menuReports {
		if report.AttemptId != attempt {
			continue
		}
		report.FailedPhase = "verification"
		report.ReasonCode = reason
		if passed {
			report.Status = contracts.MitigationReport_STATUS_SUCCESS
			report.FailedPhase = ""
			report.ReasonCode = ""
		}
		for _, round := range report.Rounds {
			round.Verification = verdictContract(program.Verdict{Result: program.Unknown})
			if passed {
				round.Verification = verdictContract(program.Verdict{Result: program.NoMatch})
			}
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
	l.refreshCombinedReports()
}

// Aggregate reports are recomputed from their own option reports. A later peer
// success cannot hide a pending or failed option in the same selection set.
func (l *ResponderLoop) refreshCombinedReports() {
	for _, report := range l.menuReports {
		if report.OptionId != "" {
			continue
		}
		parts := l.optionReportsFor(report.AttemptId)
		if len(parts) == 0 {
			continue
		}
		report.Status = contracts.MitigationReport_STATUS_SUCCESS
		report.FailedPhase = ""
		report.ReasonCode = ""
		for _, part := range parts {
			mergeMitigationStatus(report, part)
		}
	}
}
func (l *ResponderLoop) optionReportsFor(attempt string) []*contracts.MitigationReport {
	var reports []*contracts.MitigationReport
	for _, execution := range l.menuExecutions[attempt] {
		for _, report := range l.menuReports {
			if report.OptionId != "" && report.ApplicationId != "" && report.ApplicationId == execution.ApplicationID {
				reports = append(reports, cloneSelectionReport(report))
				break
			}
		}
	}
	return reports
}

func mergeMitigationStatus(combined, part *contracts.MitigationReport) {
	priority := func(status contracts.MitigationReport_Status) int {
		switch status {
		case contracts.MitigationReport_STATUS_SUCCESS:
			return 0
		case contracts.MitigationReport_STATUS_UNRESOLVED:
			return 1
		default:
			return 2
		}
	}
	if priority(part.Status) > priority(combined.Status) {
		combined.Status = part.Status
		combined.FailedPhase = part.FailedPhase
		combined.ReasonCode = part.ReasonCode
	}
}

// RefreshPendingMitigations performs observation only. The controller may call
// it between tool requests so a silent responder cannot leave settled targets
// reserved. Busy tools and persistence failures remain pending conservatively.
func (l *ResponderLoop) RefreshPendingMitigations(ctx context.Context) bool {
	if !l.operationMu.TryLock() {
		return true
	}
	defer l.operationMu.Unlock()
	if !l.mu.TryLock() {
		return true
	}
	defer l.mu.Unlock()
	if len(l.pendingWrites) > 0 {
		l.settlePending(ctx)
	}
	if err := l.persistToolState(ctx); err != nil {
		return true
	}
	return l.heldPendingWrites() > 0 || l.pendingApplicationResultsLocked()
}

func planApplicationPending(result PlanApplicationResult) bool {
	return result.RolloutPending != nil || result.PendingAction != nil || result.RollingBack
}

func (l *ResponderLoop) pendingApplicationResultsLocked() bool {
	for _, result := range l.applications {
		if planApplicationPending(result) {
			return true
		}
	}
	for _, application := range l.applicationsLog {
		if application.Pending || planApplicationPending(application.Result) {
			return true
		}
	}
	return false
}

// hasPendingMitigationsLocked is consumed by the closure guard while it owns
// both responder locks. Observe rollouts without writes, then check every
// authoritative application result, including ambiguous writes and rollback.
func (l *ResponderLoop) hasPendingMitigationsLocked() bool {
	if len(l.pendingWrites) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		l.settlePending(ctx)
	}
	return l.heldPendingWrites() > 0 || l.pendingApplicationResultsLocked()
}
