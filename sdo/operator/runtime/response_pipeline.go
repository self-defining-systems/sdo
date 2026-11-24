package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

func responseStageNames(names []string) ([]string, error) {
	if len(names) == 0 {
		return []string{"deterministic", "agent"}, nil
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "deterministic" && name != "agent" {
			return nil, fmt.Errorf("unknown response stage %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate response stage %q", name)
		}
		seen[name] = true
		result = append(result, name)
	}
	return result, nil
}

func (d KubernetesJobDispatcher) DeterministicResponseStage() ResponseStage {
	if d.LoopFactory == nil {
		return nil
	}
	return &DeterministicResponseStage{NewLoop: d.LoopFactory, Restore: func(ctx context.Context, request IncidentRequest, loop *ResponderLoop) error {
		trust, credits, err := d.programLearning(ctx, request)
		if err != nil {
			return err
		}
		loop.Trust = trust
		loop.setCredits(credits)
		return d.restoreToolState(ctx, request, loop)
	}}
}

// Before automatic execution only the incident intent is durable. Freezing a
// worker task here could overwrite its external payload at the later handoff.
func (c *Controller) automaticResponsePending(request *IncidentRequest) bool {
	if request == nil || pageOpened(request) || request.PreparedAgentTask != "" || deterministicStageReport(request.PriorStages).GetHandoffSelected() {
		return false
	}
	for _, name := range c.codePath.Selection.ResponseStages {
		if name == "agent" {
			return false
		}
		if name == "deterministic" {
			source, ok := c.dispatcher.(interface{ DeterministicResponseStage() ResponseStage })
			return ok && source.DeterministicResponseStage() != nil
		}
	}
	return false
}

// agentLifecycleDispatcher records worker dispatch at its invocation boundary.
// Entering the response pipeline alone does not invoke an agent.
type agentLifecycleDispatcher struct{ controller *Controller }

func (d agentLifecycleDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	c := d.controller
	claimed := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.emitLifecycleLocked(&contracts.LifecycleEvent{
			Event: PhaseDispatched, RecordedAt: contractTimestamp(c.clock()), IncidentId: request.IncidentID,
			Worktree: request.RepositoryWorktree, BaseCommit: request.RepositoryBaseCommit,
		})
	}
	if dispatcher, ok := c.dispatcher.(interface {
		DispatchWithClaim(context.Context, IncidentRequest, func()) (IncidentResult, error)
	}); ok {
		return dispatcher.DispatchWithClaim(ctx, request, claimed)
	}
	claimed()
	return c.dispatcher.Dispatch(ctx, request)
}

type responseEntry int

const (
	runAutomaticStages responseEntry = iota
	dispatchDirectResponder
	dispatchStagedHandoff
)

// Choose the entry before launching background work. The same choice decides
// whether automatic execution can still hold the controller's recovery checks.
func (c *Controller) responseEntry(request IncidentRequest) responseEntry {
	report := deterministicStageReport(request.PriorStages)
	if pageOpened(&request) && report == nil ||
		len(c.codePath.Selection.ResponseStages) > 0 && c.codePath.Selection.ResponseStages[0] == "agent" {
		return dispatchDirectResponder
	}
	if report.GetHandoffSelected() || report != nil && (request.PreparedAgentTask != "" || pageOpened(&request)) {
		return dispatchStagedHandoff
	}
	return runAutomaticStages
}

func (c *Controller) runResponsePipeline(ctx context.Context, request IncidentRequest, entry responseEntry) dispatchCompletion {
	dispatcher := agentLifecycleDispatcher{controller: c}
	switch entry {
	case dispatchDirectResponder:
		result, err := dispatcher.Dispatch(ctx, request)
		return dispatchCompletion{result: result, err: err}
	case dispatchStagedHandoff:
		// Rejoin the frozen handoff with no claim-time health observation.
		result, err := dispatcher.Dispatch(ctx, stageDispatchRequest(request))
		state := &IncidentState{Request: request, PriorStages: cloneStageReports(request.PriorStages), Result: &result}
		merged, encodeErr := responsePipelineResult(state, result.Timing.StartedAt, result.Timing.CompletedAt)
		if err == nil {
			err = encodeErr
		}
		return dispatchCompletion{result: merged, priorStages: state.PriorStages, err: err}
	}
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...), Coverage: cloneCoverage(request.PlaybookCoverage)}
	state.LiveAlertSet = func() ([]string, []sdk.Finding) { return c.liveJoined(request.IncidentID) }
	var stages []ResponseStage
	automaticAvailable, agentConfigured := false, false
	for _, name := range c.codePath.Selection.ResponseStages {
		switch name {
		case "deterministic":
			if source, ok := c.dispatcher.(interface{ DeterministicResponseStage() ResponseStage }); ok {
				if stage := source.DeterministicResponseStage(); stage != nil {
					if deterministic, ok := stage.(*DeterministicResponseStage); ok {
						deterministic.StabilityWindow = c.config.ClosureStabilityWindow
						if c.config.AfterPlaybook == settings.AfterPlaybookWait {
							deterministic.WaitBound = c.config.AfterPlaybookWait
						}
					}
					stages = append(stages, stage)
					automaticAvailable = true
				}
			}
		case "agent":
			agentConfigured = true
		}
	}
	// A transport without an automatic stage preserves its original frozen
	// request and all original early-closure behavior.
	if !automaticAvailable && agentConfigured {
		c.mu.Lock()
		c.responseStagesRunning = false
		c.mu.Unlock()
		result, err := dispatcher.Dispatch(ctx, request)
		return dispatchCompletion{result: result, err: err}
	}
	started := c.clock().UTC()
	err := RunResponseStages(ctx, state, stages)
	if len(state.PriorStages) == 0 && automaticAvailable {
		state.PriorStages = []*contracts.StageReport{{Stage: "deterministic", ApplyCompletedAt: contractTimestamp(c.clock()),
			RecoveryWait: &contracts.RecoveryWait{ExitReason: contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED}}}
	}
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == request.IncidentID {
		// The result below is built from these reports, so a playbook a later
		// pass applied reaches AppliedPlaybooks and the root causes.
		if stagePassProgressed(deterministicStageReport(state.PriorStages), deterministicStageReport(c.currentIncidentRequest.PriorStages)) {
			state.Coverage = cloneCoverage(c.currentIncidentRequest.PlaybookCoverage)
			state.Request.InitialProgramRuns = append([]ProgramRun(nil), c.currentIncidentRequest.InitialProgramRuns...)
		}
		state.PriorStages = mergeLaterStagePasses(state.PriorStages, c.currentIncidentRequest.PriorStages)
		// Findings that joined the incident mid-way are the ones those passes
		// cleared, so the root causes need them.
		known := map[string]bool{}
		for _, finding := range state.Request.Findings {
			known[FindingFingerprint(finding)] = true
		}
		for _, finding := range c.currentIncidentRequest.Findings {
			if !known[FindingFingerprint(finding)] {
				state.Request.Findings = append(state.Request.Findings, finding)
			}
		}
	}
	c.mu.Unlock()
	result, encodeErr := responsePipelineResult(state, started, c.clock().UTC())
	if err == nil {
		err = encodeErr
	}
	return dispatchCompletion{automatic: state, result: result, priorStages: cloneStageReports(state.PriorStages), err: err}
}

// responsePipelineResult projects controller receipts into the existing outcome
// derivation path. Cause claims remain subject to independent closure evidence.
func responsePipelineResult(state *IncidentState, started, completed time.Time) (IncidentResult, error) {
	result := IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: state.Request.IncidentID, Status: IncidentCompleted, Timing: TimingMetrics{StartedAt: started, CompletedAt: completed}, ResolutionPath: "deterministic", UnresolvedFindings: append([]sdk.Finding(nil), state.Findings...)}
	result.ProgramRuns = append([]ProgramRun(nil), state.Request.InitialProgramRuns...)
	result.ProgramToolCalls = append([]ToolCall(nil), state.ProgramToolCalls...)
	seenApplication := map[string]bool{}
	seenCause := map[string]bool{}
	attemptPlans := map[string][]string{}
	for _, report := range state.PriorStages {
		for _, attempt := range report.Mitigations {
			if id := attempt.GetApplyReceipt().GetApplicationId(); id != "" {
				attemptPlans[id] = append(attemptPlans[id], attempt.PlanId)
			}
		}
	}
	for _, report := range state.PriorStages {
		for _, attempt := range report.Mitigations {
			if attempt.PostApplyDiagnose != nil {
				result.ProgramRuns = append(result.ProgramRuns, programRunState(attempt.PostApplyDiagnose))
			}
			if cause, ok := escalatedCause(attempt, state.Request.Findings); ok {
				result.ConfirmedRootCauses = mergeStageCause(result.ConfirmedRootCauses, cause)
				continue
			}
			receipt := attempt.ApplyReceipt
			if receipt == nil {
				continue
			}
			// Attempts of one stage apply share a single application; each keeps
			// its own applied playbook and root cause.
			application := planApplicationState(receipt)
			if !seenApplication[receipt.ApplicationId] {
				seenApplication[receipt.ApplicationId] = true
				recorded := application
				recorded.PlanIDs = attemptPlans[receipt.ApplicationId]
				result.PlanApplications = append(result.PlanApplications, recorded)
			}
			application = attemptApplication(application, attempt)
			if receipt.Applied {
				result.AppliedPlaybooks = append(result.AppliedPlaybooks, AppliedPlaybook{PlaybookID: attempt.PlaybookId, ParameterBindings: programBindingsState(attempt.Bindings).Objects})
			}
			// An applied program always yields a cause, cleared or pending: the
			// diagnosis it matched is what the repair addressed.
			if !receipt.Applied || receipt.RolledBack {
				continue
			}
			cause := ConfirmedRootCause{}
			addRef := func(ref sdk.ObjectRef) {
				for _, old := range cause.Resources {
					if old == ref {
						return
					}
				}
				cause.Resources = append(cause.Resources, ref)
			}
			for _, finding := range addressedFindings(attempt, state.Request.Findings) {
				fp := FindingFingerprint(finding)
				if seenCause[fp] {
					continue
				}
				seenCause[fp] = true
				if cause.Summary == "" {
					cause.Summary = finding.Summary
				}
				addRef(finding.PrimaryResource)
				mergeEarlyCauseEvidence(&cause, ConfirmedRootCause{ExplainedDetectors: []string{finding.DetectorID}, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}}})
			}
			for _, ref := range programBindingsState(attempt.Bindings).Objects {
				addRef(ref)
			}
			if cause.Summary == "" {
				cause.Summary = "playbook " + attempt.PlaybookId + " applied"
			}
			if len(cause.Resources) > 0 {
				for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{application}, nil) {
					if receipt.Success {
						for _, ref := range receipt.Resources {
							addRef(ref)
						}
					}
				}
				result.ConfirmedRootCauses = mergeStageCause(result.ConfirmedRootCauses, cause)
			}
		}
	}
	result.RepairActions = ProgramRepairReceipts(result.PlanApplications, nil)
	seenReceipt := map[string]bool{}
	for _, receipt := range result.RepairActions {
		seenReceipt[receipt.ActionID] = true
	}
	for _, receipt := range state.RepairActions {
		if !seenReceipt[receipt.ActionID] {
			result.RepairActions = append(result.RepairActions, receipt)
			seenReceipt[receipt.ActionID] = true
		}
	}
	if state.Result != nil {
		prior, err := incidentResultContract(result)
		if err != nil {
			return result, err
		}
		current := *state.Result
		current.ConfirmedRootCauses = reconcileEarlyCauses(result.ConfirmedRootCauses, current.ConfirmedRootCauses)
		prior.ConfirmedRootCauses = nil
		result = accumulateResponderResult(prior, current)
		// The ordinary accumulator merges agent-turn facts. Stage applications and
		// diagnoses also remain first-class executor evidence in the final outcome.
		apps := map[string]bool{}
		for _, application := range result.PlanApplications {
			apps[application.ID] = true
		}
		for _, application := range prior.PlanApplications {
			if !apps[application.ApplicationId] {
				result.PlanApplications = append(result.PlanApplications, planApplicationState(application))
			}
		}
		// The agent's loop starts from the stage's runs, so a stage run may already be recorded.
		recorded := make([]*contracts.ProgramRun, 0, len(result.ProgramRuns))
		for _, run := range result.ProgramRuns {
			recorded = append(recorded, programRunContract(run))
		}
		for _, run := range prior.ProgramRuns {
			if !slices.ContainsFunc(recorded, func(seen *contracts.ProgramRun) bool { return proto.Equal(seen, run) }) {
				result.ProgramRuns = append(result.ProgramRuns, programRunState(run))
				recorded = append(recorded, run)
			}
		}
		seenCalls := map[ToolCall]bool{}
		for _, call := range result.ProgramToolCalls {
			seenCalls[call] = true
		}
		for _, call := range prior.ProgramToolCalls {
			recorded := toolCallState(call)
			if !seenCalls[recorded] {
				result.ProgramToolCalls = append(result.ProgramToolCalls, recorded)
				seenCalls[recorded] = true
			}
		}
	}
	return result, nil
}

// Only controller-completed automatic response has a deliberate fresh
// reflection path. A missing worker session never grants that fallback.
func stageOnlyClosure(closure IncidentClosure) bool {
	return closure.Result != nil && closure.Result.ResolutionPath == "deterministic" && !hasResponderSession(closure) && len(closure.Request.PriorStages) > 0
}

// Resource ownership is unique even when several findings describe one repair.
func mergeStageCause(causes []ConfirmedRootCause, cause ConfirmedRootCause) []ConfirmedRootCause {
	for i := 0; i < len(causes); {
		overlap := false
		for _, left := range cause.Resources {
			for _, right := range causes[i].Resources {
				overlap = overlap || left == right
			}
		}
		if !overlap {
			i++
			continue
		}
		for _, ref := range causes[i].Resources {
			found := false
			for _, known := range cause.Resources {
				found = found || known == ref
			}
			if !found {
				cause.Resources = append(cause.Resources, ref)
			}
		}
		mergeEarlyCauseEvidence(&cause, causes[i])
		causes = append(causes[:i], causes[i+1:]...)
		i = 0
	}
	return append(causes, cause)
}

// addressedFindings are the incident findings an applied attempt was matched
// against, primary first: those that name its playbook or sit on an object it
// bound lead and, when any exist, are the whole answer. An unrelated finding
// that merely cleared beside the repair never speaks for its cause.
func addressedFindings(attempt *contracts.StageMitigation, findings []sdk.Finding) []sdk.Finding {
	covered := map[string]bool{}
	for _, fp := range attempt.GetClearedFindingFingerprints() {
		covered[fp] = true
	}
	for _, fp := range attempt.GetRemainingFindingFingerprints() {
		covered[fp] = true
	}
	bound := map[sdk.ObjectRef]bool{}
	for _, ref := range programBindingsState(attempt.GetBindings()).Objects {
		bound[ref] = true
	}
	var primary, rest []sdk.Finding
	for _, finding := range findings {
		switch {
		case slices.Contains(finding.Playbooks, attempt.GetPlaybookId()) || bound[finding.PrimaryResource]:
			primary = append(primary, finding)
		case covered[FindingFingerprint(finding)]:
			rest = append(rest, finding)
		}
	}
	if len(primary) > 0 {
		return primary
	}
	return rest
}

// liveJoined are the user tests that joined the open incident, with their
// findings, as the controller holds them now.
func (c *Controller) liveJoined(incidentID string) ([]string, []sdk.Finding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	live := c.currentIncidentRequest
	if live == nil || live.IncidentID != incidentID || len(live.JoinedKeys) == 0 {
		return nil, nil
	}
	var findings []sdk.Finding
	for _, finding := range live.Findings {
		if slices.Contains(live.JoinedKeys, findingKey(finding)) {
			findings = append(findings, finding)
		}
	}
	return slices.Clone(live.JoinedKeys), findings
}
