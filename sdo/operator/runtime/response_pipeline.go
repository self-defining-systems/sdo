package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
		trust, err := d.programTrust(ctx, request)
		if err != nil {
			return err
		}
		loop.Trust = trust
		return d.restoreToolState(ctx, request, loop)
	}}
}

// Before automatic execution only the incident intent is durable. Freezing a
// worker task here could overwrite its external payload at the later handoff.
func (c *Controller) automaticResponsePending(request *contracts.IncidentRequest) bool {
	if request == nil || len(request.PriorStages) != 0 {
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

func (c *Controller) runResponsePipeline(ctx context.Context, request IncidentRequest) (IncidentResult, []*contracts.StageReport, error) {
	// This is the exact original dispatch path for baseline experiments, including
	// its frozen task, result and error semantics.
	if len(c.codePath.Selection.ResponseStages) == 1 && c.codePath.Selection.ResponseStages[0] == "agent" {
		result, err := c.dispatcher.Dispatch(ctx, request)
		return result, nil, err
	}
	// A durable agent handoff transfers ownership for the rest of this incident.
	// Transport rejoin and controller continuations must collect that worker's
	// result, never reclassify its repairs as an automatic-only completion.
	if request.PreparedAgentTask != "" && len(request.PriorStages) > 0 {
		c.mu.Lock()
		c.responseStagesRunning = false
		c.mu.Unlock()
		result, err := c.dispatcher.Dispatch(ctx, request)
		state := &IncidentState{Request: request, PriorStages: cloneStageReports(request.PriorStages), Result: &result}
		merged, encodeErr := responsePipelineResult(state, result.Timing.StartedAt, result.Timing.CompletedAt)
		if err == nil {
			err = encodeErr
		}
		return merged, cloneStageReports(request.PriorStages), err
	}
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...), Coverage: cloneCoverage(request.PlaybookCoverage)}
	var stages []ResponseStage
	automaticAvailable, agentConfigured := false, false
	for _, name := range c.codePath.Selection.ResponseStages {
		switch name {
		case "deterministic":
			if source, ok := c.dispatcher.(interface{ DeterministicResponseStage() ResponseStage }); ok {
				if stage := source.DeterministicResponseStage(); stage != nil {
					stages = append(stages, stage)
					automaticAvailable = true
				}
			}
		case "agent":
			agentConfigured = true
			stages = append(stages, AgentResponseStage{Dispatcher: c.dispatcher, Prepare: func(ctx context.Context, request IncidentRequest) (IncidentRequest, error) {
				c.mu.Lock()
				if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == state.Request.IncidentID {
					c.currentIncidentRequest.PlaybookCoverage = cloneCoverage(state.Coverage)
					c.currentIncidentRequest.InitialProgramRuns = append([]ProgramRun(nil), state.Request.InitialProgramRuns...)
				}
				c.mu.Unlock()
				return c.prepareResponseStageHandoff(ctx, request)
			}})
		}
	}
	// A transport without an automatic stage preserves its original frozen
	// request and all original early-closure behavior.
	if !automaticAvailable && agentConfigured {
		c.mu.Lock()
		c.responseStagesRunning = false
		c.mu.Unlock()
		result, err := c.dispatcher.Dispatch(ctx, request)
		return result, nil, err
	}
	started := time.Now().UTC()
	err := RunResponseStages(ctx, state, stages)
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == request.IncidentID {
		c.currentIncidentRequest.PriorStages = mergeLaterStagePasses(state.PriorStages, c.currentIncidentRequest.PriorStages)
		c.currentIncidentRequest.PlaybookCoverage = cloneCoverage(state.Coverage)
		c.currentIncidentRequest.InitialProgramRuns = append([]ProgramRun(nil), state.Request.InitialProgramRuns...)
	}
	c.mu.Unlock()
	result, encodeErr := responsePipelineResult(state, started, time.Now().UTC())
	if err == nil {
		err = encodeErr
	}
	return result, cloneStageReports(state.PriorStages), err
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
			if attempt.Result != "applied_cleared" {
				continue
			}
			cleared := map[string]bool{}
			for _, fp := range attempt.ClearedFindingFingerprints {
				cleared[fp] = true
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
			for _, finding := range state.Request.Findings {
				fp := FindingFingerprint(finding)
				if !cleared[fp] || seenCause[fp] {
					continue
				}
				seenCause[fp] = true
				if cause.Summary == "" {
					cause.Summary = finding.Summary
				}
				addRef(finding.PrimaryResource)
				mergeEarlyCauseEvidence(&cause, ConfirmedRootCause{ExplainedDetectors: []string{finding.DetectorID}, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}}})
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
		for _, run := range prior.ProgramRuns {
			result.ProgramRuns = append(result.ProgramRuns, programRunState(run))
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

// The task including prior stages is frozen in the controller's durable state
// before the dispatcher can create a worker. State saves share one CAS writer.
func (c *Controller) prepareResponseStageHandoff(ctx context.Context, request IncidentRequest) (IncidentRequest, error) {
	prepared := request
	if preparer, ok := c.dispatcher.(interface {
		PrepareDispatch(context.Context, IncidentRequest) (IncidentRequest, error)
	}); ok {
		var err error
		prepared, err = preparer.PrepareDispatch(ctx, request)
		if err != nil {
			return IncidentRequest{}, fmt.Errorf("prepare stage handoff: %w", err)
		}
	}
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != request.IncidentID {
		c.mu.Unlock()
		return IncidentRequest{}, ErrStateConflict
	}
	c.currentIncidentRequest.PriorStages = cloneStageReports(request.PriorStages)
	c.currentIncidentRequest.PreparedAgentTask = prepared.PreparedAgentTask
	c.currentIncidentRequest.MaxResponseLoops = prepared.MaxResponseLoops
	c.mu.Unlock()
	if err := c.PersistState(ctx); err != nil {
		return IncidentRequest{}, fmt.Errorf("persist stage handoff: %w", err)
	}
	c.mu.Lock()
	c.responseStagesRunning = false
	c.mu.Unlock()
	return prepared, nil
}
