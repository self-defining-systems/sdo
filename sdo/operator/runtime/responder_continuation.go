package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/sdk"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

// Response continuation is controller state, separate from provider sessions and
// apply batches. Persist it before dispatch so restart rejoins the same task.
type responderContinuation struct {
	Attempts int
	Feedback string
	Result   *contracts.IncidentResult
	Done     bool
}

type agentTaskExecutor interface {
	Execute(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)
}

func (d KubernetesJobDispatcher) executeResponder(ctx context.Context, executor worker_registry.KubernetesExecutor, task *contracts.AgentTask, loop *ResponderLoop, request IncidentRequest) (IncidentResult, error) {
	if loop == nil {
		return d.executeResponderTask(ctx, executor, task)
	}
	return d.continueResponder(ctx, executor, task, loop, request, time.Now)
}

func (d KubernetesJobDispatcher) executeResponderTask(ctx context.Context, executor agentTaskExecutor, task *contracts.AgentTask) (IncidentResult, error) {
	result, err := executor.Execute(ctx, task)
	if err != nil {
		return IncidentResult{}, err
	}
	return incidentResultFromAgent(result, task, d.Registry)
}

func (d KubernetesJobDispatcher) continueResponder(ctx context.Context, executor worker_registry.KubernetesExecutor, original *contracts.AgentTask, loop *ResponderLoop, request IncidentRequest, now func() time.Time) (IncidentResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return IncidentResult{}, err
		}
		loop.mu.Lock()
		state := loop.continuation
		loop.mu.Unlock()
		if state.Done && state.Result != nil {
			return incidentResultState(state.Result), nil
		}
		if state.Result != nil && !now().Before(request.ResponseDeadline) {
			result := incidentResultState(state.Result)
			result.Status, result.ResponseLoopExhausted, result.Error = IncidentFailed, true, "original response deadline exhausted with unresolved findings"
			state.Done = true
			var err error
			state.Result, err = incidentResultContract(result)
			if err != nil {
				return IncidentResult{}, err
			}
			if err := loop.saveContinuation(ctx, state); err != nil {
				return IncidentResult{}, err
			}
			return result, nil
		}
		task := proto.Clone(original).(*contracts.AgentTask)
		if state.Attempts > 0 {
			task.TaskId = fmt.Sprintf("%s-continue-%d", original.GetTaskId(), state.Attempts)
			executor.Environment["SDO_RESPONSE_CONTINUATION"] = state.Feedback
		}
		payload, err := wire.Canonical(task)
		if err != nil {
			return IncidentResult{}, err
		}
		executor.TaskPayload = string(payload)
		result, err := d.executeResponderTask(ctx, executor, task)
		if err != nil {
			return IncidentResult{}, err
		}
		result.RepairActions = loop.checkDeletionReceipts(ctx, result.RepairActions)
		result = accumulateResponderResult(state.Result, result)
		verify := func() ToolResponse {
			if loop.VerifyResult != nil {
				return loop.VerifyResult(ctx, result)
			}
			return loop.Handle(ctx, ToolRequest{Command: "verify"})
		}
		verification := verify()
		// Clearing persistence is observation work, not another response turn.
		// Wait for the controller rather than re-prompting without a named fault.
		for !verification.Healthy && len(verification.Findings) == 0 && verification.Error == "" && now().Before(request.ResponseDeadline) {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return IncidentResult{}, ctx.Err()
			case <-timer.C:
			}
			verification = verify()
		}
		if err := ctx.Err(); err != nil {
			return IncidentResult{}, err
		}
		result.UnresolvedFindings = append([]sdk.Finding(nil), verification.Findings...)
		state.Attempts++
		result.ResponseLoopsUsed = state.Attempts
		result.VerificationEvidence = append(result.VerificationEvidence, VerificationEvidence{Name: "controller-response-verification", Passed: verification.Healthy, Details: verification.Summary, ObservedAt: now().UTC()})
		limit := loop.MaxBatches
		if request.MaxResponseLoops > 0 && request.MaxResponseLoops < limit {
			limit = request.MaxResponseLoops
		}
		exhausted := state.Attempts >= limit || loop.Exhausted() || !now().Before(request.ResponseDeadline)
		repairs := loop.continuationRepairs(result)
		noProgress := responderNoProgress(state.Feedback, verification, repairs)
		deniedNoProgress := !verification.Healthy && verification.Error == "" && authorizationDeniedNoProgress(state.Feedback, verification, repairs)
		sharedOwnership := sharedOwnershipBlocker(verification, repairs)
		state.Done = verification.Healthy || exhausted || noProgress || deniedNoProgress || sharedOwnership
		if verification.Healthy {
			result.Status = IncidentCompleted
			result.FinalDetectorStates = verification.DetectorStates
		} else if sharedOwnership {
			result.Status = IncidentFailed
			result.Error = "shared_ownership"
		} else if deniedNoProgress {
			result.Status = IncidentFailed
			result.Error = "authorization_denied_no_progress"
		} else if noProgress {
			result.Status = IncidentFailed
			result.Error = "no_progress"
		} else if exhausted {
			result.Status = IncidentFailed
			result.ResponseLoopExhausted = true
			result.Error = "response deadline or loop budget exhausted with unresolved findings"
		}
		encoded, err := continuationFeedback(state.Attempts, verification, repairs...)
		if err != nil {
			return IncidentResult{}, err
		}
		state.Feedback = string(encoded)
		state.Result, err = incidentResultContract(result)
		if err != nil {
			return IncidentResult{}, err
		}
		if err := loop.saveContinuation(ctx, state); err != nil {
			return IncidentResult{}, err
		}
		if state.Done {
			return result, nil
		}
	}
}

type sdkFindingFeedback struct {
	Resource    sdk.ObjectRef `json:"primary_resource"`
	Fingerprint string        `json:"fingerprint"`
	RuleID      string        `json:"rule_id"`
	DetectorID  string        `json:"detector_id"`
	Summary     string        `json:"summary"`
	Evidence    string        `json:"evidence"`
}

func accumulateResponderResult(previous *contracts.IncidentResult, current IncidentResult) IncidentResult {
	if previous == nil {
		return current
	}
	prior := incidentResultState(previous)
	// Resumed workers retain prior receipts. Merge by action identity so actions
	// are never replayed or credited twice; usage is incremental per CLI turn.
	seen := map[string]bool{}
	for _, action := range current.RepairActions {
		seen[action.ActionID] = true
	}
	for _, action := range prior.RepairActions {
		if !seen[action.ActionID] {
			current.RepairActions = append(current.RepairActions, action)
		}
	}
	seenDrift := map[string]bool{}
	for _, event := range current.DriftEvents {
		seenDrift[driftEventKey(event)] = true
	}
	for _, event := range prior.DriftEvents {
		if !seenDrift[driftEventKey(event)] {
			current.DriftEvents = append(current.DriftEvents, event)
		}
	}
	current.RepairChanges = append(prior.RepairChanges, current.RepairChanges...)
	current.ConfirmedRootCauses = append(prior.ConfirmedRootCauses, current.ConfirmedRootCauses...)
	current.AppliedPlaybooks = append(prior.AppliedPlaybooks, current.AppliedPlaybooks...)
	current.VerificationEvidence = append(prior.VerificationEvidence, current.VerificationEvidence...)
	if prior.Timing.StartedAt.Before(current.Timing.StartedAt) {
		current.Timing.StartedAt = prior.Timing.StartedAt
	}
	current.Usage = *memory.AddUsage(&prior.Usage, &current.Usage)
	return current
}

// ContinueAfterVerification lets the observation controller resume a response
// when its later independent verification contradicts the dispatcher's check.
// The request, workspace, provider checkpoint and original deadline stay fixed.
func (d KubernetesJobDispatcher) ContinueAfterVerification(ctx context.Context, request IncidentRequest, verification ToolResponse, now time.Time) (bool, error) {
	if verification.Healthy || (len(verification.Findings) == 0 && verification.Error == "") || d.LoopFactory == nil || !now.Before(request.ResponseDeadline) {
		return false, nil
	}
	loop := d.LoopFactory(request)
	if loop == nil {
		return false, nil
	}
	if err := d.restoreToolState(ctx, request, loop); err != nil {
		return false, err
	}
	state := loop.continuation
	limit := loop.MaxBatches
	if request.MaxResponseLoops > 0 && request.MaxResponseLoops < limit {
		limit = request.MaxResponseLoops
	}
	if state.Result == nil || state.Attempts >= limit || loop.Exhausted() {
		return false, nil
	}
	repairs := loop.continuationRepairs(incidentResultState(state.Result))
	switch state.Result.GetError() {
	case "authorization_denied_no_progress":
		return false, nil
	case "no_progress":
		if responderNoProgress(state.Feedback, verification, repairs) {
			return false, nil
		}
	case "shared_ownership":
		if sharedOwnershipBlocker(verification, repairs) {
			return false, nil
		}
	}
	if !state.Done {
		return true, nil
	} // replay of a controller transition after a crash
	feedback, err := continuationFeedback(state.Attempts, verification, repairs...)
	if err != nil {
		return false, err
	}
	state.Done = false
	state.Feedback = string(feedback)
	if err := loop.saveContinuation(ctx, state); err != nil {
		return false, err
	}
	return true, nil
}

func (l *ResponderLoop) saveContinuation(ctx context.Context, state responderContinuation) error {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	previous := l.continuation
	l.continuation = state
	if err := l.persistToolState(ctx); err != nil {
		l.continuation = previous
		return err
	}
	return nil
}

// Both continuation paths publish the same factual schema, including object and
// finding identity. A pending observation window is not an unhealthy finding.
type continuationReceipt struct {
	FailureReason string          `json:"failure_reason,omitempty"`
	ActionID      string          `json:"action_id"`
	Kind          string          `json:"kind"`
	Target        string          `json:"target"`
	Resources     []sdk.ObjectRef `json:"resources"`
	Summary       string          `json:"summary"`
	Details       string          `json:"details"`
	StartedAt     time.Time       `json:"started_at"`
	CompletedAt   time.Time       `json:"completed_at"`
	Success       bool            `json:"success"`
	Reversible    bool            `json:"reversible"`
}
type continuationRepair struct {
	Kind          string              `json:"kind"`
	ActionID      string              `json:"action_id"`
	ApplicationID string              `json:"application_id,omitempty"`
	ProgramIDs    []string            `json:"program_ids,omitempty"`
	Summary       string              `json:"summary"`
	RolledBack    bool                `json:"rolled_back,omitempty"`
	Error         string              `json:"error,omitempty"`
	Receipt       continuationReceipt `json:"receipt"`
}
type responderFeedback struct {
	Attempt     int                  `json:"attempt"`
	Healthy     bool                 `json:"healthy"`
	Error       string               `json:"error,omitempty"`
	Summary     string               `json:"summary"`
	StillBroken []sdkFindingFeedback `json:"still_broken"`
	AlreadyDone []continuationRepair `json:"already_done"`
}

func (l *ResponderLoop) continuationRepairs(result IncidentResult) []continuationRepair {
	repairs := make([]continuationRepair, 0)
	l.mu.Lock()
	previousFeedback := l.continuation.Feedback
	completed := l.applicationRepairsLocked()
	menuActions := map[string]MenuRoundExecution{}
	for _, executions := range l.menuExecutions {
		for _, execution := range executions {
			for _, receipt := range l.menuRepairs {
				if execution.ApplicationID != "" && strings.HasPrefix(receipt.ActionID, execution.ApplicationID+"-a") {
					menuActions[receipt.ActionID] = execution
				}
			}
		}
	}
	l.mu.Unlock()
	var previous responderFeedback
	if previousFeedback != "" {
		_ = json.Unmarshal([]byte(previousFeedback), &previous)
	}
	seen := map[string]bool{}
	// Controller-produced receipts take precedence over provider-authored data.
	for _, repair := range completed {
		repairs = append(repairs, repair)
		seen[repair.ActionID] = true
	}
	for _, receipt := range append(l.MenuRepairActions(), result.RepairActions...) {
		if seen[receipt.ActionID] {
			continue
		}
		seen[receipt.ActionID] = true
		repair := continuationRepair{Kind: "recorded-action", ActionID: receipt.ActionID, Summary: receipt.Summary, Receipt: feedbackReceipt(receipt)}
		if execution, ok := menuActions[receipt.ActionID]; ok {
			repair.Kind = "program-application"
			repair.ApplicationID = execution.ApplicationID
			if execution.ProgramID != "" {
				repair.ProgramIDs = []string{execution.ProgramID}
			}
			repair.RolledBack = execution.RolledBack
			repair.Error = execution.Error
		}
		repairs = append(repairs, repair)
	}
	for _, repair := range previous.AlreadyDone {
		if !seen[repair.ActionID] {
			repairs = append(repairs, repair)
			seen[repair.ActionID] = true
		}
	}
	// A recorded application summarizes all of its applied action resources.
	// Menu receipts remain durable evidence, but must not list that same repair
	// twice in the continuation. Preserve individual receipts when no application
	// summary exists, including restored feedback from older tool state.
	aggregateApplications := map[string]bool{}
	for _, repair := range repairs {
		if repair.ApplicationID != "" && repair.Receipt.Kind == "program-plan" {
			aggregateApplications[repair.ApplicationID] = true
		}
	}
	unique := repairs[:0]
	for _, repair := range repairs {
		if aggregateApplications[repair.ApplicationID] && repair.Receipt.Kind != "program-plan" {
			continue
		}
		unique = append(unique, repair)
	}
	return unique
}

// Caller holds l.mu. This projection contains immutable repair facts only;
// execution plans, live handles and rollback preimages never enter tool state.
func (l *ResponderLoop) applicationRepairsLocked() []continuationRepair {
	result := append([]continuationRepair(nil), l.completedRepairs...)
	seen := map[string]bool{}
	for _, repair := range result {
		seen[repair.ActionID] = true
	}
	for _, application := range l.applicationsLog {
		for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{application}, nil) {
			repair := continuationRepair{Kind: "program-application", ActionID: receipt.ActionID, ApplicationID: application.ID, ProgramIDs: application.ProgramIDs, Summary: receipt.Summary, RolledBack: application.RolledBack, Error: application.Error, Receipt: feedbackReceipt(receipt)}
			if seen[repair.ActionID] {
				for i := range result {
					if result[i].ActionID == repair.ActionID {
						result[i] = repair
					}
				}
			} else {
				result = append(result, repair)
				seen[repair.ActionID] = true
			}
		}
	}
	return result
}

func feedbackReceipt(receipt RepairActionReceipt) continuationReceipt {
	return continuationReceipt{ActionID: receipt.ActionID, Kind: receipt.Kind, Target: receipt.Target, Resources: receipt.Resources, Summary: receipt.Summary, Details: receipt.Details, StartedAt: receipt.StartedAt, CompletedAt: receipt.CompletedAt, Success: receipt.Success, Reversible: receipt.Reversible, FailureReason: receipt.FailureReason}
}

func continuationFeedback(attempt int, verification ToolResponse, repairs ...continuationRepair) ([]byte, error) {
	feedback := responderFeedback{Attempt: attempt, Healthy: verification.Healthy, Error: verification.Error, Summary: verification.Summary, StillBroken: []sdkFindingFeedback{}, AlreadyDone: append([]continuationRepair{}, repairs...)}
	for _, finding := range verification.Findings {
		feedback.StillBroken = append(feedback.StillBroken, sdkFindingFeedback{Resource: finding.PrimaryResource, Fingerprint: FindingFingerprint(finding), DetectorID: finding.DetectorID, RuleID: finding.RuleID, Summary: finding.Summary, Evidence: finding.Evidence})
	}
	return json.Marshal(feedback)
}

// Caller holds c.mu. Closure drift has already been freshly checked against
// accepted source. Health samples must be fresh before closure, but waiting for
// that observation must not reset responderCompletedAt or consume another Job.
func (c *Controller) continuationVerificationLocked(healthVerified, driftClear bool, driftFindings []sdk.Finding) ToolResponse {
	result := ToolResponse{Command: "verify", Healthy: healthVerified && driftClear, Findings: append([]sdk.Finding(nil), driftFindings...)}
	ids := append([]string(nil), c.healthDetectorIDs...)
	if len(ids) == 0 && c.currentIncidentRequest != nil {
		seen := map[string]bool{}
		for _, finding := range c.currentIncidentRequest.Findings {
			if !seen[finding.DetectorID] {
				ids = append(ids, finding.DetectorID)
				seen[finding.DetectorID] = true
			}
		}
	}
	states, _ := c.latestEvaluations(ids, time.Time{}, false)
	result.DetectorStates = states
	firing := map[string]map[string]bool{}
	var errors, pending []string
	for _, id := range ids {
		var latest *DetectorEvaluation
		for i := range states {
			if states[i].DetectorID == id {
				latest = &states[i]
				break
			}
		}
		if latest == nil || latest.Status == DetectorEvaluationPending || (len(c.healthDetectorIDs) > 0 && latest.EvaluatedAt.Before(c.responderCompletedAt)) {
			description := id
			if latest != nil && latest.Reason != "" {
				description += ": " + latest.Reason
			}
			pending = append(pending, description)
			continue
		}
		if latest.Status == DetectorEvaluationFiring {
			firing[id] = map[string]bool{}
			for _, fp := range latest.Fingerprints {
				firing[id][fp] = true
			}
		}
		if latest.Status == DetectorEvaluationError {
			errors = append(errors, id+": "+latest.Error)
		}
	}
	for _, state := range c.tracker.Snapshot() {
		if state.Active && firing[state.DetectorID][FindingFingerprint(state.Finding)] {
			result.Findings = append(result.Findings, state.Finding)
		} else if state.Active {
			for _, id := range ids {
				if id == state.DetectorID {
					pending = append(pending, id)
					break
				}
			}
		}
	}
	sortFindings(result.Findings)
	result.Error = strings.Join(errors, "; ")
	switch {
	case result.Healthy:
		result.Summary = "controller independently verified healthy"
	case len(result.Findings) > 0:
		result.Summary = "controller verification retains named unresolved findings"
	case result.Error != "":
		result.Summary = "controller detector verification failed: " + result.Error
	default:
		result.Summary = fmt.Sprintf("awaiting post-response clear evaluation or clearing confirmation for %v since %s", pending, c.responderCompletedAt.UTC().Format(time.RFC3339Nano))
	}
	return result
}

// responderVerification uses the closure checker while a responder is still
// running. Health/traffic persistence belongs to the observation controller;
// admitted drift is independently compared against a fresh snapshot, just as
// maybeCloseIncident does. A tool never advances the controller's lifecycle.
func (c *Controller) responderVerification(ctx context.Context, incidentID string) ToolResponse {
	return c.responderResultVerification(ctx, incidentID, nil)
}
func (c *Controller) responderResultVerification(ctx context.Context, incidentID string, result *IncidentResult) ToolResponse {
	c.observationMu.Lock()
	defer c.observationMu.Unlock()
	snapshot, err := c.provider.Snapshot(ctx)
	if err != nil {
		return ToolResponse{Command: "verify", Error: "create verification snapshot: " + err.Error()}
	}
	c.mu.Lock()
	current := c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID
	c.mu.Unlock()
	if !current {
		return ToolResponse{Command: "verify", Error: "incident is no longer current"}
	}
	driftClear, driftFindings := c.verifyClosureDriftWithResult(ctx, time.Now().UTC(), snapshot, result)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		return ToolResponse{Command: "verify", Error: "incident is no longer current"}
	}
	_, health := c.finalVerificationStates()
	response := c.continuationVerificationLocked(health, driftClear, driftFindings)
	if c.currentIncidentRequest.ObservationGeneration != observationGeneration(snapshot) {
		response.Healthy = false
		response.Error = "observation scope changed; incident evidence superseded"
	}
	return response
}
