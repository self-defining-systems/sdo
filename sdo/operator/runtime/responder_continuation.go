package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/sdk"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
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
		verification := loop.Handle(ctx, ToolRequest{Command: "verify"})
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
			verification = loop.Handle(ctx, ToolRequest{Command: "verify"})
		}
		if err := ctx.Err(); err != nil {
			return IncidentResult{}, err
		}
		state.Attempts++
		result = accumulateResponderResult(state.Result, result)
		result.VerificationEvidence = append(result.VerificationEvidence, VerificationEvidence{Name: "controller-response-verification", Passed: verification.Healthy, Details: verification.Summary, ObservedAt: now().UTC()})
		limit := loop.MaxBatches
		if request.MaxResponseLoops > 0 && request.MaxResponseLoops < limit {
			limit = request.MaxResponseLoops
		}
		exhausted := state.Attempts >= limit || loop.Exhausted() || !now().Before(request.ResponseDeadline)
		state.Done = verification.Healthy || exhausted
		if verification.Healthy {
			result.Status = IncidentCompleted
			result.FinalDetectorStates = verification.DetectorStates
		} else if exhausted {
			result.Status = IncidentFailed
			result.ResponseLoopExhausted = true
			result.Error = "response deadline or loop budget exhausted with unresolved findings"
		}
		encoded, err := continuationFeedback(state.Attempts, verification, loop.continuationRepairs(result)...)
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
	current.RepairChanges = append(prior.RepairChanges, current.RepairChanges...)
	current.ConfirmedRootCauses = append(prior.ConfirmedRootCauses, current.ConfirmedRootCauses...)
	current.AppliedPlaybooks = append(prior.AppliedPlaybooks, current.AppliedPlaybooks...)
	current.VerificationEvidence = append(prior.VerificationEvidence, current.VerificationEvidence...)
	if prior.Timing.StartedAt.Before(current.Timing.StartedAt) {
		current.Timing.StartedAt = prior.Timing.StartedAt
	}
	target := current.Usage.ProtoReflect()
	source := prior.Usage.ProtoReflect()
	fields := source.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if field.HasPresence() && (!source.Has(field) || !target.Has(field)) {
			target.Clear(field) // an unknown component cannot become a known total
			continue
		}
		switch field.Kind() {
		case protoreflect.Int64Kind:
			target.Set(field, protoreflect.ValueOfInt64(target.Get(field).Int()+source.Get(field).Int()))
		case protoreflect.DoubleKind:
			target.Set(field, protoreflect.ValueOfFloat64(target.Get(field).Float()+source.Get(field).Float()))
		}
	}
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
	if !state.Done {
		return true, nil
	} // replay of a controller transition after a crash
	feedback, err := continuationFeedback(state.Attempts, verification, loop.continuationRepairs(incidentResultState(state.Result))...)
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
	ActionID    string          `json:"action_id"`
	Kind        string          `json:"kind"`
	Target      string          `json:"target"`
	Resources   []sdk.ObjectRef `json:"resources"`
	Summary     string          `json:"summary"`
	Details     string          `json:"details"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt time.Time       `json:"completed_at"`
	Success     bool            `json:"success"`
	Reversible  bool            `json:"reversible"`
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
	applications := l.PlanApplications()
	// Controller-produced receipts take precedence over provider-authored data.
	for _, application := range applications {
		for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{application}, nil) {
			repairs = append(repairs, continuationRepair{Kind: "program-application", ActionID: receipt.ActionID, ApplicationID: application.ID, ProgramIDs: application.ProgramIDs, Summary: receipt.Summary, RolledBack: application.RolledBack, Error: application.Error, Receipt: feedbackReceipt(receipt)})
			seen[receipt.ActionID] = true
		}
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
	return repairs
}
func feedbackReceipt(receipt RepairActionReceipt) continuationReceipt {
	return continuationReceipt{ActionID: receipt.ActionID, Kind: receipt.Kind, Target: receipt.Target, Resources: receipt.Resources, Summary: receipt.Summary, Details: receipt.Details, StartedAt: receipt.StartedAt, CompletedAt: receipt.CompletedAt, Success: receipt.Success, Reversible: receipt.Reversible}
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
	firing := map[string]bool{}
	var errors, pending []string
	for _, id := range ids {
		var latest *DetectorEvaluation
		for i := range states {
			if states[i].DetectorID == id {
				latest = &states[i]
				break
			}
		}
		if latest == nil || (len(c.healthDetectorIDs) > 0 && latest.EvaluatedAt.Before(c.responderCompletedAt)) {
			pending = append(pending, id)
			continue
		}
		if latest.Status == DetectorEvaluationFiring {
			firing[id] = true
		}
		if latest.Status == DetectorEvaluationError {
			errors = append(errors, id+": "+latest.Error)
		}
	}
	for _, state := range c.tracker.Snapshot() {
		if state.Active && firing[state.DetectorID] {
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
	driftClear, driftFindings := c.verifyClosureDrift(ctx, time.Now().UTC(), snapshot)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		return ToolResponse{Command: "verify", Error: "incident is no longer current"}
	}
	_, health := c.finalVerificationStates()
	result := c.continuationVerificationLocked(health, driftClear, driftFindings)
	if c.currentIncidentRequest.ObservationGeneration != observationGeneration(snapshot) {
		result.Healthy = false
		result.Error = "observation scope changed; incident evidence superseded"
	}
	return result
}
