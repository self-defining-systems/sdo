package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"strings"
	"time"

	"sdo.dev/operator/sdk"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

// The responder's single task record is controller state, separate from provider
// sessions and apply batches. Persist it before dispatch so a restart rejoins the
// same task: SDO sends the agent exactly one incident turn per incident.
type responderContinuation struct {
	Attempts int
	Result   *contracts.IncidentResult
	Done     bool
}

type agentTaskExecutor interface {
	Execute(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)
}

// executeResponder runs the incident's one agent task. The agent's own loop,
// guided by `sdo detect`, pursues the opening alerts; SDO never prompts it again
// for the same symptoms. When it returns before Recovered the controller
// escalates the incident.
func (d KubernetesJobDispatcher) executeResponder(ctx context.Context, executor worker_registry.KubernetesExecutor, task *contracts.AgentTask, loop *ResponderLoop, request IncidentRequest) (IncidentResult, error) {
	if loop == nil {
		return d.executeResponderTask(ctx, executor, task)
	}
	var state responderContinuation
	loop.locked(func() { state = loop.continuation })
	if state.Done && state.Result != nil {
		return incidentResultState(state.Result), nil
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
	result.RepairActions = goIssuedReceipts(result.RepairActions)
	verification := loop.Handle(ctx, ToolRequest{Command: "verify"})
	if loop.VerifyResult != nil {
		verification = loop.VerifyResult(ctx, result)
	}
	if verification.Error == "" {
		// The controller's check observes this turn's writes.
		loop.locked(loop.closeLoop)
	}
	if err := ctx.Err(); err != nil {
		return IncidentResult{}, err
	}
	result.UnresolvedFindings = append([]sdk.Finding(nil), verification.Findings...)
	result.ResponseLoopsUsed = 1
	result.VerificationEvidence = append(result.VerificationEvidence, VerificationEvidence{Name: "controller-response-verification", Passed: verification.Healthy, Details: verification.Summary, ObservedAt: lifecycleio.Now().UTC()})
	if verification.Healthy {
		result.Status = IncidentCompleted
		result.FinalDetectorStates = verification.DetectorStates
	}
	state.Attempts, state.Done = 1, true
	if state.Result, err = incidentResultContract(result); err != nil {
		return IncidentResult{}, err
	}
	if err := loop.saveContinuation(ctx, state); err != nil {
		return IncidentResult{}, err
	}
	return result, nil
}

func (d KubernetesJobDispatcher) executeResponderTask(ctx context.Context, executor agentTaskExecutor, task *contracts.AgentTask) (IncidentResult, error) {
	result, err := executor.Execute(ctx, task)
	if err != nil {
		return IncidentResult{}, err
	}
	return incidentResultFromAgent(result, task, d.Registry)
}

// accumulateResponderResult merges a later report into the receipts and usage
// of an earlier snapshot, by action identity, so no action is replayed or
// credited twice.
func accumulateResponderResult(previous *contracts.IncidentResult, current IncidentResult) IncidentResult {
	if previous == nil {
		return current
	}
	prior := incidentResultState(previous)
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
	ScriptDigest  string          `json:"script_digest,omitempty"`
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
	// Owners reach the outcome for cause attribution, not the responder.
	Owners []ResourceOwners `json:"-"`
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
	return continuationReceipt{ActionID: receipt.ActionID, Kind: receipt.Kind, Target: receipt.Target, Resources: receipt.Resources, Summary: receipt.Summary, Details: receipt.Details, StartedAt: receipt.StartedAt, CompletedAt: receipt.CompletedAt, Success: receipt.Success, Reversible: receipt.Reversible, FailureReason: receipt.FailureReason, ScriptDigest: receipt.ScriptDigest, Owners: receipt.Owners}
}

// Caller holds c.mu. Closure drift has already been freshly checked against
// accepted source. Old clear samples await freshness without another Job;
// named incomplete observations allow one bounded continuation.
func (c *Controller) continuationVerificationLocked(healthVerified bool, source closureSourceReport) ToolResponse {
	result := ToolResponse{Command: "verify", Healthy: healthVerified && len(source.Rollouts) == 0 && len(source.Effects) == 0, Findings: append([]sdk.Finding(nil), source.Rollouts...), Drift: append([]sdk.Finding(nil), source.Drift...)}
	for _, effect := range source.Effects {
		result.VerificationBlockers = append(result.VerificationBlockers, VerificationBlocker{DetectorID: "controller.recovery-effect", Reason: "awaiting disappearance of repaired workload child " + effect.PrimaryResource.Kind + "/" + effect.PrimaryResource.Name})
	}
	ids := append([]string(nil), c.healthDetectorIDs...)
	if c.currentIncidentRequest != nil {
		ids = c.recoveryDetectorIDs(*c.currentIncidentRequest)
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
		if latest == nil || latest.Status == DetectorEvaluationPending || (len(c.healthDetectorIDs) > 0 && latest.EvaluatedAt.Before(c.recoveryObservationEpochLocked())) {
			reason := "awaiting detector evaluation"
			if latest != nil {
				reason = latest.Reason
				if reason == "" {
					reason = "awaiting clearing confirmation or a fresh post-response evaluation"
				}
			}
			description := id + ": " + reason
			pending = append(pending, description)
			// Old clear samples need only a fresh observation; a pending or
			// absent detector names an actual verification blocker.
			if latest == nil || latest.Status == DetectorEvaluationPending {
				result.VerificationBlockers = append(result.VerificationBlockers, VerificationBlocker{DetectorID: id, Reason: description})
			}
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
			result.VerificationBlockers = append(result.VerificationBlockers, VerificationBlocker{DetectorID: id, Reason: latest.Error})
		}
	}
	for _, state := range c.tracker.Snapshot() {
		if state.Active && firing[state.DetectorID][FindingFingerprint(state.Finding)] {
			result.Findings = append(result.Findings, state.Finding)
		} else if state.Active {
			for _, id := range ids {
				if id == state.DetectorID {
					pending = append(pending, id)
					result.VerificationBlockers = append(result.VerificationBlockers, VerificationBlocker{DetectorID: id, Reason: "clearing confirmation pending"})
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
		if len(result.Drift) > 0 {
			result.Summary += fmt.Sprintf("; %d object(s) still differ from the accepted source: restore each or explain why it is intended before returning", len(result.Drift))
		}
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
	defer c.lockObservation("responder_verify")()
	if c.config.ExternalIncidentsOnly {
		return ToolResponse{Command: "verify", VerificationStatus: VerificationAwaitingResponderCompletion, Summary: "External incident: declare completion by returning your result. No detector verification is available."}
	}
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
	source := c.verifyClosureSource(ctx, lifecycleio.Now().UTC(), snapshot, result)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		return ToolResponse{Command: "verify", Error: "incident is no longer current"}
	}
	_, health := c.finalVerificationStates()
	response := c.continuationVerificationLocked(health, source)
	// A direct result check may discover contradictions, but only the running
	// controller publishes the continuous recovery proof consumed by tools.
	response.Healthy = response.Healthy && c.cachedVerification != nil && c.cachedIncidentID == incidentID && c.cachedVerification.Healthy
	if !response.Healthy && len(response.Findings) == 0 && response.Error == "" {
		response.Summary = "awaiting controller recovery stability proof"
	}
	return response
}
