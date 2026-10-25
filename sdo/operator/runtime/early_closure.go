package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
)

// Stability is deliberately not restored: a restarted observer must establish
// a new continuous window. A published fence and frozen closure are durable.
func (c *Controller) resetEarlyStability() {
	c.mu.Lock()
	c.closureStableSince = time.Time{}
	c.mu.Unlock()
}
func acquireClosureGuard(l *ResponderLoop) (*apihistory.MutationObservation, func(), bool) {
	noop := func() {}
	if l == nil {
		return nil, noop, true
	}
	if !l.operationMu.TryLock() {
		return nil, noop, false
	}
	if !l.mu.TryLock() {
		l.operationMu.Unlock()
		return nil, noop, false
	}
	release := func() { l.mu.Unlock(); l.operationMu.Unlock() }
	if l.gateError != nil {
		release()
		return nil, noop, false
	}
	var guard *apihistory.MutationObservation
	if l.MutationGate != nil {
		var err error
		guard, err = l.MutationGate.TryObserve()
		if err != nil || guard == nil {
			release()
			return nil, noop, false
		}
	}
	l.closureReceipts = nil
	if l.ExternalReceipts != nil {
		receipts, pending, revision, err := l.ExternalReceipts()
		if err != nil || pending {
			if guard != nil {
				guard.Close()
			}
			release()
			return nil, noop, false
		}
		l.closureReceipts = receipts
		if guard != nil {
			guard.Revision += "/" + revision
		}
	}
	return guard, func() {
		if guard != nil {
			guard.Close()
		}
		release()
	}, true
}

// The controller-owned receipts survive a missing final model report. Only an
// opening detector finding on a successfully repaired resource supplies a cause;
// the independent final detector states still decide whether it earns credit.
func earlyResultSnapshot(l *ResponderLoop, request IncidentRequest, at time.Time) *IncidentResult {
	result := &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted, Timing: TimingMetrics{StartedAt: at, CompletedAt: at}, ResolutionPath: "responder"}
	if l == nil {
		return result
	}
	result.ResponderSessionID = l.menuSession.GetProviderSessionId()
	result.ProgramToolCalls = append([]ToolCall(nil), l.calls...)
	result.ProgramRuns = append([]ProgramRun(nil), l.runs...)
	result.PlanApplications = append([]RecordedPlanApplication(nil), l.applicationsLog...)
	result.ProgramFailureSignals = append([]string(nil), l.failureSignals...)
	result.RepairActions = ProgramRepairReceipts(result.PlanApplications, nil)
	result.RepairActions = append(result.RepairActions, l.menuRepairs...)
	result.RepairActions = append(result.RepairActions, l.closureReceipts...)
	for _, repair := range l.completedRepairs {
		receipt := RepairActionReceipt{ActionID: repair.Receipt.ActionID, Kind: repair.Receipt.Kind, Target: repair.Receipt.Target, Resources: repair.Receipt.Resources, Summary: repair.Receipt.Summary, Details: repair.Receipt.Details, StartedAt: repair.Receipt.StartedAt, CompletedAt: repair.Receipt.CompletedAt, Success: repair.Receipt.Success, Reversible: repair.Receipt.Reversible, FailureReason: repair.Receipt.FailureReason}
		seen := false
		for _, prior := range result.RepairActions {
			seen = seen || prior.ActionID == receipt.ActionID
		}
		if !seen {
			result.RepairActions = append(result.RepairActions, receipt)
		}
	}
	for _, finding := range request.Findings {
		repaired := false
		for _, receipt := range result.RepairActions {
			for _, ref := range receipt.Resources {
				repaired = repaired || (receipt.Success && receipt.FailureReason == "" && ref == finding.PrimaryResource && !receipt.CompletedAt.After(at))
			}
		}
		if repaired {
			result.ConfirmedRootCauses = append(result.ConfirmedRootCauses, ConfirmedRootCause{Summary: finding.Summary, Resources: []sdk.ObjectRef{finding.PrimaryResource}, ExplainedDetectors: []string{finding.DetectorID}, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}}})
		}
	}
	return result
}
func (c *Controller) earlyReportPendingLocked() bool {
	return !c.incidentOpen && c.currentIncidentRequest != nil && c.pendingClosure != nil && c.pendingClosure.VerifiedWhileResponderRunning && c.pendingClosure.Request.IncidentID == c.currentIncidentRequest.IncidentID
}
func (c *Controller) finishEarlyReport(ctx context.Context, now time.Time) {
	c.mu.Lock()
	if !c.earlyReportPendingLocked() {
		c.mu.Unlock()
		return
	}
	done := c.responderDone
	if !done && now.Before(c.pendingClosure.VerifiedAt.Add(c.config.ResponderReportGrace)) {
		c.mu.Unlock()
		return
	}
	id := c.currentIncidentRequest.IncidentID
	cancel := c.responderCancel
	loop := c.responderLoop
	c.mu.Unlock()
	if !done {
		if stopper, ok := c.dispatcher.(interface {
			StopResponder(context.Context, string, int) error
		}); ok {
			attempts := 0
			if loop != nil {
				loop.mu.Lock()
				attempts = loop.continuation.Attempts
				loop.mu.Unlock()
			}
			if err := stopper.StopResponder(ctx, id, attempts); err != nil {
				c.reportBrokerError("end verified responder report grace", err)
				return
			}
		}
		if cancel != nil {
			cancel()
		}
	}
	c.mu.Lock()
	if c.earlyReportPendingLocked() && c.currentIncidentRequest.IncidentID == id {
		c.clearIncidentLocked()
	}
	c.mu.Unlock()
}

// Stop only this incident's worker Jobs, including its existing continuations.
// The mutation fence remains in force while Kubernetes terminates their Pods.
func (d KubernetesJobDispatcher) StopResponder(ctx context.Context, id string, attempts int) error {
	zero := int64(0)
	policy := metav1.DeletePropagationForeground
	for attempt := 0; attempt <= attempts; attempt++ {
		taskID := id
		if attempt > 0 {
			taskID = fmt.Sprintf("%s-continue-%d", id, attempt)
		}
		err := d.Client.BatchV1().Jobs(d.Namespace).Delete(ctx, IncidentJobName(taskID), metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func cloneVerification(response ToolResponse) ToolResponse {
	data, _ := json.Marshal(response)
	var cloned ToolResponse
	_ = json.Unmarshal(data, &cloned)
	return cloned
}
func (c *Controller) cacheVerificationLocked(response ToolResponse) {
	prior := map[string]bool{}
	if c.cachedVerification != nil {
		for _, finding := range c.cachedVerification.Findings {
			prior[FindingFingerprint(finding)] = true
		}
	}
	for _, finding := range response.Findings {
		if !prior[FindingFingerprint(finding)] {
			c.findingGeneration++
			break
		}
	}
	copy := cloneVerification(response)
	c.cachedVerification = &copy
	if c.currentIncidentRequest != nil {
		c.cachedIncidentID = c.currentIncidentRequest.IncidentID
	}
	if c.verificationChanged != nil {
		close(c.verificationChanged)
	}
	c.verificationChanged = make(chan struct{})
}
func (c *Controller) incidentVerificationWarning(id string) *apihistory.IncidentVerified {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == id && len(c.pendingClosure.FinalDetectorStates) > 0 {
		return apihistory.VerificationWarning(c.pendingClosure.VerifiedAt)
	}
	return nil
}

// Waiting observes publication by the controller; it never takes a fresh
// detector sample, advances a window, or holds an observation/responder lock.
func (c *Controller) cachedResponderVerification(ctx context.Context, id string, wait time.Duration) ToolResponse {
	if wait < 0 {
		return ToolResponse{Command: "verify", Error: "wait seconds must not be negative"}
	}
	c.mu.Lock()
	missing := c.cachedVerification == nil || c.cachedIncidentID != id
	c.mu.Unlock()
	if missing {
		c.mu.Lock()
		if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != id {
			c.mu.Unlock()
			return ToolResponse{Command: "verify", Error: "incident is no longer current"}
		}
		response := ToolResponse{Command: "verify", Summary: "awaiting controller observation", Findings: append([]sdk.Finding(nil), c.currentIncidentRequest.Findings...), DetectorStates: cloneEvaluations(c.currentIncidentRequest.DetectorHistory)}
		c.cacheVerificationLocked(response)
		c.mu.Unlock()
	}
	var timer *time.Timer
	var deadline <-chan time.Time
	if wait > 0 {
		timer = time.NewTimer(wait)
		deadline = timer.C
		defer timer.Stop()
	}
	c.mu.Lock()
	generation := c.findingGeneration
	c.mu.Unlock()
	for {
		c.mu.Lock()
		if c.cachedIncidentID != id {
			c.mu.Unlock()
			return ToolResponse{Command: "verify", Error: "incident is no longer current"}
		}
		response := cloneVerification(*c.cachedVerification)
		changed := c.verificationChanged
		ready := response.IncidentVerified != nil || c.findingGeneration != generation
		// A normally completed incident is already stably closed by its old path.
		if c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == id && len(c.pendingClosure.FinalDetectorStates) > 0 {
			response.IncidentVerified = apihistory.VerificationWarning(c.pendingClosure.VerifiedAt)
			ready = true
		}
		c.mu.Unlock()
		if wait == 0 || ready {
			return response
		}
		select {
		case <-ctx.Done():
			response.Error = ctx.Err().Error()
			return response
		case <-deadline:
			return response
		case <-changed:
		}
	}
}

func (c *Controller) cachedFindingView(id string) ToolResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cachedVerification != nil && c.cachedIncidentID == id {
		return cloneVerification(*c.cachedVerification)
	}
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == id {
		return ToolResponse{Findings: append([]sdk.Finding(nil), c.currentIncidentRequest.Findings...)}
	}
	return ToolResponse{}
}
