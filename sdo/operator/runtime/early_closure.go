package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

// Stability is deliberately not restored: a restarted observer must establish
// a new continuous window. A published fence and frozen closure are durable.
func (c *Controller) resetEarlyStability() {
	c.mu.Lock()
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	c.mu.Unlock()
}

// responderClosureGuard owns the loop locks until release. Controller result
// projections must use this guard, never copy fields from a live loop directly.
type responderClosureGuard struct {
	observation *apihistory.MutationObservation
	loop        *ResponderLoop
}

func (g *responderClosureGuard) resultSnapshot(request IncidentRequest, at time.Time) *IncidentResult {
	return earlyResultSnapshotLocked(g.loop, request, at)
}

// closureGuardState says why a closure pass could or could not observe the loop.
type closureGuardState int

const (
	// closureGuardHeld: the guard is held and the pass observed the loop.
	closureGuardHeld closureGuardState = iota
	// closureGuardBusy: a short lock was in use, so this pass saw nothing. It
	// says nothing about safety: a write made meanwhile moves the mutation
	// revision the next observed pass compares, so the pass is skipped and the
	// stability window is left alone.
	closureGuardBusy
	// closureGuardUnsafe: a write is in flight or unsettled (a running
	// operation, a pending mitigation, a gate or receipt error), so recovery
	// cannot be proved now and the stability window restarts.
	closureGuardUnsafe
)

// acquireClosureGuard holds the loop for one closure pass. Responder reads
// (`sdo detect --wait` polls for minutes) hold operationMu for reading, so the
// guard shares it: only a writer, which takes operationMu exclusively, is a
// reason to call the loop unsafe.
func acquireClosureGuard(l *ResponderLoop) (*responderClosureGuard, func(), closureGuardState) {
	noop := func() {}
	if l == nil {
		return &responderClosureGuard{}, noop, closureGuardHeld
	}
	if !l.operationMu.TryRLock() {
		return nil, noop, closureGuardUnsafe
	}
	if !l.mu.TryLock() {
		l.operationMu.RUnlock()
		return nil, noop, closureGuardBusy
	}
	release := func() { l.mu.Unlock(); l.operationMu.RUnlock() }
	// MP exposes reservations through this optional, no-I/O interface. Keep the
	// closure guard usable before that implementation is integrated.
	if pending, ok := any(l).(interface{ hasPendingMitigationsLocked() bool }); ok {
		if pending.hasPendingMitigationsLocked() {
			release()
			return nil, noop, closureGuardUnsafe
		}
	} else {
		for _, application := range l.applicationsLog {
			if application.Pending {
				release()
				return nil, noop, closureGuardUnsafe
			}
		}
	}
	if l.gateError != nil {
		release()
		return nil, noop, closureGuardUnsafe
	}
	var guard *apihistory.MutationObservation
	if l.MutationGate != nil {
		var err error
		guard, err = l.MutationGate.TryObserve()
		if err != nil || guard == nil {
			release()
			return nil, noop, closureGuardUnsafe
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
			return nil, noop, closureGuardUnsafe
		}
		l.closureReceipts = receipts
		if guard != nil {
			guard.Revision += "/" + revision
		}
	}
	return &responderClosureGuard{observation: guard, loop: l}, func() {
		if guard != nil {
			guard.Close()
		}
		release()
	}, closureGuardHeld
}

// The controller-owned receipts survive a missing final model report. Only an
// opening detector finding on a successfully repaired resource supplies a cause;
// the independent final detector states still decide whether it earns credit.
func earlyResultSnapshot(l *ResponderLoop, request IncidentRequest, at time.Time) *IncidentResult {
	if l != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
	}
	return earlyResultSnapshotLocked(l, request, at)
}

// earlyResultSnapshotLocked is shared by the synchronized snapshot entry point
// and the closure guard, which already owns mu for its entire observation.
func earlyResultSnapshotLocked(l *ResponderLoop, request IncidentRequest, at time.Time) *IncidentResult {
	result := &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted, Timing: TimingMetrics{StartedAt: at, CompletedAt: at}, ResolutionPath: "responder"}
	if l == nil {
		return result
	}
	result.ResponderSessionID = l.menuSession.GetProviderSessionId()
	result.ProgramToolCalls = append([]ToolCall(nil), l.calls...)
	result.ProgramRuns = append([]ProgramRun(nil), l.runs...)
	result.PlanApplications = l.applicationEvidenceLocked()
	result.ProgramFailureSignals = append([]string(nil), l.failureSignals...)
	// Archived applications have no live preimages; retain their original
	// persisted receipts rather than reconstructing weaker evidence.
	result.RepairActions = ProgramRepairReceipts(l.applicationsLog, nil)
	result.RepairActions = append(result.RepairActions, l.menuRepairs...)
	result.RepairActions = append(result.RepairActions, l.closureReceipts...)
	for _, repair := range l.completedRepairs {
		receipt := repairActionFromContinuation(repair.Receipt)
		seen := false
		for _, prior := range result.RepairActions {
			seen = seen || prior.ActionID == receipt.ActionID
		}
		if !seen {
			result.RepairActions = append(result.RepairActions, receipt)
		}
	}
	causeByResource := map[sdk.ObjectRef]int{}
	for _, finding := range request.Findings {
		repaired := false
		for _, receipt := range result.RepairActions {
			for _, ref := range receipt.Resources {
				repaired = repaired || (receipt.Success && receipt.FailureReason == "" && ref == finding.PrimaryResource && !receipt.CompletedAt.After(at))
			}
		}
		if repaired {
			cause := ConfirmedRootCause{Summary: finding.Summary, Resources: []sdk.ObjectRef{finding.PrimaryResource}, ExplainedDetectors: []string{finding.DetectorID}, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}}}
			if index, ok := causeByResource[finding.PrimaryResource]; ok {
				mergeEarlyCauseEvidence(&result.ConfirmedRootCauses[index], cause)
			} else {
				causeByResource[finding.PrimaryResource] = len(result.ConfirmedRootCauses)
				result.ConfirmedRootCauses = append(result.ConfirmedRootCauses, cause)
			}
		}
	}
	return result
}
func (c *Controller) earlyReportPendingLocked() bool {
	return !c.incidentOpen && c.currentIncidentRequest != nil && c.pendingClosure != nil && c.pendingClosure.VerifiedWhileResponderRunning && c.pendingClosure.Request.IncidentID == c.currentIncidentRequest.IncidentID
}

func (c *Controller) earlyProofInvalidatedLocked() bool {
	return c.earlyReportPendingLocked() && !slices.Equal(c.currentIncidentRequest.JoinedKeys, c.pendingClosure.Request.JoinedKeys)
}

// A verified closure waits for a running session's final result, bounded by the
// response deadline, then retires it. Uncertain quiescence retries retain their
// own deadline. Mutation fencing and independently observed quiescence hold the
// outcome.
func (c *Controller) earlyReportWakeLocked() time.Time {
	if !c.earlyReportPendingLocked() {
		return time.Time{}
	}
	if !c.earlyStopRetryAt.IsZero() {
		return c.earlyStopRetryAt
	}
	if c.responderDone || c.config.WatchOnly {
		return c.clock()
	}
	return c.finalResultDeadlineLocked()
}

// finalResultDeadlineLocked is how long a verified closure waits for the
// running session's final result: the incident's response deadline (never
// before verification).
func (c *Controller) finalResultDeadlineLocked() time.Time {
	deadline := c.currentIncidentRequest.ResponseDeadline
	if deadline.Before(c.pendingClosure.VerifiedAt) {
		return c.pendingClosure.VerifiedAt
	}
	return deadline
}

func (c *Controller) finishEarlyReport(ctx context.Context, now time.Time) {
	c.mu.Lock()
	if !c.earlyReportPendingLocked() {
		c.mu.Unlock()
		return
	}
	if !c.earlyStopRetryAt.IsZero() && now.Before(c.earlyStopRetryAt) {
		c.mu.Unlock()
		return
	}
	done := c.responderDone
	// The session's final result is the one result SDO acts on: an earlier
	// structured result is provisional, and the changes made after it are in the
	// final one. Wait for the session to finish, bounded by the response
	// deadline.
	if !done && !c.config.WatchOnly && now.Before(c.finalResultDeadlineLocked()) {
		c.mu.Unlock()
		return
	}
	id := c.currentIncidentRequest.IncidentID
	cancel := c.responderCancel
	attempts := max(0, c.currentIncidentRequest.MaxResponseLoops)
	c.mu.Unlock()
	if err := c.stopResponderWork(ctx, id, attempts, cancel); err != nil {
		c.mu.Lock()
		c.earlyStopRetryAt = now.Add(time.Second)
		c.mu.Unlock()
		if !errors.Is(err, errResponderTerminating) {
			c.reportBrokerError("end verified responder report grace", err)
		}
		return
	}
	// Workers are now quiescent, so this includes their final atomic snapshot.
	usage, err := c.responderUsage(id)
	if err != nil {
		c.reportBrokerError("recover retired responder usage", err)
		return
	}
	c.mu.Lock()
	if c.earlyReportPendingLocked() && c.currentIncidentRequest.IncidentID == id {
		if usage != nil {
			c.pendingClosure.Result.Usage = *usage
		}
		if !done {
			c.pendingClosure.ResponderCompletedAt = now.UTC()
		}
		if c.earlyProofInvalidatedLocked() {
			err := c.reobserveClosureLocked(now, c.currentIncidentRequest)
			c.mu.Unlock()
			if err != nil {
				c.reportBrokerError("resume recovery after a joined alert", err)
			}
			return
		}
		c.clearIncidentLocked()
	}
	c.mu.Unlock()
}

// Stop scheduling turns before inventory and prove Job/Pod quiescence even
// when dispatch completion has already arrived. Both bounded lifecycle paths
// use this same termination protocol.
func (c *Controller) stopResponderWork(ctx context.Context, id string, attempts int, cancel context.CancelFunc) error {
	if cancel != nil {
		cancel()
	}
	if stopper, ok := c.dispatcher.(interface {
		StopResponder(context.Context, string, int) error
	}); ok {
		return stopper.StopResponder(ctx, id, attempts)
	}
	return nil
}

var errResponderTerminating = errors.New("verified responder is still terminating")

// responderJobNames recovers durable continuation and request-conflict names.
func (d KubernetesJobDispatcher) responderJobNames(ctx context.Context, id string, attempts int) (map[string]bool, error) {
	if d.Client == nil {
		return nil, errors.New("incident worker client is required")
	}
	store := lifecycleio.NewConfigMapStateStore(d.Client, d.Namespace, IncidentJobName(id)+"-tools-state")
	state, _, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("read responder termination journal: %w", err)
	}
	if packed := state.GetResponderToolStateJson(); len(packed) > 0 {
		payload, err := unpackToolState(packed)
		if err != nil {
			return nil, fmt.Errorf("read responder termination journal: %w", err)
		}
		var journal responderToolState
		if err := json.Unmarshal(payload, &journal); err != nil {
			return nil, fmt.Errorf("decode responder termination journal: %w", err)
		}
		if (journal.Version != 1 && journal.Version != 2) || journal.IncidentID != id || journal.Continuation.Attempts < 0 {
			return nil, errors.New("responder termination journal does not match incident")
		}
		attempts = max(attempts, journal.Continuation.Attempts)
	}
	names := map[string]bool{IncidentJobName(id): true}
	for attempt := 1; attempt <= attempts; attempt++ {
		names[IncidentJobName(fmt.Sprintf("%s-continue-%d", id, attempt))] = true
	}
	// The worker registry owns physical execution identity, including bound
	// standby Jobs whose names differ from their original/continuation task.
	executions, err := worker_registry.IncidentExecutionNames(ctx, d.Client, d.Namespace, d.RepositoryMountPath, id, false)
	if err != nil {
		return nil, fmt.Errorf("enumerate responder executions: %w", err)
	}
	for _, name := range executions {
		names[name] = true
	}
	return names, nil
}

// Request termination for every owned worker, then independently re-read the
// same inventory used by restart retirement. Delete acceptance is not quiescence.
func (d KubernetesJobDispatcher) StopResponder(ctx context.Context, id string, attempts int) error {
	// Investigators end before anything else: the closing turn and the seal come after this stop.
	investigatorErr := d.stopInvestigators(ctx, id)
	return errors.Join(investigatorErr, d.stopExecutions(ctx, id, attempts))
}

// stopExecutions deletes every Job and Pod of one session identity and re-reads until none is active.
func (d KubernetesJobDispatcher) stopExecutions(ctx context.Context, id string, attempts int) error {
	inventory, err := d.responderExecutions(ctx, id, attempts)
	if err != nil {
		return err
	}
	zero := int64(0)
	policy := metav1.DeletePropagationForeground
	options := func(uid types.UID) metav1.DeleteOptions {
		result := metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy}
		if uid != "" {
			result.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		return result
	}
	var failures []error
	for _, job := range inventory.jobs {
		if inventory.names[job.Name] {
			if err := d.Client.BatchV1().Jobs(d.Namespace).Delete(ctx, job.Name, options(job.UID)); err != nil && !apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("stop responder Job %s: %w", job.Name, err))
			}
		}
	}
	for _, pod := range inventory.pods {
		if inventory.ownsPod(pod) && !terminalResponderPod(pod) {
			if err := d.Client.CoreV1().Pods(d.Namespace).Delete(ctx, pod.Name, options(pod.UID)); err != nil && !apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("stop responder Pod %s: %w", pod.Name, err))
			}
		}
	}
	remaining, err := d.responderExecutions(ctx, id, attempts)
	if err != nil {
		failures = append(failures, err)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if remaining.active() {
		return errResponderTerminating
	}
	return nil
}

func cloneVerification(response ToolResponse) ToolResponse {
	data, _ := json.Marshal(response)
	var cloned ToolResponse
	_ = json.Unmarshal(data, &cloned)
	return cloned
}

// markNewSinceDispatchLocked names the findings that first activated after the
// responder launched. The controller runs one incident at a time, so such a
// finding may be a separate fault; the responder is told so it reports it as
// its own cause instead of folding it into this incident's diagnosis.
func (c *Controller) markNewSinceDispatchLocked(response *ToolResponse) {
	response.NewSinceDispatch = nil
	if c.incidentDispatchedAt.IsZero() {
		return
	}
	for _, finding := range response.Findings {
		if c.arrivedAfterDispatchLocked(finding) {
			response.NewSinceDispatch = append(response.NewSinceDispatch, finding)
		}
	}
	if count := len(response.NewSinceDispatch); count > 0 {
		response.Summary += fmt.Sprintf("; %d finding(s) new since dispatch (new_since_dispatch): possibly a separate fault, report it as its own cause; closure still waits on health", count)
	}
}

// markAlertSetLocked tells the responder what Recovered waits on when
// alerts joined after dispatch.
func (c *Controller) markAlertSetLocked(response *ToolResponse) {
	if c.currentIncidentRequest == nil || len(c.currentIncidentRequest.JoinedKeys) == 0 {
		return
	}
	if line := alertSetLine(c.currentIncidentRequest); line != "" {
		response.Summary += "; " + line
	}
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
	c.markNewSinceDispatchLocked(&copy)
	c.markAlertSetLocked(&copy)
	c.cachedVerification = &copy
	if c.currentIncidentRequest != nil {
		c.cachedIncidentID = c.currentIncidentRequest.IncidentID
	}
	if c.verificationChanged != nil {
		close(c.verificationChanged)
	}
	c.verificationChanged = make(chan struct{})
}

// pendingClosureVerifiedLocked reports a closure frozen at verification. An
// unconfirmed recheck keeps that verification; it only withholds final health.
func (c *Controller) pendingClosureVerifiedLocked(id string) bool {
	if c.earlyProofInvalidatedLocked() && c.currentIncidentRequest.IncidentID == id {
		return false
	}
	if c.pendingClosure != nil && !c.pendingClosure.Frozen && c.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED && c.cachedIncidentID == id && c.cachedVerification != nil && !c.cachedVerification.Healthy {
		return false
	}
	return c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == id &&
		(len(c.pendingClosure.FinalDetectorStates) > 0 || c.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED)
}

func (c *Controller) incidentVerificationWarning(id string) *apihistory.IncidentVerified {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingClosureVerifiedLocked(id) {
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
		timer = lifecycleio.NewTimer(wait)
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
		ready := response.VerificationStatus == VerificationAwaitingResponderCompletion || response.IncidentVerified != nil || c.findingGeneration != generation
		// A normally completed incident is already stably closed by its old path.
		if c.pendingClosureVerifiedLocked(id) {
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
