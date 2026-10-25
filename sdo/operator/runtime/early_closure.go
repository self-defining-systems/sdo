package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
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
	// MP exposes reservations through this optional, no-I/O interface. Keep the
	// closure guard usable before that implementation is integrated.
	if pending, ok := any(l).(interface{ hasPendingMitigationsLocked() bool }); ok {
		if pending.hasPendingMitigationsLocked() {
			release()
			return nil, noop, false
		}
	} else {
		for _, application := range l.applicationsLog {
			if application.Pending {
				release()
				return nil, noop, false
			}
		}
	}
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
	if !done && now.Before(c.pendingClosure.VerifiedAt.Add(c.config.ResponderReportGrace)) {
		c.mu.Unlock()
		return
	}
	id := c.currentIncidentRequest.IncidentID
	cancel := c.responderCancel
	loop := c.responderLoop
	attempts := max(0, c.currentIncidentRequest.MaxResponseLoops)
	c.mu.Unlock()
	// Stop scheduling new turns before taking the durable worker inventory.
	if cancel != nil {
		cancel()
	}
	// Dispatch completion alone does not establish Pod quiescence. Check even
	// when a cancellation completion arrives between termination retries.
	if stopper, ok := c.dispatcher.(interface {
		StopResponder(context.Context, string, int) error
	}); ok {
		if loop != nil {
			loop.mu.Lock()
			attempts = max(attempts, loop.continuation.Attempts)
			loop.mu.Unlock()
		}
		if err := stopper.StopResponder(ctx, id, attempts); err != nil {
			c.mu.Lock()
			c.earlyStopRetryAt = now.Add(time.Second)
			c.mu.Unlock()
			if !errors.Is(err, errResponderTerminating) {
				c.reportBrokerError("end verified responder report grace", err)
			}
			return
		}
	}
	c.mu.Lock()
	if c.earlyReportPendingLocked() && c.currentIncidentRequest.IncidentID == id {
		if !done {
			c.pendingClosure.ResponderCompletedAt = now.UTC()
		}
		c.clearIncidentLocked()
	}
	c.mu.Unlock()
}

var errResponderTerminating = errors.New("verified responder is still terminating")

// responderJobNames recovers durable continuation and request-conflict names.
// The tool journal also covers requests staged on the PVC, without ConfigMaps.
func (d KubernetesJobDispatcher) responderJobNames(ctx context.Context, id string, attempts int) (map[string]bool, error) {
	if d.Client == nil {
		return nil, errors.New("incident worker client is required")
	}
	store := lifecycleio.NewConfigMapStateStore(d.Client, d.Namespace, IncidentJobName(id)+"-tools-state")
	state, _, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("read responder termination journal: %w", err)
	}
	if payload := state.GetResponderToolStateJson(); len(payload) > 0 {
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
	requests, err := d.Client.CoreV1().ConfigMaps(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("enumerate responder requests: %w", err)
	}
	for _, request := range requests.Items {
		if !strings.HasPrefix(request.Name, "sdo-") || !strings.HasSuffix(request.Name, "-request") {
			continue
		}
		task := new(contracts.AgentTask)
		if err := protojson.Unmarshal([]byte(request.Data[jobRequestDataKey]), task); err != nil {
			return nil, fmt.Errorf("decode worker request %s: %w", request.Name, err)
		}
		if !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
			continue
		}
		incident := new(contracts.IncidentRequest)
		if err := task.GetRequest().UnmarshalTo(incident); err != nil {
			return nil, fmt.Errorf("decode incident request %s: %w", request.Name, err)
		}
		if incident.GetIncidentId() != id {
			continue
		}
		taskID := task.GetTaskId()
		if taskID != id {
			suffix, continuation := strings.CutPrefix(taskID, id+"-continue-")
			attempt, err := strconv.Atoi(suffix)
			if !continuation || err != nil || attempt <= 0 || strconv.Itoa(attempt) != suffix {
				return nil, fmt.Errorf("unexpected responder task identity %q", taskID)
			}
		}
		name := strings.TrimSuffix(request.Name, "-request")
		if name != IncidentJobName(taskID) && name != worker_registry.TaskJobName(task) {
			return nil, fmt.Errorf("responder request %s does not match task identity", request.Name)
		}
		names[name] = true
	}
	return names, nil
}

// Request termination for every owned worker, then independently check all Jobs
// and Pods. Keep the closure fence on uncertainty or asynchronous termination.
func (d KubernetesJobDispatcher) StopResponder(ctx context.Context, id string, attempts int) error {
	names, err := d.responderJobNames(ctx, id, attempts)
	if err != nil {
		return err
	}
	// Staged requests may have no request ConfigMap at their content-addressed
	// Job name. Every continuation still mounts the same narrow incident session.
	var sessionSubPath string
	if d.RepositoryPVC != "" {
		sessionSubPath, err = memory.IncidentSessionSubPath(d.RepositoryPVCSubPath, id)
		if err != nil {
			return fmt.Errorf("resolve responder session ownership: %w", err)
		}
	}
	ownsSession := func(spec corev1.PodSpec) bool {
		if sessionSubPath == "" {
			return false
		}
		volumes := map[string]bool{}
		for _, volume := range spec.Volumes {
			volumes[volume.Name] = volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == d.RepositoryPVC
		}
		containers := append(append([]corev1.Container(nil), spec.Containers...), spec.InitContainers...)
		for _, container := range containers {
			for _, mount := range container.VolumeMounts {
				if volumes[mount.Name] && mount.MountPath == "/sdo/session" && mount.SubPath == sessionSubPath && mount.SubPathExpr == "" {
					return true
				}
			}
		}
		return false
	}
	ownsPod := func(pod corev1.Pod) bool {
		if ownsSession(pod.Spec) {
			return true
		}
		for _, key := range []string{"sdo.dev/incident", "job-name", "batch.kubernetes.io/job-name"} {
			if names[pod.Labels[key]] {
				return true
			}
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Job" && names[owner.Name] {
				return true
			}
		}
		return false
	}
	terminalPod := func(pod corev1.Pod) bool {
		return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
	}
	jobs, err := d.Client.BatchV1().Jobs(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("enumerate responder Jobs: %w", err)
	}
	pods, err := d.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("enumerate responder Pods: %w", err)
	}
	for _, job := range jobs.Items {
		if ownsSession(job.Spec.Template.Spec) {
			names[job.Name] = true
		}
	}
	zero := int64(0)
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy}
	var failures []error
	for _, job := range jobs.Items {
		if names[job.Name] {
			if err := d.Client.BatchV1().Jobs(d.Namespace).Delete(ctx, job.Name, options); err != nil && !apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("stop responder Job %s: %w", job.Name, err))
			}
		}
	}
	for _, pod := range pods.Items {
		if ownsPod(pod) && !terminalPod(pod) {
			if err := d.Client.CoreV1().Pods(d.Namespace).Delete(ctx, pod.Name, options); err != nil && !apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("stop responder Pod %s: %w", pod.Name, err))
			}
		}
	}
	// Delete acceptance does not establish container termination.
	jobs, err = d.Client.BatchV1().Jobs(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		failures = append(failures, fmt.Errorf("verify responder Jobs: %w", err))
	}
	pods, err = d.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		failures = append(failures, fmt.Errorf("verify responder Pods: %w", err))
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	for _, job := range jobs.Items {
		if !names[job.Name] && !ownsSession(job.Spec.Template.Spec) {
			continue
		}
		terminal := job.Status.CompletionTime != nil
		for _, condition := range job.Status.Conditions {
			terminal = terminal || condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed)
		}
		if job.Status.Active > 0 || job.DeletionTimestamp != nil || !terminal {
			return errResponderTerminating
		}
	}
	for _, pod := range pods.Items {
		if ownsPod(pod) && !terminalPod(pod) {
			return errResponderTerminating
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
