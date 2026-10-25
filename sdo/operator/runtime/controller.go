package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"log/slog"
	"math/rand/v2"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/core"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
)

type SnapshotProvider interface {
	Snapshot(context.Context) (sdk.DetectionContext, error)
}

type Dispatcher interface {
	Dispatch(context.Context, IncidentRequest) (IncidentResult, error)
}

type ControllerConfig struct {
	Application         string
	Namespace           string
	SourceCommit        string
	DeployedCommit      string
	RepositoryWorktree  string
	CorpusSessionRoot   string
	ResponseTimeout     time.Duration
	VerificationTimeout time.Duration
	FiringThreshold     int
	ClearThreshold      int
	BatchDebounce       time.Duration
	// ConfirmationInterval bounds how long a detector with a finding below its
	// firing threshold waits for its next evaluation. Zero leaves confirmation
	// to watch events and the detector's own interval.
	ConfirmationInterval time.Duration
	RepairPolicy         string
	// ClosureRetry bounds resubmission of a closure the broker rejects.
	ClosureRetry ClosureRetryPolicy
	// DispatchRetry bounds how quickly a transient dispatch failure
	// re-executes the dispatch effect.
	DispatchRetry DispatchRetryPolicy
	// GateConfirmation is the responder gate's clear hysteresis, published in
	// the incident view. Zero makes the gate follow closure's clear count.
	GateConfirmation GateConfirmationPolicy
}

type dispatchCompletion struct {
	incidentID string
	result     IncidentResult
	err        error
}

type Controller struct {
	config                      ControllerConfig
	startedAt                   time.Time
	scheduler                   *Scheduler
	detectors                   map[string]sdk.Detector
	tracker                     *FindingStateTracker
	batcher                     *Batcher
	provider                    SnapshotProvider
	dispatcher                  Dispatcher
	history                     []DetectorEvaluation
	drift                       *DriftControllerConfig
	driftEvents                 []sdk.DriftEvent
	sourceInventory             *SourceInventory
	pendingOnboarding           *contracts.NamespaceOnboarding
	onboardingClient            kubernetes.Interface
	onboardingControl           string
	namespaceOnboarding         *contracts.NamespaceOnboarding
	sourceInventoryError        string
	sourceInventoryRetryAt      time.Time
	sourceInventoryFindings     []sdk.Finding
	acceptedSourceDrift         []sdk.DriftEvent
	acceptedSourceDriftRevision string
	// Healthy capture state shares the observation loop's serialization.
	healthyMemoryCaptured   bool
	healthyMemoryBaselineAt time.Time
	healthyMemoryReference  string
	healthyMemoryCandidates map[time.Time]string
	// routingSweeps tracks background open@detect routing sweeps so tests can
	// join them before removing the capture directory.
	routingSweeps     sync.WaitGroup
	ownershipAdoption OwnershipAdoptionStatus
	adoptionNeeded    bool
	results           chan dispatchCompletion
	// gateClears and gateProbes hold the submit gate's confirmation of active
	// findings (GateConfirmationPolicy); they are never persisted, so a
	// restarted controller confirms again from fresh evaluations.
	gateClears    map[string]*gateClear
	gateProbes    map[string]time.Time
	stateStore    StateStore
	stateRevision string
	// stagedTaskIncident is the incident whose prepared task the last saved state references on the repository volume.
	stagedTaskIncident string

	// observationMu serializes tool closure checks with a complete controller
	// observation, including tracker and evaluation-history updates. Lock before mu.
	observationMu sync.Mutex
	mu            sync.Mutex
	// durable is the state stateStore last confirmed durable. With a store
	// attached, an effect runs only while this state records it, so a
	// restarted controller can recover every effect that ran.
	durable                *contracts.ControllerState
	incidentOpen           bool
	responderDone          bool
	currentIncidentRequest *IncidentRequest
	dispatchState          contracts.DispatchState
	incidentFindingKeys    []string
	cleanedHelpers         []string
	healthDetectorIDs      []string
	detectorSpecs          map[string]sdk.DetectorSpec
	currentIncidentResult  *IncidentResult
	dispatchError          string
	incidentDetectedAt     time.Time
	incidentDispatchedAt   time.Time
	responderCompletedAt   time.Time
	// detectorClearSince maps each detector to the start of its current
	// streak of clear evaluations; a firing or erroring evaluation ends
	// the streak. Guarded by mu.
	detectorClearSince         map[string]time.Time
	pendingClosure             *IncidentClosure
	closureState               contracts.ClosureState
	closureReceipt             *ClosureReceipt
	closureFailure             *ClosureFailure
	failedClosures             []*contracts.FailedIncidentClosure
	dispatchFailureAttempts    int
	dispatchNextRetryAt        time.Time
	lastAcknowledgedIncidentID string
	incidentView               *IncidentView
	broker                     IncidentBroker
	workspaceResults           chan workspaceCompletion
	closureResults             chan closureCompletion
	acknowledgmentResults      chan acknowledgmentCompletion

	// Baseline, when set, records healthy configuration and attaches the
	// diff against it to each new incident request.
	Baseline StateBaseline
	// Helpers, when set, deletes responder helper objects once the
	// responder completes, before recovery is verified.
	Helpers HelperCleaner
	// firingSink receives detector firing telemetry; nil disables it.
	firingSink FiringSink
	// lifecycleSink receives incident lifecycle telemetry; nil disables it.
	lifecycleSink LifecycleSink
	// evaluationIteration counts evaluation passes that took a snapshot. It is
	// durable so telemetry iterations keep increasing across restarts.
	evaluationIteration int
	// evaluationAt is the time of the evaluation pass in progress.
	evaluationAt time.Time
	// timeline summarizes each finding's firing history since the last incident
	// closure was cut.
	timeline      []DetectorTimelineEntry
	stateRestored bool

	// CheckHealth refuses evaluation and effects while accepted health policy is stale.
	CheckHealth              func(context.Context) error
	checkInvestigationHealth func(context.Context, string) error
	// ReconcileRendered is the Go-only deployment lock publication edge.
	ReconcileRendered func(context.Context, string) (string, error)
	OnError           func(error)
	OnResult          func(IncidentResult)
	OnEvaluation      func([]sdk.Finding)
	OnIncidentClosed  func(IncidentClosure)
	OnClosureFailed   func(ClosureFailure)
	CanAct            func() bool
	GuardAction       func(context.Context) (context.Context, context.CancelFunc, error)
	// now overrides the wall clock for closure and dispatch retry backoff in
	// tests.
	now func() time.Time
	// dispatchJitter overrides the dispatch retry jitter source in tests; it
	// must return a value in [0, 1). Nil defaults to a real random source.
	dispatchJitter func() float64
}

func NewController(
	config ControllerConfig,
	detectors []sdk.Detector,
	provider SnapshotProvider,
	dispatcher Dispatcher,
	start time.Time,
) (*Controller, error) {
	if provider == nil {
		return nil, fmt.Errorf("snapshot provider is required")
	}
	if dispatcher == nil {
		return nil, fmt.Errorf("dispatcher is required")
	}
	if config.FiringThreshold < 1 || config.ClearThreshold < 1 {
		return nil, fmt.Errorf("firing and clear thresholds must be positive")
	}
	if config.ResponseTimeout <= 0 {
		return nil, fmt.Errorf("response timeout must be positive")
	}
	if config.VerificationTimeout < 0 {
		return nil, fmt.Errorf("verification timeout must not be negative")
	}
	if config.ConfirmationInterval < 0 {
		return nil, fmt.Errorf("confirmation interval must not be negative")
	}
	if err := config.GateConfirmation.validate(); err != nil {
		return nil, err
	}
	if config.VerificationTimeout == 0 {
		config.VerificationTimeout = config.ResponseTimeout
	}
	if config.RepairPolicy == "" {
		config.RepairPolicy = "commit"
	}
	if config.RepairPolicy != "commit" && config.RepairPolicy != "recorded-actions" {
		return nil, fmt.Errorf("unsupported repair policy %q", config.RepairPolicy)
	}
	closureRetry, err := config.ClosureRetry.withDefaults()
	if err != nil {
		return nil, err
	}
	config.ClosureRetry = closureRetry
	dispatchRetry, err := config.DispatchRetry.withDefaults()
	if err != nil {
		return nil, err
	}
	config.DispatchRetry = dispatchRetry
	if err := core.ValidateDetectors(detectors); err != nil {
		return nil, err
	}
	healthDetectorIDs := make([]string, 0)
	detectorSpecs := make(map[string]sdk.DetectorSpec, len(detectors))
	byID := make(map[string]sdk.Detector, len(detectors))
	tracker := NewFindingStateTracker(config.FiringThreshold, config.ClearThreshold)
	for _, detector := range detectors {
		spec := detector.Spec()
		detectorSpecs[spec.ID] = spec
		byID[spec.ID] = detector
		if spec.Persistence.Firing > 0 && spec.Persistence.Clearing > 0 {
			tracker.SetPolicy(spec.ID, spec.Persistence.Firing, spec.Persistence.Clearing, spec.Persistence.MinDuration)
		}
		if spec.Class == sdk.DetectorClassHealth {
			healthDetectorIDs = append(healthDetectorIDs, spec.ID)
		}
	}
	sort.Strings(healthDetectorIDs)
	return &Controller{
		config: config, startedAt: start, scheduler: NewScheduler(detectors, start, config.Namespace), detectors: byID,
		tracker:    tracker,
		gateClears: make(map[string]*gateClear), gateProbes: make(map[string]time.Time),
		batcher: NewDebouncedBatcher(config.BatchDebounce), provider: provider, dispatcher: dispatcher,
		results: make(chan dispatchCompletion, 1), healthDetectorIDs: healthDetectorIDs,
		detectorSpecs:    detectorSpecs,
		workspaceResults: make(chan workspaceCompletion, 1), closureResults: make(chan closureCompletion, 1),
		acknowledgmentResults: make(chan acknowledgmentCompletion, 1),
		dispatchJitter:        rand.Float64,
	}, nil
}

func (c *Controller) Step(ctx context.Context, now time.Time, event *sdk.WatchKind) error {
	if event == nil {
		return c.StepEvents(ctx, now, nil)
	}
	return c.StepEvents(ctx, now, []sdk.WatchKind{*event})
}

// StepEvents evaluates, against one snapshot, every detector that is due or
// watches any of the coalesced watch events.
func (c *Controller) StepEvents(ctx context.Context, now time.Time, events []sdk.WatchKind) error {
	c.observationMu.Lock()
	defer c.observationMu.Unlock()
	if err := c.prepareHealthEffects(ctx, now); err != nil {
		return err
	}
	if err := c.retireExpiredIncident(ctx, now); err != nil {
		return err
	}
	if err := c.prepareSourceInventory(ctx); err != nil {
		return err
	}
	c.processDispatchCompletions(now)
	c.processBrokerCompletions()
	c.attachPendingDrift()
	c.mu.Lock()
	reconcileReady := !c.incidentOpen && (c.broker == nil || !c.closureBlocksNextLocked())
	c.mu.Unlock()
	if c.ReconcileRendered != nil && reconcileReady {
		actionCtx, cancel, err := c.actionContext(ctx)
		revision := c.CurrentAcceptedCommit()
		if err == nil {
			revision, err = c.ReconcileRendered(actionCtx, revision)
			cancel()
		}
		if err != nil {
			if c.OnError != nil {
				c.OnError(fmt.Errorf("reconcile rendered lock: %w", err))
			}
		}
		if revision != "" && revision != c.CurrentAcceptedCommit() {
			c.mu.Lock()
			c.config.SourceCommit = revision
			c.adoptionNeeded = true
			c.mu.Unlock()
		}
	}
	deferInventory := c.pendingOnboarding != nil
	if !deferInventory {
		c.observeSourceInventory(now)
	}
	detectors := c.scheduler.SelectEvents(now, events)
	probes := c.dueGateProbes(now, detectors)
	var objectChanges []ObjectChange
	if c.drift != nil {
		if c.drift.Events != nil {
			objectChanges = c.drift.Events.TakeObjectChanges()
		}

	}
	if len(detectors) == 0 && len(probes) == 0 && len(objectChanges) == 0 && !c.adoptionNeeded && !c.hasRetainedDrift() {
		c.forgetGate()
		c.maybeCloseIncident(ctx, now, nil)
		err := c.dispatchReady(ctx, now, nil)
		c.refreshIncidentView(now, false)
		return err
	}
	snapshot, err := c.provider.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("create detection snapshot: %w", err)
	}
	c.processDrift(ctx, now, snapshot, objectChanges)

	onboardingCandidate := c.beginOnboarding(ctx, detectors)
	sampleFindings := make([]sdk.Finding, 0)
	errored := false
	c.mu.Lock()
	c.evaluationIteration++
	c.evaluationAt = now.UTC()
	c.mu.Unlock()
	for _, detector := range detectors {
		spec := detector.Spec()
		findings, detectErr := sdk.DetectObserved(ctx, detector, snapshot)
		if detectErr != nil {
			c.resetGate(spec.ID)
			c.recordDetectorError(spec.ID, now, detectErr)
			errored = true
			continue
		}
		validFindings := make([]sdk.Finding, 0, len(findings))
		valid := true
		for _, finding := range findings {
			if finding.DetectorID == "" {
				finding.DetectorID = spec.ID
			}
			if len(finding.Playbooks) == 0 && len(spec.Playbooks) > 0 {
				finding.Playbooks = append([]string(nil), spec.Playbooks...)
			}
			finding.Fingerprint = FindingFingerprint(finding)
			if validationErr := core.ValidateFinding(spec, finding); validationErr != nil {
				c.resetGate(spec.ID)
				c.recordDetectorError(spec.ID, now, validationErr)
				valid = false
				errored = true
				break
			}
			validFindings = append(validFindings, finding)
		}
		if !valid {
			continue
		}
		changes := c.tracker.Observe(now, spec.ID, validFindings)
		c.observeGate(spec.ID, now, validFindings)
		sampleFindings = append(sampleFindings, validFindings...)
		c.recordEvaluation(spec.ID, now, validFindings)
		c.batcher.RemoveKeys(changes.Cleared)
		if c.config.ConfirmationInterval > 0 && c.tracker.HasPendingDetector(spec.ID) {
			c.scheduler.Expedite(spec.ID, now.Add(c.config.ConfirmationInterval))
		}
		for _, finding := range changes.Activated {
			if c.mergeIntoOpenIncident(finding) {
				continue
			}
			batchSpec := c.detectorSpecs[spec.ID]
			if batchSpec.Class != "" && !severityAtLeast(finding.Severity, batchSpec.Batching.Severity) {
				continue
			}
			if c.attachBeforeLaunch([]sdk.Finding{finding}) {
				continue
			}
			if batchSpec.Class == "" {
				c.batcher.AddAt(finding, now)
			} else {
				c.batcher.AddAtWithDebounce(finding, now, batchSpec.Batching.Debounce)
			}
		}
		c.recordFiring(spec, now, validFindings, changes)
	}
	c.finishOnboarding(ctx, onboardingCandidate, sampleFindings, errored, now)
	if deferInventory && c.pendingOnboarding == nil {
		c.observeSourceInventory(now)
	}
	c.probeGate(ctx, now, snapshot, probes)
	c.forgetGate()
	if len(detectors) > 0 {
		sort.Slice(sampleFindings, func(left int, right int) bool {
			return sampleFindings[left].Fingerprint < sampleFindings[right].Fingerprint
		})
		if c.OnEvaluation != nil {
			c.OnEvaluation(sampleFindings)
		}
		healthy := !errored && c.quiet() && c.completeHealthyEvaluation()
		if c.Baseline != nil {
			c.Baseline.Observe(now, healthy)
		}
		c.captureHealthyMemoryBaseline(ctx, now, snapshot, healthy)
	}
	if c.adoptionNeeded {
		c.mu.Lock()
		retryAt := c.ownershipAdoption.RetryAt
		retryCommit := c.ownershipAdoption.SourceCommit
		c.mu.Unlock()
		if retryCommit != c.config.SourceCommit || !now.Before(retryAt) {
			if err := c.adoptOwnership(ctx, now, snapshot); err != nil {
				c.recordAdoptionError(now, err)
				if c.OnError != nil {
					c.OnError(fmt.Errorf("ownership adoption: %w", err))
				}
			}
		}
	}

	c.maybeCloseIncident(ctx, now, snapshot)
	err = c.dispatchReady(ctx, now, snapshot)
	c.refreshIncidentView(now, true)
	return err
}

// quiet reports that nothing is wrong or pending: no incident or closure,
// no active or pending finding, and nothing waiting in the batcher. Only a
// quiet state may become the configuration baseline.
func (c *Controller) quiet() bool {
	c.mu.Lock()
	busy := c.incidentOpen || c.closureBlocksNextLocked()
	c.mu.Unlock()
	_, batched := c.batcher.Deadline()
	return !busy && !batched && c.tracker.Quiet()
}

// EvaluateAll evaluates every detector against one fresh snapshot, for example
// when observation resumes after a planned maintenance window.
func (c *Controller) EvaluateAll(ctx context.Context, now time.Time) error {
	c.adoptionNeeded = c.drift != nil && c.drift.Source != nil && c.drift.Client != nil
	c.scheduler.ExpediteAll(now)
	return c.StepEvents(ctx, now, nil)
}

func (c *Controller) mergeIntoOpenIncident(finding sdk.Finding) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		return false
	}
	key := FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
	finding.Fingerprint = FindingFingerprint(finding)
	for index, existing := range c.currentIncidentRequest.Findings {
		if FindingStateKey(existing.DetectorID, FindingFingerprint(existing)) == key {
			c.currentIncidentRequest.Findings[index] = finding
			if c.incidentDispatchedAt.IsZero() {
				c.currentIncidentRequest.PreparedAgentTask = ""
			}
			sortFindings(c.currentIncidentRequest.Findings)
			return true
		}
	}
	return false
}

// attachBeforeLaunch adds newly activated findings to an open incident whose
// responder has never been launched. The request is still only controller
// state, so the responder receives one incident with all corroborating
// evidence instead of the later finding waiting in the batcher until closure.
func (c *Controller) attachBeforeLaunch(findings []sdk.Finding) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.canAttachBeforeLaunchLocked() {
		return false
	}
	request := c.currentIncidentRequest
	existing := make(map[string]struct{}, len(request.Findings))
	for _, finding := range request.Findings {
		existing[FindingStateKey(finding.DetectorID, FindingFingerprint(finding))] = struct{}{}
	}
	for _, finding := range findings {
		finding.Fingerprint = FindingFingerprint(finding)
		if _, ok := existing[FindingStateKey(finding.DetectorID, finding.Fingerprint)]; ok {
			continue
		}
		request.Findings = append(request.Findings, finding)
	}
	sortFindings(request.Findings)
	request.SurfacedPlaybooks = surfacedPlaybooks(request.Findings)
	request.RelevantOutcomes = relevantOutcomeEvidence(
		c.config.RepositoryWorktree, request.Findings, c.config.SourceCommit, c.detectorOrigins(),
	)
	request.DetectorHistory = compactDetectorHistory(c.history)
	request.PreparedAgentTask = ""
	c.incidentFindingKeys = findingKeys(request.Findings)
	c.noteBatchedLocked(request.IncidentID, findings)
	return true
}

// canAttachBeforeLaunchLocked requires c.mu. A responder that was ever
// launched keeps its original request, including across dispatch retries,
// because an idempotent dispatcher may rejoin the same Job.
func (c *Controller) canAttachBeforeLaunchLocked() bool {
	if !c.incidentOpen || c.currentIncidentRequest == nil || !c.incidentDispatchedAt.IsZero() {
		return false
	}
	switch c.dispatchState {
	case contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING, contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING, contracts.DispatchState_DISPATCH_STATE_PENDING:
		return true
	default:
		return false
	}
}

func (c *Controller) dispatchReady(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) error {
	c.mu.Lock()
	incidentOpen := c.incidentOpen
	closurePending := c.closureBlocksNextLocked() && c.broker != nil
	c.mu.Unlock()
	if incidentOpen {
		c.mu.Lock()
		attachable := c.canAttachBeforeLaunchLocked()
		c.mu.Unlock()
		if attachable && c.batcher.Ready(now) {
			c.attachBeforeLaunch(c.batcher.DrainReady(now))
		}
		return nil
	}
	if closurePending {
		return nil
	}
	if !c.batcher.Ready(now) {
		return nil
	}
	if snapshot == nil {
		var err error
		snapshot, err = c.provider.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("bind incident observation scope: %w", err)
		}
	}
	batch := c.batcher.DrainReady(now)
	if len(batch) == 0 {
		return nil
	}
	// Drift is already confirmed when queued; its window only coalesces
	// drift-only incidents. A health incident must not leave it behind.
	if hasNonDriftFinding(batch) {
		batch = append(batch, c.batcher.DrainDetectorPrefix("drift.")...)
		sortFindings(batch)
	}
	request := c.incidentRequest(now, batch)
	request.ObservationGeneration = observationGeneration(snapshot)
	if c.config.CorpusSessionRoot != "" {
		if err := c.bindIncidentHealthyBaseline(request.IncidentID); err != nil {
			c.reportHealthyCaptureError(err)
		}
		c.captureOpenDetect(ctx, snapshot, request)
	}
	if c.Baseline != nil {
		request.StateChanges = cloneStateChanges(c.Baseline.Changes(now))
	}
	c.mu.Lock()
	c.incidentOpen = true
	c.responderDone = false
	c.currentIncidentRequest = cloneIncidentRequest(&request)
	c.currentIncidentResult = nil
	c.dispatchError = ""
	c.incidentDetectedAt = now.UTC()
	c.incidentDispatchedAt = time.Time{}
	c.responderCompletedAt = time.Time{}
	c.resetDispatchRetryLocked()
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	if c.broker != nil {
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
	}
	c.incidentFindingKeys = findingKeys(batch)
	c.noteBatchedLocked(request.IncidentID, batch)
	c.pruneTimelineLocked(batch)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseOpened, RecordedAt: contractTimestamp(c.incidentDetectedAt), IncidentId: request.IncidentID,
		FindingKeys: append([]string(nil), c.incidentFindingKeys...), SurfacedPlaybooks: playbookIDs(request.SurfacedPlaybooks),
	})
	c.mu.Unlock()
	return nil
}

// captureOpenDetect stores the open@detect corpus snapshot for the incident
// being opened. It is evidence for later learning, never a gate: a failure is
// reported and the incident opens anyway, and the routing sweep (real TCP
// probes, up to its time budget) runs in the background so it cannot delay the
// responder.
func (c *Controller) captureOpenDetect(ctx context.Context, snapshot sdk.DetectionContext, request IncidentRequest) {
	report := func(err error) {
		if c.OnError != nil {
			c.OnError(err)
		}
	}
	if snapshot == nil {
		var err error
		if snapshot, err = c.provider.Snapshot(ctx); err != nil {
			report(fmt.Errorf("capture incident open snapshot: %w", err))
			return
		}
	}
	if err := memory.CaptureIncidentSnapshot(c.config.CorpusSessionRoot, request.IncidentID, "open@detect", snapshot, request.Namespace, findingCaptureRefs(request.Findings)...); err != nil {
		report(fmt.Errorf("capture incident open snapshot: %w", err))
		return
	}
	if c.drift == nil || c.drift.View == nil {
		return
	}
	view := c.drift.View(snapshot)
	c.routingSweeps.Add(1)
	go func() {
		defer c.routingSweeps.Done()
		if err := captureRoutingEvidence(ctx, c.config.CorpusSessionRoot, request.IncidentID, "open@detect", request.Findings, view); err != nil {
			report(fmt.Errorf("capture incident routing evidence: %w", err))
		}
	}()
}

func hasNonDriftFinding(findings []sdk.Finding) bool {
	for _, finding := range findings {
		if !strings.HasPrefix(finding.DetectorID, "drift.") {
			return true
		}
	}
	return false
}

// playbookIDs flattens surfaced playbooks to logical IDs for telemetry.
func playbookIDs(playbooks []SurfacedPlaybook) []string {
	if len(playbooks) == 0 {
		return nil
	}
	ids := make([]string, 0, len(playbooks))
	for _, playbook := range playbooks {
		ids = append(ids, playbook.PlaybookID)
	}
	return ids
}

func severityAtLeast(actual sdk.FindingSeverity, threshold sdk.FindingSeverity) bool {
	rank := map[sdk.FindingSeverity]int{
		sdk.SeverityInfo: 1, sdk.SeverityWarning: 2, sdk.SeverityCritical: 3, sdk.SeverityLatent: 3,
	}
	return rank[actual] >= rank[threshold]
}

func (c *Controller) NextWake() time.Time {
	next := c.scheduler.NextRun()
	c.mu.Lock()
	// A batch deadline only matters when the batch can be dispatched or
	// attached. Otherwise an expired deadline would spin the runtime loop for
	// the whole incident.
	batchActionable := (!c.incidentOpen && (!c.closureBlocksNextLocked() || c.broker == nil)) ||
		c.canAttachBeforeLaunchLocked()
	c.mu.Unlock()
	if deadline, ok := c.batcher.Deadline(); ok && batchActionable && (next.IsZero() || deadline.Before(next)) {
		return deadline
	}
	c.mu.Lock()
	verificationDeadline := c.responderCompletedAt.Add(c.config.VerificationTimeout)
	verificationPending := c.incidentOpen && c.responderDone
	c.mu.Unlock()
	if verificationPending && (next.IsZero() || verificationDeadline.Before(next)) {
		next = verificationDeadline
	}
	if probe := c.nextGateProbe(); !probe.IsZero() && (next.IsZero() || probe.Before(next)) {
		next = probe
	}
	return c.closureRetryWake(c.dispatchRetryWake(next))
}

func (c *Controller) IncidentOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.incidentOpen
}

func (c *Controller) recordEvaluation(detectorID string, now time.Time, findings []sdk.Finding) {
	status := DetectorEvaluationClear
	fingerprints := make([]string, 0, len(findings))
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			status = DetectorEvaluationFiring
			fingerprints = append(fingerprints, finding.Fingerprint)
		}
	}
	sort.Strings(fingerprints)
	c.history = append(c.history, DetectorEvaluation{
		DetectorID: detectorID, EvaluatedAt: now.UTC(), Status: status, Fingerprints: fingerprints,
	})
	c.trackClearStreak(detectorID, now, status == DetectorEvaluationClear)
	if len(c.history) > 100 {
		c.history = append([]DetectorEvaluation(nil), c.history[len(c.history)-100:]...)
	}
}

func (c *Controller) recordDetectorError(detectorID string, now time.Time, err error) {
	c.history = append(c.history, DetectorEvaluation{
		DetectorID: detectorID, EvaluatedAt: now.UTC(), Status: DetectorEvaluationError, Error: err.Error(),
		Fingerprints: []string{},
	})
	c.trackClearStreak(detectorID, now, false)
	if c.OnError != nil {
		c.OnError(fmt.Errorf("detector %q: %w", detectorID, err))
	}
}

// trackClearStreak records when detectorID began its current streak of clear
// evaluations, and ends the streak on any other evaluation.
func (c *Controller) trackClearStreak(detectorID string, now time.Time, clear bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !clear {
		delete(c.detectorClearSince, detectorID)
		return
	}
	if c.detectorClearSince == nil {
		c.detectorClearSince = make(map[string]time.Time)
	}
	if _, ok := c.detectorClearSince[detectorID]; !ok {
		c.detectorClearSince[detectorID] = now.UTC()
	}
}

// healthClearedAtLocked bounds repair attribution by the incident's findings.
// Latent findings can dispatch while health has been clear since before the
// incident; that earlier streak cannot be the repair cutoff. Called with c.mu held.
func (c *Controller) healthClearedAtLocked(finalStates []DetectorEvaluation) *time.Time {
	states := finalStates
	if c.currentIncidentRequest != nil {
		ids := map[string]bool{}
		for _, finding := range c.currentIncidentRequest.Findings {
			ids[finding.DetectorID] = true
		}
		for id := range ids {
			if since, ok := c.detectorClearSince[id]; ok && !since.Before(c.incidentDispatchedAt) {
				states = append(states, DetectorEvaluation{DetectorID: id})
			}
		}
	}
	var cleared time.Time
	for _, state := range states {
		since, ok := c.detectorClearSince[state.DetectorID]
		if !ok {
			continue
		}
		if since.Before(c.incidentDispatchedAt) {
			continue
		}
		if since.After(cleared) {
			cleared = since
		}
	}
	if cleared.IsZero() {
		return nil
	}
	return &cleared
}

// CurrentAcceptedCommit returns the repository revision used for new tasks.
func (c *Controller) CurrentAcceptedCommit() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.SourceCommit
}

// detectorOrigins maps each learned incident detector to the incident it was
// learned from.
func (c *Controller) detectorOrigins() map[string]string {
	origins := make(map[string]string)
	for id, spec := range c.detectorSpecs {
		if spec.Class == sdk.DetectorClassIncident && spec.OriginatingIncident != "" {
			origins[id] = spec.OriginatingIncident
		}
	}
	return origins
}

func (c *Controller) incidentRequest(now time.Time, findings []sdk.Finding) IncidentRequest {
	incidentID := fmt.Sprintf("%s-%d", c.config.Application, now.UnixNano())
	driftEvents := driftEventsForFindings(c.driftEvents, findings)
	return IncidentRequest{
		SchemaVersion: ProtocolSchemaVersion, Application: c.config.Application, Namespace: c.config.Namespace,
		IncidentID: incidentID, Findings: findings,
		DriftEvents:         driftEvents,
		NamespaceOnboarding: cloneNamespaceOnboarding(c.namespaceOnboarding),
		DetectorHistory:     compactDetectorHistory(c.history), SurfacedPlaybooks: surfacedPlaybooks(findings),
		RelevantOutcomes: relevantOutcomeEvidence(
			c.config.RepositoryWorktree, findings, c.config.SourceCommit, c.detectorOrigins(),
		),
		SourceCommit: c.config.SourceCommit, DeployedCommit: c.config.DeployedCommit,
		RepositoryWorktree: c.config.RepositoryWorktree, RepositoryBaseCommit: c.config.SourceCommit,
		ResponseDeadline:  now.Add(c.config.ResponseTimeout).UTC(),
		CancellationToken: "cancel-" + incidentID,
		RepairPolicy:      c.config.RepairPolicy,
	}
}

func driftEventsForFindings(history []sdk.DriftEvent, findings []sdk.Finding) []sdk.DriftEvent {
	events := make([]sdk.DriftEvent, 0)
	seen := make(map[string]bool)
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		encoded, err := json.Marshal(finding.Metadata["drift_event"])
		if err != nil || string(encoded) == "null" {
			continue
		}
		var linked sdk.DriftEvent
		if err := json.Unmarshal(encoded, &linked); err != nil {
			continue
		}
		key := driftEventKey(linked) + "\x00" + linked.ObservedAt.Format(time.RFC3339Nano)
		if seen[key] {
			continue
		}
		seen[key] = true
		for _, event := range history {
			if event.Ref == linked.Ref && event.ObservedAt.Equal(linked.ObservedAt) && driftEventKey(event) == driftEventKey(linked) {
				linked = event
			}
		}
		events = append(events, linked)
	}
	return events
}

func compactDetectorHistory(history []DetectorEvaluation) []DetectorEvaluation {
	compacted := make([]DetectorEvaluation, 0, len(history))
	keys := make([]string, 0, len(history))
	for _, evaluation := range history {
		key := evaluation.DetectorID + "\x00" + string(evaluation.Status) + "\x00" +
			strings.Join(evaluation.Fingerprints, "\x00") + "\x00" + evaluation.Error
		if len(keys) >= 2 && key == keys[len(keys)-1] && key == keys[len(keys)-2] {
			compacted[len(compacted)-1] = evaluation
			continue
		}
		compacted = append(compacted, evaluation)
		keys = append(keys, key)
	}
	const limit = 12
	if len(compacted) > limit {
		compacted = compacted[len(compacted)-limit:]
	}
	return append([]DetectorEvaluation(nil), compacted...)
}

func surfacedPlaybooks(findings []sdk.Finding) []SurfacedPlaybook {
	byID := make(map[string]SurfacedPlaybook)
	for _, finding := range findings {
		for _, id := range finding.Playbooks {
			byID[id] = SurfacedPlaybook{PlaybookID: id, ParameterBindings: finding.ParameterBindings}
		}
	}
	result := make([]SurfacedPlaybook, 0, len(byID))
	for _, playbook := range byID {
		result = append(result, playbook)
	}
	sort.Slice(result, func(left int, right int) bool { return result[left].PlaybookID < result[right].PlaybookID })
	return result
}

func (c *Controller) processDispatchCompletions(observedAt time.Time) {
	for {
		select {
		case completion := <-c.results:
			c.handleDispatchCompletion(completion, observedAt)
		default:
			return
		}
	}
}

func (c *Controller) handleDispatchCompletion(completion dispatchCompletion, observedAt time.Time) {
	c.mu.Lock()
	if c.currentIncidentRequest == nil || completion.incidentID != c.currentIncidentRequest.IncidentID {
		currentIncidentID := ""
		if c.currentIncidentRequest != nil {
			currentIncidentID = c.currentIncidentRequest.IncidentID
		}
		c.mu.Unlock()
		c.reportBrokerError(
			"ignore stale dispatch completion",
			fmt.Errorf("completion incident %q does not match current incident %q", completion.incidentID, currentIncidentID),
		)
		return
	}
	var jobFailed *ResponderJobFailedError
	if errors.As(completion.err, &jobFailed) {
		// A failed Job is terminal and exactly-once dispatch forbids a second
		// responder, so the responder is done without a result. Health alone
		// decides closure, and the broker records the outcome as failed.
		c.responderDone = true
		c.responderCompletedAt = observedAt.UTC()
		if c.responderCompletedAt.Before(c.incidentDispatchedAt) {
			c.responderCompletedAt = c.incidentDispatchedAt
		}
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
		c.dispatchError = completion.err.Error()
		c.currentIncidentResult = nil
		if resumableSessionID.MatchString(completion.result.ResponderSessionID) {
			// Preserve only the checkpoint identity on a failed Job; there is
			// no worker result authorizing repair or learning claims.
			startedAt := c.incidentDispatchedAt
			if startedAt.IsZero() {
				startedAt = c.responderCompletedAt
			}
			c.currentIncidentResult = &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: completion.incidentID, Status: IncidentFailed, ResponderSessionID: completion.result.ResponderSessionID, Error: completion.err.Error(), Timing: TimingMetrics{StartedAt: startedAt, CompletedAt: c.responderCompletedAt}}
		}
		c.resetDispatchRetryLocked()
	} else if completion.err != nil {
		// A watcher, transport, or leadership-guard failure does not prove that
		// the durable responder Job stopped. Retry the same incident effect so
		// the idempotent dispatcher rejoins its existing Job/result instead of
		// clearing the incident lock and opening a duplicate responder. Bounded,
		// jittered backoff keeps a persistent outage (an API-server pause) from
		// spinning the dispatch effect in a hot loop.
		c.responderDone = false
		c.responderCompletedAt = time.Time{}
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
		c.dispatchError = completion.err.Error()
		c.currentIncidentResult = nil
		c.recordDispatchFailureLocked()
	} else {
		c.responderDone = true
		if len(completion.result.RepairActions) > 0 || len(completion.result.RepairChanges) > 0 {
			c.adoptionNeeded = c.drift != nil && c.drift.Source != nil && c.drift.Client != nil
		}
		c.responderCompletedAt = observedAt.UTC()
		if c.responderCompletedAt.Before(c.incidentDispatchedAt) {
			c.responderCompletedAt = c.incidentDispatchedAt
		}
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
		c.dispatchError = ""
		c.currentIncidentResult = cloneIncidentResult(&completion.result)
		c.resetDispatchRetryLocked()
	}
	if c.responderDone {
		c.emitLifecycleLocked(&contracts.LifecycleEvent{
			Event: PhaseResponderCompleted, RecordedAt: contractTimestamp(c.responderCompletedAt), IncidentId: completion.incidentID,
			DispatchError: c.dispatchError != "",
		})
	}
	c.mu.Unlock()
	if completion.err == nil && c.Helpers != nil {
		c.cleanupHelpers()
	}
	if completion.err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("dispatch incident: %w", completion.err))
	}
	if completion.err == nil && c.OnResult != nil {
		c.OnResult(completion.result)
	}
}

// cleanupHelpers deletes the responder's labelled helpers. A failure is
// reported but never blocks verification: helpers are hygiene, not health.
func (c *Controller) cleanupHelpers() {
	ctx, cancel := context.WithTimeout(context.Background(), helperCleanupTimeout)
	defer cancel()
	deleted, err := c.Helpers.CleanupHelpers(ctx)
	c.mu.Lock()
	c.cleanedHelpers = append([]string(nil), deleted...)
	c.mu.Unlock()
	if err != nil && c.OnError != nil {
		c.OnError(err)
	}
}

func (c *Controller) maybeCloseIncident(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) {
	c.mu.Lock()
	if !c.incidentOpen || !c.responderDone || c.currentIncidentRequest == nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	if snapshot == nil {
		var err error
		snapshot, err = c.provider.Snapshot(ctx)
		if err != nil {
			c.reportBrokerError("verify observation scope", err)
			return
		}
	}
	generation := observationGeneration(snapshot)
	driftClear, driftFindings := c.verifyClosureDrift(ctx, now, snapshot)
	c.mu.Lock()
	finalStates, healthVerified := c.finalVerificationStates()
	scopeCurrent := c.currentIncidentRequest.ObservationGeneration == generation
	verified := healthVerified && driftClear && scopeCurrent
	if !scopeCurrent {
		c.dispatchError = "observation scope changed; incident evidence superseded"
	}
	// A terminal dispatch failure already closes as failed once health permits
	// it. Unresolved drift must not delay it or count as verified recovery.
	terminalFailure := !scopeCurrent || (healthVerified && c.dispatchError != "")
	verification := c.continuationVerificationLocked(healthVerified, driftClear, driftFindings)
	if !verified && (len(verification.Findings) > 0 || verification.Error != "") && scopeCurrent && c.dispatchError == "" && now.Before(c.currentIncidentRequest.ResponseDeadline) {
		if continuer, ok := c.dispatcher.(interface {
			ContinueAfterVerification(context.Context, IncidentRequest, ToolResponse, time.Time) (bool, error)
		}); ok {
			resumed, err := continuer.ContinueAfterVerification(ctx, *c.currentIncidentRequest, verification, now)
			if err != nil {
				c.mu.Unlock()
				c.reportBrokerError("resume unhealthy responder", err)
				return
			}
			if resumed {
				c.responderDone = false
				c.responderCompletedAt = time.Time{}
				c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
				c.resetDispatchRetryLocked()
				c.mu.Unlock()
				return
			}
		}
	}
	if !verified {
		// One timeout bounds how long a stuck incident can block the stream. It
		// closes as a normal failed incident: with no final detector states
		// the outcome is never health-verified.
		if !terminalFailure && now.Before(c.responderCompletedAt.Add(c.config.VerificationTimeout)) {
			c.mu.Unlock()
			return
		}
		if !terminalFailure {
			logVerificationTimeout(c.currentIncidentRequest.IncidentID, c.config.VerificationTimeout, c.activeHealthDetectors())
		}
		finalStates = nil
	}
	if verified && c.config.CorpusSessionRoot != "" {
		if snapshot == nil {
			var err error
			snapshot, err = c.provider.Snapshot(ctx)
			if err != nil {
				c.mu.Unlock()
				c.reportBrokerError("capture verified snapshot", err)
				return
			}
		}
		if err := memory.CaptureIncidentSnapshot(c.config.CorpusSessionRoot, c.currentIncidentRequest.IncidentID, "fixed", snapshot, c.config.Namespace, findingCaptureRefs(c.currentIncidentRequest.Findings)...); err != nil {
			c.mu.Unlock()
			c.reportBrokerError("capture verified snapshot", err)
			return
		}
		if c.drift != nil && c.drift.View != nil {
			incidentID := c.currentIncidentRequest.IncidentID
			findings := append([]sdk.Finding(nil), c.currentIncidentRequest.Findings...)
			c.mu.Unlock()
			err := memory.CaptureRepairedProgramReads(c.config.CorpusSessionRoot, incidentID, c.drift.View)
			if err == nil {
				err = captureRoutingEvidence(ctx, c.config.CorpusSessionRoot, incidentID, "fixed", findings, c.drift.View(snapshot))
			}
			c.mu.Lock()
			if err != nil {
				c.mu.Unlock()
				c.reportBrokerError("capture verified program reads", err)
				return
			}
			if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
				c.mu.Unlock()
				return
			}
		}
	}
	verifiedAt := now.UTC()
	if verifiedAt.Before(c.responderCompletedAt) {
		verifiedAt = c.responderCompletedAt
	}
	finalChanges := c.finalStateChangesLocked(now)
	var healthClearedAt *time.Time
	if verified {
		healthClearedAt = c.healthClearedAtLocked(finalStates)
		logIncidentVerified(c.currentIncidentRequest.IncidentID, healthClearedAt, c.responderCompletedAt, verifiedAt)
	}
	closure := IncidentClosure{
		ObservationGeneration: generation,
		Request:               *cloneIncidentRequest(c.currentIncidentRequest), Result: cloneIncidentResult(c.currentIncidentResult),
		DispatchError: c.dispatchError, FinalDetectorStates: finalStates,
		IncidentDetectorStates: c.incidentDetectorStates(),
		FinalStateChanges:      finalChanges,
		HealthClearedAt:        healthClearedAt,
		DetectedAt:             c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: c.responderCompletedAt, VerifiedAt: verifiedAt,
		CleanedHelpers: append([]string(nil), c.cleanedHelpers...),
	}
	if verified {
		c.retainSourceInventoryAcceptancesLocked(c.currentIncidentResult)
	}
	closure.DetectorTimeline = cloneTimeline(c.timeline)
	sortTimeline(closure.DetectorTimeline)
	closure.IncidentDetectorFiredBeforeDispatch, closure.IncidentDetectorFiredAfterDispatch,
		closure.NoIncidentDetectorFired = timelineSummary(closure.DetectorTimeline)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseClosed, RecordedAt: contractTimestamp(verifiedAt), IncidentId: closure.Request.IncidentID,
		HealthClearedAt: optionalContractTimestamp(closure.HealthClearedAt),
		DispatchError:   closure.DispatchError != "",
	})
	c.timeline = nil
	projected, err := incidentClosureContract(closure)
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: now, Closure: projected})
	}
	if err != nil {
		c.mu.Unlock()
		c.reportBrokerError("freeze closure", err)
		return
	}
	c.clearIncidentLocked()
	c.mu.Unlock()
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
}

func (c *Controller) activeHealthDetectors() []string {
	active := []string{}
	for _, detectorID := range c.healthDetectorIDs {
		if c.tracker.HasActiveDetector(detectorID) {
			active = append(active, detectorID)
		}
	}
	return active
}

// logVerificationTimeout records that the verification window ended with
// health or admitted source drift unresolved, so the incident closes as failed.
func logVerificationTimeout(incidentID string, timeout time.Duration, active []string) {
	slog.Warn("incident_verification_timeout", "incident_id", incidentID, "verification_timeout", timeout.String(), "active_health_detectors", active)
}

// logIncidentVerified records the verification wait: how long after the
// responder completed the health detectors cleared and the incident verified.
func logIncidentVerified(incidentID string, healthClearedAt *time.Time, responderCompletedAt, verifiedAt time.Time) {
	attrs := []any{"incident_id", incidentID, "responder_completed_at", responderCompletedAt, "verified_at", verifiedAt,
		"responder_completed_to_verified_ms", verifiedAt.Sub(responderCompletedAt).Milliseconds()}
	if healthClearedAt != nil {
		attrs = append(attrs, "health_cleared_at", *healthClearedAt, "responder_completed_to_health_cleared_ms", healthClearedAt.Sub(responderCompletedAt).Milliseconds())
	}
	slog.Info("incident_verified", attrs...)
}

// finalVerificationStates reports whether the incident is verified: the health
// detectors (or, without any, the incident's own findings) have cleared and
// evaluated clear since the responder completed. maybeCloseIncident additionally
// requires a fresh comparison of admitted source drift through verifyClosureDrift.
func (c *Controller) finalVerificationStates() ([]DetectorEvaluation, bool) {
	if len(c.healthDetectorIDs) == 0 {
		if c.tracker.HasActiveKeys(c.incidentFindingKeys) {
			return nil, false
		}
		ids := make([]string, 0)
		seen := make(map[string]struct{})
		for _, finding := range c.currentIncidentRequest.Findings {
			if _, ok := seen[finding.DetectorID]; !ok {
				seen[finding.DetectorID] = struct{}{}
				ids = append(ids, finding.DetectorID)
			}
		}
		sort.Strings(ids)
		return c.latestEvaluations(ids, time.Time{}, false)
	}
	for _, detectorID := range c.healthDetectorIDs {
		if c.tracker.HasActiveDetector(detectorID) {
			return nil, false
		}
	}
	return c.latestEvaluations(c.healthDetectorIDs, c.responderCompletedAt, true)
}

// incidentDetectorStates reports the latest evaluation since responder
// completion of each non-health detector that raised a finding in the current
// incident. Detectors that have not evaluated since then are omitted.
func (c *Controller) incidentDetectorStates() []DetectorEvaluation {
	health := make(map[string]struct{}, len(c.healthDetectorIDs))
	for _, detectorID := range c.healthDetectorIDs {
		health[detectorID] = struct{}{}
	}
	ids := make([]string, 0)
	seen := make(map[string]struct{})
	for _, finding := range c.currentIncidentRequest.Findings {
		if _, ok := health[finding.DetectorID]; ok {
			continue
		}
		if _, ok := seen[finding.DetectorID]; !ok {
			seen[finding.DetectorID] = struct{}{}
			ids = append(ids, finding.DetectorID)
		}
	}
	sort.Strings(ids)
	states, _ := c.latestEvaluations(ids, c.responderCompletedAt, false)
	return states
}

// finalStateChangesLocked is the configuration diff against the healthy
// baseline at verification time: a composite's later fault can land a
// few seconds after dispatch, so the dispatch-time request diff can miss it
// while it is already visible here. Called with c.mu held; StateBaseline
// implementations keep their own lock, so there is no ordering hazard.
func (c *Controller) finalStateChangesLocked(now time.Time) *StateChanges {
	if c.Baseline == nil {
		return nil
	}
	return cloneStateChanges(c.Baseline.Changes(now))
}

func (c *Controller) latestEvaluations(
	detectorIDs []string,
	since time.Time,
	requireClear bool,
) ([]DetectorEvaluation, bool) {
	result := make([]DetectorEvaluation, 0, len(detectorIDs))
	for _, detectorID := range detectorIDs {
		found := false
		for index := len(c.history) - 1; index >= 0; index-- {
			evaluation := c.history[index]
			if evaluation.DetectorID != detectorID || evaluation.EvaluatedAt.Before(since) {
				continue
			}
			if requireClear && evaluation.Status != DetectorEvaluationClear {
				return nil, false
			}
			result = append(result, evaluation)
			found = true
			break
		}
		if !found && requireClear {
			return nil, false
		}
	}
	return result, true
}

func (c *Controller) ExportState() (*contracts.ControllerState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	history := append([]DetectorEvaluation(nil), c.history...)
	if len(history) > 100 {
		history = history[len(history)-100:]
	}
	dispatchState := c.dispatchState
	if dispatchState == contracts.DispatchState_DISPATCH_STATE_RUNNING {
		dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	} else if dispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING {
		dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
	}
	findingStates, err := findingStatesContract(c.tracker.Snapshot())
	if err != nil {
		return nil, err
	}
	pendingBatch := c.batcher.Snapshot()
	batch, err := findingsContract(pendingBatch.Findings)
	if err != nil {
		return nil, fmt.Errorf("pending batch: %w", err)
	}
	timeline := detectorTimelineContract(c.timeline)
	view, err := incidentViewContract(c.incidentView)
	if err != nil {
		return nil, err
	}
	message := &contracts.ControllerState{
		Version: RuntimeStateVersion, SchedulerDeadlines: timestampsContract(c.scheduler.Deadlines()), FindingStates: findingStates,
		PendingBatch: &contracts.BatcherState{
			Findings: batch, Deadlines: timestampsContract(pendingBatch.Deadlines), Deadline: optionalTimestamp(pendingBatch.Deadline),
		},
		History: evaluationsContract(history), IncidentOpen: c.incidentOpen, ResponderDone: c.responderDone,
		DispatchError: c.dispatchError, IncidentDetectedAt: optionalTimestamp(c.incidentDetectedAt),
		IncidentDispatchedAt: optionalTimestamp(c.incidentDispatchedAt), ResponderCompletedAt: optionalTimestamp(c.responderCompletedAt),
		DetectorClearSince:  timestampsContract(c.detectorClearSince),
		DispatchState:       dispatchState,
		IncidentFindingKeys: append([]string(nil), c.incidentFindingKeys...), ClosureState: c.closureState,
		ClosureReceipt: closureReceiptContract(c.closureReceipt), ClosureFailure: closureFailureContract(c.closureFailure), FailedClosures: cloneFailedClosures(c.failedClosures),
		LastAcknowledgedIncidentId: c.lastAcknowledgedIncidentID,
		EvaluationIteration:        int32(c.evaluationIteration), DetectorTimeline: timeline, IncidentView: view,
		NamespaceOnboarding: cloneNamespaceOnboarding(c.namespaceOnboarding),
		AcceptedSourceDrift: driftEventsContract(c.acceptedSourceDrift), AcceptedSourceDriftRevision: c.acceptedSourceDriftRevision,
		DriftEvents: driftEventsContract(c.driftEvents), OwnershipAdoption: adoptionContract(c.ownershipAdoption),
	}
	if c.currentIncidentRequest != nil {
		message.IncidentRequest, err = incidentRequestContract(*c.currentIncidentRequest)
		if err != nil {
			return nil, fmt.Errorf("incident request: %w", err)
		}
	}
	if c.currentIncidentResult != nil {
		message.IncidentResult, err = incidentResultContract(*c.currentIncidentResult)
		if err != nil {
			return nil, fmt.Errorf("incident result: %w", err)
		}
	}
	if c.pendingClosure != nil {
		message.PendingClosure, err = incidentClosureContract(*c.pendingClosure)
		if err != nil {
			return nil, fmt.Errorf("pending closure: %w", err)
		}
	}
	recovered, _ := lifecyclecore.Step(message, lifecyclecore.Event{Kind: lifecyclecore.Restart})
	return recovered, nil
}

func (c *Controller) RestoreState(state *contracts.ControllerState) error {
	state, err := c.resolvePreparedTask(state)
	if err != nil {
		return err
	}
	if state.GetNamespaceOnboarding() != nil {
		if err := onboarding.Validate(state.GetNamespaceOnboarding(), c.config.Namespace); err != nil {
			return err
		}
	}
	if err := validateRuntimeState(state); err != nil {
		return err
	}
	if err := c.scheduler.RestoreDeadlines(timestampsState(state.GetSchedulerDeadlines())); err != nil {
		return err
	}
	// Retired proactive playbooks are not scheduled detectors. Keep genuine
	// registered detector IDs and accepted-source drift, but retire their old
	// synthetic tracker and batch entries instead of re-running Diagnose.
	retired := func(id string) bool {
		return strings.HasPrefix(id, "drift.") && id != "drift.unexplained" && c.detectors[id] == nil
	}
	findings := findingStatesState(state.GetFindingStates())
	for key, finding := range findings {
		if retired(finding.DetectorID) {
			delete(findings, key)
		}
	}
	c.tracker.Restore(findings)
	pending := []sdk.Finding{}
	for _, finding := range findingsState(state.GetPendingBatch().GetFindings()) {
		if !retired(finding.DetectorID) {
			pending = append(pending, finding)
		}
	}
	c.batcher.Restore(BatcherState{Findings: pending, Deadlines: timestampsState(state.GetPendingBatch().GetDeadlines()), Deadline: controllerTime(state.GetPendingBatch().GetDeadline())})
	c.history = append([]DetectorEvaluation(nil), evaluationsState(state.GetHistory())...)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incidentOpen = state.IncidentOpen
	// A subprocess cannot survive a controller restart. Preserve the incident
	// lock, but treat the old process as finished so detector clears can close it.
	c.responderDone = state.ResponderDone
	c.currentIncidentRequest = cloneIncidentRequest(incidentRequestState(state.GetIncidentRequest()))
	c.currentIncidentResult = nil
	if state.IncidentResult != nil {
		result := incidentResultState(state.IncidentResult)
		c.currentIncidentResult = &result
	}
	c.driftEvents = append([]sdk.DriftEvent(nil), driftEventsState(state.GetDriftEvents())...)
	c.acceptedSourceDrift = driftEventsState(state.GetAcceptedSourceDrift())
	c.acceptedSourceDriftRevision = state.GetAcceptedSourceDriftRevision()
	c.ownershipAdoption = adoptionState(state.GetOwnershipAdoption())
	c.namespaceOnboarding = cloneNamespaceOnboarding(state.GetNamespaceOnboarding())
	c.sourceInventory = nil
	c.dispatchError = state.DispatchError
	c.incidentDetectedAt = controllerTime(state.GetIncidentDetectedAt())
	c.incidentDispatchedAt = controllerTime(state.GetIncidentDispatchedAt())
	c.responderCompletedAt = controllerTime(state.GetResponderCompletedAt())
	c.detectorClearSince = cloneClearSince(timestampsState(state.GetDetectorClearSince()))
	c.dispatchState = state.DispatchState
	c.incidentFindingKeys = append([]string(nil), state.IncidentFindingKeys...)
	c.pendingClosure = nil
	if state.PendingClosure != nil {
		closure := incidentClosureState(state.PendingClosure)
		c.pendingClosure = &closure
	}
	c.closureState = state.ClosureState
	c.closureReceipt = cloneClosureReceipt(closureReceiptState(state.GetClosureReceipt()))
	c.closureFailure = cloneClosureFailure(closureFailureState(state.GetClosureFailure()))
	c.failedClosures = cloneFailedClosures(state.GetFailedClosures())
	if _, err := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Restart}); err != nil {
		return err
	}
	c.lastAcknowledgedIncidentID = state.GetLastAcknowledgedIncidentId()
	c.evaluationIteration = int(state.GetEvaluationIteration())
	c.timeline = cloneTimeline(detectorTimelineState(state.GetDetectorTimeline()))
	return nil
}

func (c *Controller) LastAcknowledgedIncidentID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAcknowledgedIncidentID
}

func (c *Controller) PendingIncidentClosure() (IncidentClosure, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingClosure == nil {
		return IncidentClosure{}, false
	}
	return *cloneIncidentClosure(c.pendingClosure), true
}

func (c *Controller) SetIncidentBroker(broker IncidentBroker) error {
	if broker == nil {
		return fmt.Errorf("incident broker is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.broker = broker
	return nil
}

func findingKeys(findings []sdk.Finding) []string {
	keys := make([]string, 0, len(findings))
	for _, finding := range findings {
		keys = append(keys, FindingStateKey(finding.DetectorID, FindingFingerprint(finding)))
	}
	sort.Strings(keys)
	return keys
}

func (c *Controller) AttachStateStore(ctx context.Context, store StateStore) error {
	if store == nil {
		return fmt.Errorf("state store is required")
	}
	state, revision, err := store.Load(ctx)
	if err != nil {
		return err
	}
	if revision != "" {
		if state, err = c.resolvePreparedTask(state); err != nil {
			return err
		}
		if err := c.RestoreState(state); err != nil {
			return err
		}
	}
	c.stateStore = store
	c.stateRevision = revision
	c.mu.Lock()
	c.stateRestored = revision != ""
	c.durable = nil
	if revision != "" {
		c.durable = proto.Clone(state).(*contracts.ControllerState)
	}
	c.mu.Unlock()
	return nil
}

func (c *Controller) PersistState(ctx context.Context) error {
	if c.stateStore == nil {
		return nil
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	state, err := c.ExportState()
	if err != nil {
		return fmt.Errorf("snapshot controller state: %w", err)
	}
	if preparer, ok := c.dispatcher.(interface {
		PrepareDispatch(context.Context, IncidentRequest) (IncidentRequest, error)
	}); ok && state.DispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING && state.IncidentRequest != nil && state.IncidentRequest.PreparedAgentTask == "" {
		prepared, err := preparer.PrepareDispatch(actionCtx, *incidentRequestState(state.IncidentRequest))
		if err != nil {
			return fmt.Errorf("prepare responder dispatch: %w", err)
		}
		c.mu.Lock()
		if c.currentIncidentRequest == nil || !sameIncidentRequest(*c.currentIncidentRequest, *incidentRequestState(state.IncidentRequest)) {
			c.mu.Unlock()
			return ErrStateConflict
		}
		c.currentIncidentRequest = cloneIncidentRequest(&prepared)
		c.mu.Unlock()
		state.IncidentRequest, err = incidentRequestContract(prepared)
		if err != nil {
			return fmt.Errorf("prepared incident request: %w", err)
		}
	}
	saved, stagedIncident, err := c.externalizePreparedTask(state)
	if err != nil {
		return err
	}
	revision, err := c.stateStore.Save(actionCtx, saved, c.stateRevision)
	if err != nil {
		// The write may or may not have landed; no effect recorded since the
		// last confirmed save may run until a later save confirms it.
		c.mu.Lock()
		c.durable = nil
		c.mu.Unlock()
		return err
	}
	c.stateRevision = revision
	c.releaseStaleStagedTask(stagedIncident)
	c.mu.Lock()
	c.durable = proto.Clone(state).(*contracts.ControllerState)
	archivedIDs := make([]string, 0, len(state.GetFailedClosures()))
	for _, record := range state.GetFailedClosures() {
		archivedIDs = append(archivedIDs, record.GetFailure().GetIncidentId())
	}
	_, transitionErr := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.ArchivesPersisted, ArchivedIncidentIDs: archivedIDs})
	c.mu.Unlock()
	return transitionErr
}

func (c *Controller) actionContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if c.CheckHealth != nil {
		if err := c.CheckHealth(ctx); err != nil {
			return nil, nil, err
		}
	}
	if c.GuardAction != nil {
		return c.GuardAction(ctx)
	}
	if c.CanAct != nil && !c.CanAct() {
		return nil, nil, ErrLeadershipInactive
	}
	actionCtx, cancel := context.WithCancel(ctx)
	return actionCtx, cancel, nil
}

func (c *Controller) hasRetainedDrift() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentIncidentRequest != nil {
		for _, f := range c.currentIncidentRequest.Findings {
			if strings.HasPrefix(f.DetectorID, "drift.") {
				return true
			}
		}
	}
	for _, f := range c.batcher.Snapshot().Findings {
		if strings.HasPrefix(f.DetectorID, "drift.") {
			return true
		}
	}
	return false
}

// evaluationDetectorIDs labels the partial per-iteration sample in logs. An
// absent detector was not evaluated; it did not report clearance.
func (c *Controller) evaluationDetectorIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	for _, evaluation := range c.history {
		if evaluation.EvaluatedAt.Equal(c.evaluationAt) {
			seen[evaluation.DetectorID] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
