package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"maps"
	"math/rand/v2"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sort"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/core"
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
	// MaxFollowUps enables bounded follow-up responders: when health findings
	// stay active after a responder completes, up to this many further
	// responders are dispatched for the residual findings before the incident
	// is handed to detector review. Zero (the default) disables follow-ups.
	MaxFollowUps int
	// FollowUpCooldown is the wait after a responder completes before its
	// residual findings are treated as unresolved. It is capped by
	// VerificationTimeout.
	FollowUpCooldown time.Duration
	// Admission configures load-aware incident admission. The zero value
	// (disabled) admits every ready batch immediately, as before.
	Admission AdmissionConfig
}

type dispatchCompletion struct {
	incidentID string
	result     IncidentResult
	err        error
}

type Controller struct {
	config              ControllerConfig
	startedAt           time.Time
	scheduler           *Scheduler
	detectors           map[string]sdk.Detector
	tracker             *FindingStateTracker
	batcher             *Batcher
	provider            SnapshotProvider
	dispatcher          Dispatcher
	history             []DetectorEvaluation
	drift               *DriftControllerConfig
	driftEvents         []sdk.DriftEvent
	driftAdmission      *contracts.DriftAdmissionState
	programSweepCursors map[string]string
	ownershipAdoption   OwnershipAdoptionStatus
	adoptionNeeded      bool
	nextDriftSweep      time.Time
	results             chan dispatchCompletion
	// gateClears and gateProbes hold the submit gate's confirmation of active
	// findings (GateConfirmationPolicy); they are never persisted, so a
	// restarted controller confirms again from fresh evaluations.
	gateClears    map[string]*gateClear
	gateProbes    map[string]time.Time
	stateStore    StateStore
	stateRevision string

	mu sync.Mutex
	// durable is the state stateStore last confirmed durable. With a store
	// attached, an effect runs only while this state records it, so a
	// restarted controller can recover every effect that ran.
	durable                  *contracts.ControllerState
	incidentOpen             bool
	responderDone            bool
	currentIncidentRequest   *IncidentRequest
	dispatchState            contracts.DispatchState
	incidentFindingKeys      []string
	cleanedHelpers           []string
	healthDetectorIDs        []string
	detectorSpecs            map[string]sdk.DetectorSpec
	currentIncidentResult    *IncidentResult
	dispatchError            string
	incidentDetectedAt       time.Time
	incidentDispatchedAt     time.Time
	responderCompletedAt     time.Time
	detectorReviewRequired   bool
	detectorReviewRequiredAt time.Time
	detectorReviewReason     string
	// detectorClearSince maps each detector to the start of its current
	// streak of clear evaluations; a firing or erroring evaluation ends
	// the streak. Guarded by mu.
	detectorClearSince map[string]time.Time
	// incidentObservedChanges maps Kind/name to the first time the open
	// incident's diff showed that object changed. Guarded by mu.
	incidentObservedChanges    map[string]ObservedStateChange
	pendingClosure             *IncidentClosure
	closureState               contracts.ClosureState
	closureReceipt             *ClosureReceipt
	closureFailure             *ClosureFailure
	dispatchFailureAttempts    int
	dispatchNextRetryAt        time.Time
	lastAcknowledgedIncidentID string
	incidentView               *IncidentView
	broker                     IncidentBroker
	workspaceResults           chan workspaceCompletion
	closureResults             chan closureCompletion
	acknowledgmentResults      chan acknowledgmentCompletion
	releaseResults             chan releaseCompletion
	// pendingReleases holds incident IDs whose prepared worktree was stranded
	// when a follow-up superseded them in place; it is durable so a restart
	// still reaps them. releaseInFlight is the one currently being released and
	// is transient: a restart re-emits it from pendingReleases (release is
	// idempotent).
	pendingReleases []string
	releaseInFlight string

	// loadGauge, when set, reports host resource pressure to the admission gate;
	// nil leaves the host-load term at zero. admissionRecheckAt is the next time a
	// deferred incident re-evaluates admission; it is transient (a restart
	// re-samples immediately).
	loadGauge          LoadGauge
	admissionRecheckAt time.Time

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

	OnError          func(error)
	OnResult         func(IncidentResult)
	OnEvaluation     func([]sdk.Finding)
	OnIncidentClosed func(IncidentClosure)
	OnClosureFailed  func(ClosureFailure)
	CanAct           func() bool
	GuardAction      func(context.Context) (context.Context, context.CancelFunc, error)
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
	if config.MaxFollowUps < 0 {
		return nil, fmt.Errorf("max follow-ups must not be negative")
	}
	if config.FollowUpCooldown < 0 {
		return nil, fmt.Errorf("follow-up cooldown must not be negative")
	}
	if err := config.Admission.validate(); err != nil {
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
		config: config, startedAt: start, scheduler: NewScheduler(detectors, start), detectors: byID,
		tracker:    tracker,
		gateClears: make(map[string]*gateClear), gateProbes: make(map[string]time.Time),
		batcher: NewDebouncedBatcher(config.BatchDebounce), provider: provider, dispatcher: dispatcher,
		results: make(chan dispatchCompletion, 1), healthDetectorIDs: healthDetectorIDs,
		detectorSpecs:    detectorSpecs,
		workspaceResults: make(chan workspaceCompletion, 1), closureResults: make(chan closureCompletion, 1),
		acknowledgmentResults: make(chan acknowledgmentCompletion, 1),
		releaseResults:        make(chan releaseCompletion, 1),
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
	c.processDispatchCompletions(now)
	c.processBrokerCompletions()
	detectors := c.scheduler.SelectEvents(now, events)
	probes := c.dueGateProbes(now, detectors)
	var objectChanges []ObjectChange
	sweepDrift := false
	confirmDrift := false
	if c.drift != nil {
		at := c.nextProgramConfirmation()
		confirmDrift = !at.IsZero() && !now.Before(at)
		if c.drift.Events != nil {
			objectChanges = c.drift.Events.TakeObjectChanges()
		}
		sweepDrift = !now.Before(c.nextDriftSweep)
		if sweepDrift {
			c.nextDriftSweep = now.Add(c.drift.SweepInterval)
		}
	}
	if len(detectors) == 0 && len(probes) == 0 && len(objectChanges) == 0 && !sweepDrift && !confirmDrift && !c.adoptionNeeded && !c.hasRetainedDrift() {
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
	c.processDrift(ctx, now, snapshot, objectChanges, sweepDrift)

	sampleFindings := make([]sdk.Finding, 0)
	errored := false
	c.mu.Lock()
	c.evaluationIteration++
	c.evaluationAt = now.UTC()
	c.mu.Unlock()
	for _, detector := range detectors {
		spec := detector.Spec()
		findings, detectErr := detector.Detect(ctx, snapshot)
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
	c.probeGate(ctx, now, snapshot, probes)
	c.forgetGate()
	if len(detectors) > 0 {
		sort.Slice(sampleFindings, func(left int, right int) bool {
			return sampleFindings[left].Fingerprint < sampleFindings[right].Fingerprint
		})
		if c.OnEvaluation != nil {
			c.OnEvaluation(sampleFindings)
		}
		if c.Baseline != nil {
			c.Baseline.Observe(now, !errored && c.quiet())
		}
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

	c.refreshRetainedDrift(ctx, now, snapshot)
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
	busy := c.incidentOpen || c.pendingClosure != nil
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
	case contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING, contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING, contracts.DispatchState_DISPATCH_STATE_PENDING, admissionDeferredState:
		return true
	default:
		return false
	}
}

// SetLoadGauge attaches the admission gate's host-load source. A nil gauge (the
// default) leaves the host-load term at zero, so only the release-backlog term
// can defer admission.
func (c *Controller) SetLoadGauge(gauge LoadGauge) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadGauge = gauge
}

// sampleLoad reads the host-load gauge without holding c.mu. A nil gauge reports
// zero pressure; a gauge error is reported and treated as zero, so a transient
// read failure fails open (admit) rather than wedging incident handling.
func (c *Controller) sampleLoad() LoadSample {
	c.mu.Lock()
	gauge := c.loadGauge
	c.mu.Unlock()
	if gauge == nil {
		return LoadSample{}
	}
	sample, err := gauge.Sample()
	if err != nil {
		if c.OnError != nil {
			c.OnError(fmt.Errorf("sample host load for admission: %w", err))
		}
		return LoadSample{}
	}
	return sample
}

// reassessDeferredAdmission re-evaluates a held incident against a fresh load
// sample. It resumes (to the normal pending/workspace_pending dispatch state and
// a one-time admission_resumed event) only when pressure has cleared both low
// watermarks; otherwise it schedules the next recheck.
func (c *Controller) reassessDeferredAdmission(now time.Time) {
	sample := c.sampleLoad()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dispatchState != admissionDeferredState || c.currentIncidentRequest == nil {
		return
	}
	assessment := c.config.Admission.assess(true, sample, len(c.pendingReleases))
	if assessment.Defer {
		c.admissionRecheckAt = now.Add(c.config.Admission.RecheckInterval)
		return
	}
	next := contracts.DispatchState_DISPATCH_STATE_PENDING
	if c.broker != nil {
		next = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
	}
	c.dispatchState = next
	c.admissionRecheckAt = time.Time{}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseAdmissionResumed, RecordedAt: contractTimestamp(now.UTC()), IncidentId: c.currentIncidentRequest.IncidentID,
		LoadPressure: assessment.Pressure, LoadThreshold: assessment.Threshold,
		ReleaseBacklog: int32(assessment.ReleaseBacklog), Reason: assessment.Reason,
	})
}

// emitAdmissionDeferredLocked records that a ready batch was held under load.
// Callers hold c.mu. Each (incident, admission_deferred) pair is emitted at most
// once, so the event id is stable and a crash-replay drops the duplicate.
func (c *Controller) emitAdmissionDeferredLocked(now time.Time, incidentID string, assessment admissionAssessment) {
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseAdmissionDeferred, RecordedAt: contractTimestamp(now.UTC()), IncidentId: incidentID,
		LoadPressure: assessment.Pressure, LoadThreshold: assessment.Threshold,
		ReleaseBacklog: int32(assessment.ReleaseBacklog), Reason: assessment.Reason,
	})
}

func (c *Controller) dispatchReady(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) error {
	c.mu.Lock()
	incidentOpen := c.incidentOpen
	deferred := c.dispatchState == admissionDeferredState
	closurePending := c.pendingClosure != nil && c.broker != nil
	c.mu.Unlock()
	if incidentOpen {
		if deferred {
			// A held incident re-evaluates admission against a fresh sample; it
			// resumes only once pressure clears both low watermarks.
			c.reassessDeferredAdmission(now)
		}
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
	priorAdmission := cloneDriftAdmission(c.driftAdmission)
	batch = c.admitDriftBatch(now, batch)
	if len(batch) == 0 {
		return nil
	}
	request := c.incidentRequest(now, batch)
	requeue := func(err error) error {
		c.driftAdmission = priorAdmission
		for _, finding := range batch {
			c.batcher.AddAt(finding, now)
		}
		return err
	}
	if c.config.CorpusSessionRoot != "" {
		if snapshot == nil {
			var err error
			snapshot, err = c.provider.Snapshot(ctx)
			if err != nil {
				return requeue(fmt.Errorf("capture incident open snapshot: %w", err))
			}
		}
		if err := memory.CaptureIncidentSnapshot(c.config.CorpusSessionRoot, request.IncidentID, "open@detect", snapshot, request.Namespace); err != nil {
			return requeue(fmt.Errorf("capture incident open snapshot: %w", err))
		}
		if c.drift != nil && c.drift.View != nil {
			if err := captureRoutingEvidence(ctx, c.config.CorpusSessionRoot, request.IncidentID, "open@detect", request.Findings, c.drift.View(snapshot)); err != nil {
				return requeue(fmt.Errorf("capture incident routing evidence: %w", err))
			}
		}

	}
	if c.Baseline != nil {
		request.StateChanges = cloneStateChanges(c.Baseline.Changes(now))
	}
	// Sample host load outside the lock so a gauge read never holds c.mu.
	sample := c.sampleLoad()
	c.mu.Lock()
	c.incidentOpen = true
	c.responderDone = false
	c.currentIncidentRequest = cloneIncidentRequest(&request)
	c.currentIncidentResult = nil
	c.dispatchError = ""
	c.incidentDetectedAt = now.UTC()
	c.incidentDispatchedAt = time.Time{}
	c.responderCompletedAt = time.Time{}
	c.detectorReviewRequired = false
	c.detectorReviewRequiredAt = time.Time{}
	c.detectorReviewReason = ""
	c.resetDispatchRetryLocked()
	c.incidentObservedChanges = make(map[string]ObservedStateChange)
	c.observeStateChangesLocked(request.StateChanges, now)
	readyState := contracts.DispatchState_DISPATCH_STATE_PENDING
	if c.broker != nil {
		readyState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
	}
	// Load-aware admission: when the controller is over a pressure threshold the
	// incident is held (admission_deferred) before any worktree is prepared or
	// responder dispatched, rather than racing a congested drain. The incident
	// keeps its identity and still coalesces later findings; it is never dropped.
	assessment := c.config.Admission.assess(false, sample, len(c.pendingReleases))
	c.dispatchState = readyState
	if assessment.Defer {
		c.dispatchState = admissionDeferredState
		c.admissionRecheckAt = now.Add(c.config.Admission.RecheckInterval)
	}
	c.incidentFindingKeys = findingKeys(batch)
	c.noteBatchedLocked(request.IncidentID, batch)
	c.pruneTimelineLocked(batch)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseOpened, RecordedAt: contractTimestamp(c.incidentDetectedAt), IncidentId: request.IncidentID,
		FindingKeys: append([]string(nil), c.incidentFindingKeys...), SurfacedPlaybooks: playbookIDs(request.SurfacedPlaybooks),
	})
	if assessment.Defer {
		c.emitAdmissionDeferredLocked(now, request.IncidentID, assessment)
	}
	c.mu.Unlock()
	return nil
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
	if at := c.nextProgramConfirmation(); !at.IsZero() && (next.IsZero() || at.Before(next)) {
		next = at
	}
	if c.drift != nil && (next.IsZero() || c.nextDriftSweep.Before(next)) {
		next = c.nextDriftSweep
	}
	c.mu.Lock()
	// A batch deadline only matters when the batch can be dispatched or
	// attached. Otherwise an expired deadline would spin the runtime loop for
	// the whole incident.
	batchActionable := (!c.incidentOpen && (c.pendingClosure == nil || c.broker == nil)) ||
		c.canAttachBeforeLaunchLocked()
	c.mu.Unlock()
	if deadline, ok := c.batcher.Deadline(); ok && batchActionable && (next.IsZero() || deadline.Before(next)) {
		return deadline
	}
	c.mu.Lock()
	verificationDeadline := c.responderCompletedAt.Add(c.config.VerificationTimeout)
	verificationPending := c.incidentOpen && c.responderDone && !c.detectorReviewRequired
	if verificationPending && c.followUpsRemainLocked() {
		if due := c.followUpDueAtLocked(); due.Before(verificationDeadline) {
			verificationDeadline = due
		}
	}
	c.mu.Unlock()
	if verificationPending && (next.IsZero() || verificationDeadline.Before(next)) {
		next = verificationDeadline
	}
	if probe := c.nextGateProbe(); !probe.IsZero() && (next.IsZero() || probe.Before(next)) {
		next = probe
	}
	c.mu.Lock()
	if c.dispatchState == admissionDeferredState {
		// A held incident re-evaluates admission on a bounded cadence. A restored
		// controller has no recheck time yet, so it re-samples on the next tick.
		recheck := c.admissionRecheckAt
		if recheck.IsZero() {
			recheck = c.evaluationAt
		}
		if next.IsZero() || recheck.Before(next) {
			next = recheck
		}
	}
	c.mu.Unlock()
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
		DriftEvents:     driftEvents,
		DetectorHistory: compactDetectorHistory(c.history), SurfacedPlaybooks: surfacedPlaybooks(findings),
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
		if !c.responderDone {
			c.chargeDriftResponse(observedAt, completion.result)
		}
		c.responderDone = true
		if len(completion.result.RepairActions) > 0 || len(completion.result.RepairChanges) > 0 {
			c.adoptionNeeded = c.drift != nil && c.drift.Source != nil && c.drift.Client != nil
			c.nextDriftSweep = observedAt
		}
		c.responderCompletedAt = observedAt.UTC()
		if c.responderCompletedAt.Before(c.incidentDispatchedAt) {
			c.responderCompletedAt = c.incidentDispatchedAt
		}
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
		c.dispatchError = ""
		completion.result.DriftEvents = checkedDriftAssessments(*c.currentIncidentRequest, completion.result.DriftEvents)
		for _, label := range completion.result.DriftEvents {
			for i, event := range c.currentIncidentRequest.DriftEvents {
				if driftEventKey(label) == driftEventKey(event) && sameDriftObservation(label, event) {
					c.currentIncidentRequest.DriftEvents[i] = label
				}
			}
		}
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
	finalStates, verified := c.finalVerificationStates()
	if !verified {
		if c.detectorReviewRequired {
			c.mu.Unlock()
			return
		}
		if c.startFollowUpLocked(now) || c.startDriftLabelFollowUpLocked(now) || c.requireDriftLabelReviewLocked(now) {
			c.mu.Unlock()
			return
		}
		deadline := c.responderCompletedAt.Add(c.config.VerificationTimeout)
		if !now.Before(deadline) {
			c.requireDetectorReviewLocked(now, fmt.Sprintf(
				"health detectors did not clear within %s after responder completion",
				c.config.VerificationTimeout,
			))
		}
		c.mu.Unlock()
		return
	}
	if c.config.CorpusSessionRoot != "" {
		if snapshot == nil {
			var err error
			snapshot, err = c.provider.Snapshot(ctx)
			if err != nil {
				c.mu.Unlock()
				c.reportBrokerError("capture verified snapshot", err)
				return
			}
		}
		if err := memory.CaptureIncidentSnapshot(c.config.CorpusSessionRoot, c.currentIncidentRequest.IncidentID, "fixed", snapshot, c.config.Namespace); err != nil {
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
	c.observeStateChangesLocked(finalChanges, now)
	closure := IncidentClosure{
		Request: *cloneIncidentRequest(c.currentIncidentRequest), Result: cloneIncidentResult(c.currentIncidentResult),
		DispatchError: c.dispatchError, FinalDetectorStates: finalStates,
		IncidentDetectorStates: c.incidentDetectorStates(),
		FinalStateChanges:      finalChanges,
		ObservedStateChanges:   c.observedStateChangesLocked(),
		HealthClearedAt:        c.healthClearedAtLocked(finalStates),
		DetectedAt:             c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: c.responderCompletedAt, VerifiedAt: verifiedAt,
		CleanedHelpers:       append([]string(nil), c.cleanedHelpers...),
		DetectorReviewReason: c.detectorReviewReason,
	}
	if !c.detectorReviewRequiredAt.IsZero() {
		reviewAt := c.detectorReviewRequiredAt
		closure.DetectorReviewRequiredAt = &reviewAt
	}
	closure.DetectorTimeline = cloneTimeline(c.timeline)
	sortTimeline(closure.DetectorTimeline)
	closure.IncidentDetectorFiredBeforeDispatch, closure.IncidentDetectorFiredAfterDispatch,
		closure.NoIncidentDetectorFired = timelineSummary(closure.DetectorTimeline)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseClosed, RecordedAt: contractTimestamp(verifiedAt), IncidentId: closure.Request.IncidentID,
		HealthClearedAt: optionalContractTimestamp(closure.HealthClearedAt), DetectorReviewRequired: closure.DetectorReviewRequiredAt != nil,
		DispatchError: closure.DispatchError != "",
	})
	c.timeline = nil
	c.pendingClosure = cloneIncidentClosure(&closure)
	c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	c.closureReceipt = nil
	c.incidentOpen = false
	c.responderDone = false
	c.currentIncidentRequest = nil
	c.currentIncidentResult = nil
	c.dispatchError = ""
	c.incidentDetectedAt = time.Time{}
	c.incidentDispatchedAt = time.Time{}
	c.responderCompletedAt = time.Time{}
	c.detectorReviewRequired = false
	c.detectorReviewRequiredAt = time.Time{}
	c.detectorReviewReason = ""
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_IDLE
	c.incidentFindingKeys = nil
	c.cleanedHelpers = nil
	c.incidentObservedChanges = nil
	c.mu.Unlock()
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
}

// followUpAttemptLocked returns how many follow-ups the open incident chain has
// already used. The count lives in the persisted request, so a restarted
// controller resumes with the same bound.
func (c *Controller) followUpAttemptLocked() int {
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.FollowUp == nil {
		return 0
	}
	return c.currentIncidentRequest.FollowUp.Attempt
}

func (c *Controller) followUpsRemainLocked() bool {
	return c.config.MaxFollowUps > 0 && c.followUpAttemptLocked() < c.config.MaxFollowUps
}

func (c *Controller) followUpDueAtLocked() time.Time {
	cooldown := c.config.FollowUpCooldown
	if cooldown > c.config.VerificationTimeout {
		cooldown = c.config.VerificationTimeout
	}
	return c.responderCompletedAt.Add(cooldown)
}

// startFollowUpLocked replaces a completed incident whose health findings are
// still active with a follow-up request for the residual findings. It keeps
// the incident open, so exactly one responder is in flight and one closure is
// cut when verification finally succeeds. It reports whether it started one.
func (c *Controller) startFollowUpLocked(now time.Time) bool {
	if !c.followUpsRemainLocked() || now.Before(c.followUpDueAtLocked()) {
		return false
	}
	// Health detectors own verification; without any, the incident's own
	// findings are what verification waits on.
	var residualKeys []string
	if len(c.healthDetectorIDs) == 0 {
		residualKeys = c.incidentFindingKeys
	}
	residual := c.tracker.ActiveFindings(c.healthDetectorIDs, residualKeys)
	if len(residual) == 0 {
		return false
	}
	c.dispatchFollowUpLocked(now, residual, false)
	return true
}

// startDriftLabelFollowUpLocked asks the responder for the structured labels it
// left out. Quiet health leaves unlabeled unexplained drift as the only
// verification blocker, and a label can arrive only in a responder result, so
// the controller requests it once instead of waiting for VerificationTimeout.
// It is bounded separately from MaxFollowUps, which benchmarks leave at zero.
func (c *Controller) startDriftLabelFollowUpLocked(now time.Time) bool {
	unlabeled := c.unlabeledDriftBlockersLocked()
	if len(unlabeled) == 0 || c.currentIncidentRequest.FollowUp != nil && c.currentIncidentRequest.FollowUp.DriftLabelsOnly {
		return false
	}
	c.dispatchFollowUpLocked(now, unlabeled, true)
	return true
}

// requireDriftLabelReviewLocked records detector review as soon as a labels-only
// follow-up came back and the drift is still unlabeled, naming the events.
func (c *Controller) requireDriftLabelReviewLocked(now time.Time) bool {
	request := c.currentIncidentRequest
	unlabeled := c.unlabeledDriftBlockersLocked()
	if len(unlabeled) == 0 || request.FollowUp == nil || !request.FollowUp.DriftLabelsOnly {
		return false
	}
	events := make([]string, 0, len(unlabeled))
	for _, finding := range unlabeled {
		events = append(events, fmt.Sprintf("%s/%s", finding.PrimaryResource.Kind, finding.PrimaryResource.Name))
	}
	c.requireDetectorReviewLocked(now, fmt.Sprintf(
		"health is quiet but the responder returned no structured drift_events label for unexplained drift on %s",
		strings.Join(events, ", ")))
	return true
}

// unlabeledDriftBlockersLocked returns the active unexplained drift findings
// that are the only thing keeping a completed responder's incident from
// verifying: health is clear, no other drift detector is firing, and the result
// carries no completed label for them. Prose never counts as a label.
func (c *Controller) unlabeledDriftBlockersLocked() []sdk.Finding {
	if c.currentIncidentRequest == nil || c.currentIncidentResult == nil || c.dispatchError != "" {
		return nil
	}
	if len(c.healthDetectorIDs) > 0 {
		for _, id := range c.healthDetectorIDs {
			if c.tracker.HasActiveDetector(id) {
				return nil
			}
		}
		if _, clear := c.latestEvaluations(c.healthDetectorIDs, c.responderCompletedAt, true); !clear {
			return nil
		}
	}
	driftIDs := []string{}
	for _, finding := range c.currentIncidentRequest.Findings {
		if strings.HasPrefix(finding.DetectorID, "drift.") {
			driftIDs = append(driftIDs, finding.DetectorID)
		}
	}
	active := c.tracker.ActiveFindings(driftIDs, nil)
	labeled := checkedDriftAssessments(*c.currentIncidentRequest, c.currentIncidentResult.DriftEvents)
	unlabeled := []sdk.Finding{}
	for _, finding := range active {
		if finding.DetectorID != "drift.unexplained" {
			return nil
		}
		events := driftEventsForFindings(nil, []sdk.Finding{finding})
		if len(events) != 1 {
			return nil
		}
		for _, label := range labeled {
			if driftEventKey(label) == driftEventKey(events[0]) && sameDriftObservation(label, events[0]) {
				events = nil
				break
			}
		}
		if events != nil {
			unlabeled = append(unlabeled, finding)
		}
	}
	return unlabeled
}

func (c *Controller) requireDetectorReviewLocked(now time.Time, reason string) {
	c.detectorReviewRequired = true
	c.detectorReviewRequiredAt = now.UTC()
	c.detectorReviewReason = reason
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseDetectorReviewRequired, RecordedAt: contractTimestamp(c.detectorReviewRequiredAt),
		IncidentId: c.currentIncidentRequest.IncidentID, Reason: c.detectorReviewReason,
		DetectorReviewRequired: true,
	})
}

func (c *Controller) dispatchFollowUpLocked(now time.Time, residual []sdk.Finding, driftLabelsOnly bool) {
	parent := c.currentIncidentRequest
	request := c.incidentRequest(now, residual)
	if request.IncidentID == parent.IncidentID {
		request.IncidentID = fmt.Sprintf("%s-f%d", request.IncidentID, c.followUpAttemptLocked()+1)
		request.CancellationToken = "cancel-" + request.IncidentID
	}
	original := parent.IncidentID
	priorSummary := ""
	if parent.FollowUp != nil {
		original = parent.FollowUp.OriginalIncidentID
		priorSummary = parent.FollowUp.PriorSummary
	}
	attempt := c.followUpAttemptLocked() + 1
	request.FollowUp = &FollowUpContext{
		OriginalIncidentID: original, ParentIncidentID: parent.IncidentID,
		Attempt: attempt, MaxFollowUps: max(c.config.MaxFollowUps, attempt),
		PriorSummary:    boundedFollowUpSummary(priorSummary, followUpSummary(parent, c.currentIncidentResult, c.dispatchError)),
		DriftLabelsOnly: driftLabelsOnly,
	}
	request.RepositoryWorktree = parent.RepositoryWorktree
	request.RepositoryBaseCommit = parent.RepositoryBaseCommit
	c.batcher.RemoveKeys(findingKeys(residual))
	// With a broker the follow-up re-prepares a distinct worktree (keyed on its
	// own incident id), so the parent's prepared worktree is abandoned without a
	// closure and must be released at the source. Without a broker the follow-up
	// reuses the parent's worktree in place (set above), so there is nothing to
	// release.
	if c.broker != nil && parent.IncidentID != "" && parent.IncidentID != request.IncidentID {
		c.enqueueReleaseLocked(parent.IncidentID)
	}
	supersededAt := now.UTC()
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		EventId: lifecycleEventID(parent.IncidentID, string(PhaseSuperseded), request.IncidentID),
		Event:   PhaseSuperseded, RecordedAt: contractTimestamp(supersededAt), IncidentId: parent.IncidentID,
		SupersededBy: request.IncidentID, Reason: "follow_up",
	})
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseOpened, RecordedAt: contractTimestamp(supersededAt), IncidentId: request.IncidentID,
		ParentIncidentId: parent.IncidentID, FollowUpAttempt: int32(request.FollowUp.Attempt),
		FindingKeys: findingKeys(residual), SurfacedPlaybooks: playbookIDs(request.SurfacedPlaybooks),
	})
	c.currentIncidentRequest = cloneIncidentRequest(&request)
	c.currentIncidentResult = nil
	c.dispatchError = ""
	c.responderDone = false
	c.responderCompletedAt = time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	if c.broker != nil {
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
	}
	c.incidentFindingKeys = findingKeys(residual)
}

// enqueueReleaseLocked records an incident whose prepared worktree was stranded
// by a supersede so the run loop reaps it through the broker. Idempotent;
// callers hold c.mu.
func (c *Controller) enqueueReleaseLocked(incidentID string) {
	for _, id := range c.pendingReleases {
		if id == incidentID {
			return
		}
	}
	c.pendingReleases = append(c.pendingReleases, incidentID)
}

const followUpSummaryLimit = 3000

func followUpSummary(request *IncidentRequest, result *IncidentResult, dispatchError string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "Responder for incident %s was dispatched for:", request.IncidentID)
	for _, finding := range request.Findings {
		fmt.Fprintf(&out, " [%s/%s %s]", finding.DetectorID, finding.RuleID, finding.Summary)
	}
	out.WriteString(". ")
	switch {
	case result == nil && dispatchError != "":
		fmt.Fprintf(&out, "It failed: %s.", dispatchError)
	case result == nil:
		out.WriteString("It reported no result.")
	default:
		fmt.Fprintf(&out, "It finished with status %s.", result.Status)
		for _, cause := range result.ConfirmedRootCauses {
			fmt.Fprintf(&out, " Root cause: %s.", cause.Summary)
		}
		for _, change := range result.RepairChanges {
			fmt.Fprintf(&out, " Repair: %s.", change)
		}
		for _, action := range result.RepairActions {
			fmt.Fprintf(&out, " Action: %s.", action.Summary)
		}
		if result.Error != "" {
			fmt.Fprintf(&out, " Error: %s.", result.Error)
		}
	}
	return out.String()
}

func boundedFollowUpSummary(previous string, current string) string {
	combined := current
	if previous != "" {
		combined = previous + "\n" + current
	}
	if len(combined) > followUpSummaryLimit {
		combined = combined[len(combined)-followUpSummaryLimit:]
	}
	return combined
}

func (c *Controller) finalVerificationStates() ([]DetectorEvaluation, bool) {
	if len(c.healthDetectorIDs) == 0 {
		if c.tracker.HasActiveKeys(c.incidentFindingKeys) {
			return nil, false
		}
		ids := make([]string, 0)
		seen := make(map[string]struct{})
		requireDriftClear := false
		for _, finding := range c.currentIncidentRequest.Findings {
			if strings.HasPrefix(finding.DetectorID, "drift.") {
				requireDriftClear = true
			}
			if _, ok := seen[finding.DetectorID]; !ok {
				seen[finding.DetectorID] = struct{}{}
				ids = append(ids, finding.DetectorID)
			}
		}
		sort.Strings(ids)
		if requireDriftClear {
			return c.latestEvaluations(ids, c.responderCompletedAt, true)
		}
		return c.latestEvaluations(ids, time.Time{}, false)
	}
	for _, detectorID := range c.healthDetectorIDs {
		if c.tracker.HasActiveDetector(detectorID) {
			return nil, false
		}
	}
	ids := append([]string(nil), c.healthDetectorIDs...)
	for _, finding := range c.currentIncidentRequest.Findings {
		if strings.HasPrefix(finding.DetectorID, "drift.") {
			if c.tracker.HasActiveDetector(finding.DetectorID) {
				return nil, false
			}
			found := false
			for _, id := range ids {
				found = found || id == finding.DetectorID
			}
			if !found {
				ids = append(ids, finding.DetectorID)
			}
		}
	}
	return c.latestEvaluations(ids, c.responderCompletedAt, true)
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

// observeStateChangesLocked records the first time the open incident's diff
// showed each object changed. Called with c.mu held.
func (c *Controller) observeStateChangesLocked(changes *StateChanges, now time.Time) {
	if changes == nil || !c.incidentOpen {
		return
	}
	if c.incidentObservedChanges == nil {
		c.incidentObservedChanges = make(map[string]ObservedStateChange)
	}
	for _, change := range changes.Changes {
		key := change.Kind + "/" + change.Name
		if _, seen := c.incidentObservedChanges[key]; seen {
			continue
		}
		c.incidentObservedChanges[key] = ObservedStateChange{
			Kind: change.Kind, Name: change.Name, FirstObservedAt: now.UTC(),
		}
	}
}

// observedStateChangesLocked lists the open incident's observed changes by
// first observation; nil without a baseline, when nothing can be checked.
// Called with c.mu held.
func (c *Controller) observedStateChangesLocked() []ObservedStateChange {
	if c.Baseline == nil || c.incidentObservedChanges == nil {
		return nil
	}
	observed := make([]ObservedStateChange, 0, len(c.incidentObservedChanges))
	for _, entry := range c.incidentObservedChanges {
		observed = append(observed, entry)
	}
	sort.Slice(observed, func(left int, right int) bool {
		if !observed[left].FirstObservedAt.Equal(observed[right].FirstObservedAt) {
			return observed[left].FirstObservedAt.Before(observed[right].FirstObservedAt)
		}
		if observed[left].Kind != observed[right].Kind {
			return observed[left].Kind < observed[right].Kind
		}
		return observed[left].Name < observed[right].Name
	})
	return observed
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
	closureState := c.closureState
	if closureState == contracts.ClosureState_CLOSURE_STATE_PROCESSING {
		closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	} else if closureState == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
		closureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
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
		DetectorReviewRequired: c.detectorReviewRequired, DetectorReviewRequiredAt: optionalTimestamp(c.detectorReviewRequiredAt),
		DetectorReviewReason: c.detectorReviewReason, DetectorClearSince: timestampsContract(c.detectorClearSince),
		IncidentObservedChanges: observedStateChangesContract(c.observedStateChangesLocked()), DispatchState: dispatchState,
		IncidentFindingKeys: append([]string(nil), c.incidentFindingKeys...), ClosureState: closureState,
		ClosureReceipt: closureReceiptContract(c.closureReceipt), ClosureFailure: closureFailureContract(c.closureFailure),
		LastAcknowledgedIncidentId: c.lastAcknowledgedIncidentID, PendingReleases: append([]string(nil), c.pendingReleases...),
		EvaluationIteration: int32(c.evaluationIteration), DetectorTimeline: timeline, IncidentView: view,
		DriftEvents: driftEventsContract(c.driftEvents), OwnershipAdoption: adoptionContract(c.ownershipAdoption), DriftAdmission: cloneDriftAdmission(c.driftAdmission), ProgramSweepCursors: maps.Clone(c.programSweepCursors),
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
	return message, nil
}

func (c *Controller) RestoreState(state *contracts.ControllerState) error {
	if err := validateRuntimeState(state); err != nil {
		return err
	}
	if err := c.scheduler.RestoreDeadlines(timestampsState(state.GetSchedulerDeadlines())); err != nil {
		return err
	}
	c.tracker.Restore(findingStatesState(state.GetFindingStates()))
	c.batcher.Restore(BatcherState{Findings: findingsState(state.GetPendingBatch().GetFindings()), Deadlines: timestampsState(state.GetPendingBatch().GetDeadlines()), Deadline: controllerTime(state.GetPendingBatch().GetDeadline())})
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
	c.driftAdmission = cloneDriftAdmission(state.DriftAdmission)
	c.programSweepCursors = maps.Clone(state.ProgramSweepCursors)
	c.ownershipAdoption = adoptionState(state.GetOwnershipAdoption())
	c.dispatchError = state.DispatchError
	c.incidentDetectedAt = controllerTime(state.GetIncidentDetectedAt())
	c.incidentDispatchedAt = controllerTime(state.GetIncidentDispatchedAt())
	c.responderCompletedAt = controllerTime(state.GetResponderCompletedAt())
	c.detectorReviewRequired = state.DetectorReviewRequired
	c.detectorReviewRequiredAt = controllerTime(state.GetDetectorReviewRequiredAt())
	c.detectorReviewReason = state.DetectorReviewReason
	c.detectorClearSince = cloneClearSince(timestampsState(state.GetDetectorClearSince()))
	c.incidentObservedChanges = nil
	if state.IncidentOpen {
		c.incidentObservedChanges = make(map[string]ObservedStateChange, len(observedStateChangesState(state.GetIncidentObservedChanges())))
		for _, entry := range observedStateChangesState(state.GetIncidentObservedChanges()) {
			c.incidentObservedChanges[entry.Kind+"/"+entry.Name] = entry
		}
	}
	c.dispatchState = state.DispatchState
	c.incidentFindingKeys = append([]string(nil), state.IncidentFindingKeys...)
	c.pendingClosure = nil
	if state.PendingClosure != nil {
		closure := incidentClosureState(state.PendingClosure)
		c.pendingClosure = &closure
	}
	c.closureState = state.ClosureState
	if c.pendingClosure != nil && c.closureState == contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED {
		c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	}
	c.closureReceipt = cloneClosureReceipt(closureReceiptState(state.GetClosureReceipt()))
	c.closureFailure = cloneClosureFailure(closureFailureState(state.GetClosureFailure()))
	c.lastAcknowledgedIncidentID = state.GetLastAcknowledgedIncidentId()
	c.pendingReleases = append([]string(nil), state.PendingReleases...)
	c.releaseInFlight = ""
	c.evaluationIteration = int(state.GetEvaluationIteration())
	c.timeline = cloneTimeline(detectorTimelineState(state.GetDetectorTimeline()))
	return nil
}

func (c *Controller) LastAcknowledgedIncidentID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAcknowledgedIncidentID
}

func (c *Controller) DetectorReviewRequired() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.detectorReviewRequired
}

func (c *Controller) DetectorReviewStatus() (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.detectorReviewRequired, c.detectorReviewReason
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
	revision, err := c.stateStore.Save(actionCtx, state, c.stateRevision)
	if err != nil {
		// The write may or may not have landed; no effect recorded since the
		// last confirmed save may run until a later save confirms it.
		c.mu.Lock()
		c.durable = nil
		c.mu.Unlock()
		return err
	}
	c.stateRevision = revision
	c.mu.Lock()
	c.durable = proto.Clone(state).(*contracts.ControllerState)
	c.mu.Unlock()
	return nil
}

func (c *Controller) actionContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
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
