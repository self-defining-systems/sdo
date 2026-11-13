package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"log/slog"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/core"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

type SnapshotProvider interface {
	Snapshot(context.Context) (sdk.DetectionContext, error)
}

type Dispatcher interface {
	Dispatch(context.Context, IncidentRequest) (IncidentResult, error)
}

type ControllerConfig struct {
	ExternalIncidentsOnly bool
	// WatchOnly detects, opens, verifies and closes incidents as usual but never
	// responds: the controller's dispatcher is replaced by one that idles until
	// the incident ends, and no automatic stage is selected.
	WatchOnly bool
	// Telemetry is the launch revision's telemetry context; a memory swap
	// replaces it with the new revision's (RemoteMemory.Telemetry).
	Telemetry        *contracts.TelemetryContext
	Replay           string
	CodePath         CodePathConfig
	CodePathRegistry *CodePathRegistry

	Application        string
	Namespace          string
	SourceCommit       string
	DeployedCommit     string
	RepositoryWorktree string
	CorpusSessionRoot  string
	// AppStateChanges supplies `sdo changes` app-state rows; nil lists none.
	AppStateChanges        AppStateChangeSource
	ResponseTimeout        time.Duration
	VerificationTimeout    time.Duration
	ClosureStabilityWindow time.Duration
	// StageBudget bounds one automatic-stage pass; a pass re-run on the
	// responder's loop gets twice this.
	StageBudget   time.Duration
	BatchDebounce time.Duration
	// ConfirmationInterval bounds how long a detector with a finding below its
	// firing threshold waits for its next evaluation. Zero leaves confirmation
	// to watch events and the detector's own interval.
	ConfirmationInterval time.Duration
	RepairPolicy         string
	// AfterPlaybook and AfterPlaybookWait are response.after_playbook and
	// response.after_playbook_wait; empty AfterPlaybook is the agent policy.
	AfterPlaybook     string
	AfterPlaybookWait time.Duration
	// ClosureRetry bounds resubmission of a closure the broker rejects.
	ClosureRetry ClosureRetryPolicy
	// DispatchRetry bounds how quickly a transient dispatch failure
	// re-executes the dispatch effect.
	DispatchRetry DispatchRetryPolicy
	// GateConfirmation is the responder gate's clear hysteresis, published in
	// the incident view. Zero makes the gate follow closure's clear count.
	GateConfirmation GateConfirmationPolicy
	// StartupBound is how long a new install holds its openers until the
	// application is seen healthy (startupPeriod). Zero runs no period.
	StartupBound time.Duration
}

type dispatchCompletion struct {
	priorStages []*contracts.StageReport
	incidentID  string
	result      IncidentResult
	err         error
}

type Controller struct {
	// signals is the 15-minute ring the incident timeline is drawn from.
	signals           *incidentSignals
	ExternalIncidents func(context.Context) (*ExternalIncident, error)
	memoryRefresh     func(context.Context) (bool, error)
	config            ControllerConfig
	// closureRecheckIdentity/Since bound an incomplete closure-time recheck.
	closureRecheckIdentity      string
	closureRecheckSince         time.Time
	closureRecheckRetryAt       time.Time
	codePath                    *CodePath
	startedAt                   time.Time
	scheduler                   *Scheduler
	detectors                   map[string]sdk.Detector
	tracker                     *FindingStateTracker
	batcher                     *Batcher
	provider                    SnapshotProvider
	dispatcher                  Dispatcher
	responseStagesRunning       bool
	midStage                    midIncidentStage
	history                     []DetectorEvaluation
	drift                       *DriftControllerConfig
	driftEvents                 []sdk.DriftEvent
	sourceInventory             *SourceInventory
	installEnvironment          *contracts.NamespaceOnboarding
	pendingOnboarding           *contracts.NamespaceOnboarding
	onboardingReachability      *onboardingReachabilityState
	onboardingClient            kubernetes.Interface
	onboardingControl           string
	namespaceOnboarding         *contracts.NamespaceOnboarding
	disabledSetupDetectors      []*contracts.DisabledSetupDetector
	setupEvaluatedRevision      string
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
	// background tracks every goroutine an effect starts (responder dispatch,
	// broker workspace/closure/acknowledgment, open@detect routing sweeps).
	// They write under the session root until they return; Wait joins them.
	background        sync.WaitGroup
	ownershipAdoption OwnershipAdoptionStatus
	// acceptanceSettleWaits counts consecutive adoption passes that waited
	// for an object to settle; it spaces the next pass.
	acceptanceSettleWaits int
	adoptionNeeded        bool
	results               chan dispatchCompletion
	// gateClears and gateProbes hold the submit gate's confirmation of active
	// findings (GateConfirmationPolicy); they are never persisted, so a
	// restarted controller confirms again from fresh evaluations.
	gateClears    map[string]*gateClear
	gateProbes    map[string]time.Time
	persistenceMu sync.Mutex
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
	durable *contracts.ControllerState
	// checkpoint survives effect gating failures: recovery must retain accepted receipts.
	checkpoint              *contracts.ControllerState
	incidentOpen            bool
	responderDone           bool
	responderLoop           *ResponderLoop
	responderCancel         context.CancelFunc
	finishedHoldWake        time.Time
	earlyStopRetryAt        time.Time
	pausedEffectRetryAt     time.Time
	recoveryObserverStarted time.Time
	recoverySafetyLost      bool
	closureStableSince      time.Time
	closureMutationRevision string
	earlyLastMutationAt     time.Time
	cachedVerification      *ToolResponse
	cachedIncidentID        string
	verificationChanged     chan struct{}
	findingGeneration       uint64
	currentIncidentRequest  *IncidentRequest
	dispatchState           contracts.DispatchState
	incidentFindingKeys     []string
	cleanedHelpers          []string
	healthDetectorIDs       []string
	detectorSpecs           map[string]sdk.DetectorSpec
	currentIncidentResult   *IncidentResult
	dispatchError           string
	incidentDetectedAt      time.Time
	incidentDispatchedAt    time.Time
	responderCompletedAt    time.Time
	escalatedAt             time.Time
	// detectorClearSince maps each detector to the start of its current
	// streak of clear evaluations; a firing or erroring evaluation ends
	// the streak. Guarded by mu.
	detectorClearSince                 map[string]time.Time
	pendingClosure                     *IncidentClosure
	closureState                       contracts.ClosureState
	closureReceipt                     *ClosureReceipt
	closureFailure                     *ClosureFailure
	failedClosures                     []*contracts.FailedIncidentClosure
	dispatchFailureAttempts            int
	dispatchNextRetryAt                time.Time
	lastAcknowledgedIncidentID         string
	lastAcknowledgedExternalIncidentID string
	incidentView                       *IncidentView
	broker                             IncidentBroker
	gitRead                            gitRead
	openers                            openers
	workspaceResults                   chan workspaceCompletion
	closureResults                     chan closureCompletion
	acknowledgmentResults              chan acknowledgmentCompletion
	reflectionResults                  chan reflectionCompletion
	head                               headPollState
	reflections                        reflectionQueueState
	busy                               busyWindow
	// startup holds a new install's openers until the application is seen
	// healthy or the bound passes; nil when the period is off.
	startup *startupPeriod

	// Baseline, when set, records healthy configuration and attaches the
	// diff against it to each new incident request.
	Baseline StateBaseline
	// PodReadBaseline, when set, receives the pod readings of each promoted
	// healthy baseline; the snapshot provider carries them with current reads.
	PodReadBaseline *PodReadBaseline
	// VantageBaseline, when set, receives the client dial readings of each
	// promoted healthy baseline.
	VantageBaseline *VantageBaseline
	// AppState, when set, receives each evaluation's health so last-healthy
	// app state settles like the other baselines.
	AppState *AppStateStore
	// Helpers, when set, deletes responder helper objects once the
	// responder completes, before recovery is verified.
	Helpers HelperCleaner
	// firingSink receives detector firing telemetry; nil disables it.
	firingSink FiringSink
	// lifecycleSink receives incident lifecycle telemetry; nil disables it.
	lifecycleSink LifecycleSink
	// pendingLifecycle holds incident_opened events until the state that
	// contains their incident is saved.
	pendingLifecycle []*contracts.LifecycleEvent
	// evaluationIteration counts evaluation passes that took a snapshot. It is
	// durable so telemetry iterations keep increasing across restarts.
	evaluationIteration int
	// stepCount is the heartbeat: Step passes begun with observationMu held,
	// durable so it never runs backwards across a restart.
	stepCount int64
	// evaluationAt is the time of the evaluation pass in progress.
	evaluationAt time.Time
	// timeline summarizes each finding's firing history since the last incident
	// closure was cut.
	timeline []DetectorTimelineEntry
	// lateJoinedKeys are the open incident's findings that joined its request
	// after the responder launched (learned matches, late drift). They stay
	// after_dispatch in telemetry; restore rebuilds them from the timeline.
	lateJoinedKeys map[string]struct{}
	stateRestored  bool

	// CheckHealth refuses evaluation and effects while the compiled detector bundle
	// lags the accepted health policy; the rebuilt controller resumes the work.
	CheckHealth      func(context.Context) error
	OnError          func(error)
	OnResult         func(IncidentResult)
	OnEvaluation     func([]sdk.Finding)
	OnIncidentClosed func(IncidentClosure)
	// closureEvidence replaces the default capture and settlement of a verified
	// closure; tests use it to make that work slow or fail.
	closureEvidence func(context.Context, closureEvidenceInput) (closureEvidence, error)
	closurePrep     *closurePreparation
	closurePrepared chan struct{}
	OnClosureFailed func(ClosureFailure)
	CanAct          func() bool
	GuardAction     func(context.Context) (context.Context, context.CancelFunc, error)
	// now overrides the wall clock for closure and dispatch retry backoff in
	// tests.
	now func() time.Time
	// dispatchJitter overrides the dispatch retry jitter source in tests; it
	// must return a value in [0, 1). Nil defaults to a real random source.
	dispatchJitter func() float64
}

// WithSettings fills the timing and policy fields from the operator
// configuration. Every NewController caller (the controller, --evaluate-once,
// embedded runs and tests) takes them from settings.Default() or settings.Read;
// the mode fields stay the caller's, since an evaluation runs every detector
// whatever the mode.
func (c ControllerConfig) WithSettings(policy settings.Config) ControllerConfig {
	c.Replay = policy.Learning.Replay
	c.ResponseTimeout = time.Duration(policy.Response.Timeout)
	c.VerificationTimeout = time.Duration(policy.Response.VerificationTimeout)
	c.StageBudget = time.Duration(policy.Response.StageBudget)
	c.ClosureStabilityWindow = time.Duration(policy.Detection.StabilityWindow)
	c.RepairPolicy = policy.Response.RepairPolicy
	c.StartupBound = time.Duration(policy.Response.StartupBound)
	c.AfterPlaybook = policy.Response.AfterPlaybook
	c.AfterPlaybookWait = time.Duration(policy.Response.AfterPlaybookWait)
	return c
}

// WithMode applies the operator configuration's mode: watch-only never
// responds, external accepts external alerts only. Frozen memory is the
// broker's (NativeIncidentBroker.FrozenMemory).
func (c ControllerConfig) WithMode(mode settings.Mode) ControllerConfig {
	c.ExternalIncidentsOnly, c.WatchOnly = mode == settings.ModeExternal, mode == settings.ModeWatchOnly
	return c
}

func NewController(
	config ControllerConfig,
	detectors []sdk.Detector,
	provider SnapshotProvider,
	dispatcher Dispatcher,
	start time.Time,
) (*Controller, error) {
	if _, err := settings.Replay(config.Replay); err != nil {
		return nil, err
	}
	if config.Replay == "off" {
		config.CorpusSessionRoot = ""
	}
	if config.ExternalIncidentsOnly {
		detectors = nil
		config.CodePath.Overrides.ResponseStages = []string{"agent"}
	}
	if config.WatchOnly {
		if config.ExternalIncidentsOnly {
			return nil, fmt.Errorf("watch-only conflicts with external incidents")
		}
		config.CodePath.Overrides.ResponseStages = []string{"agent"}
		dispatcher = watchOnlyDispatcher{}
	}
	registry := config.CodePathRegistry
	if registry == nil {
		registry = NewCodePathRegistry()
	}
	resolvedPath, err := registry.Resolve(config.CodePath)
	if err != nil {
		return nil, err
	}
	if provider == nil {
		return nil, fmt.Errorf("snapshot provider is required")
	}
	if dispatcher == nil {
		return nil, fmt.Errorf("dispatcher is required")
	}
	// Timing and policy come from the operator configuration (WithSettings);
	// the controller has no defaults of its own for them.
	if config.ResponseTimeout <= 0 || config.VerificationTimeout <= 0 || config.StageBudget <= 0 {
		return nil, fmt.Errorf("response timeout, verification timeout and stage budget must be positive (ControllerConfig.WithSettings)")
	}
	if config.ClosureStabilityWindow <= 0 {
		return nil, fmt.Errorf("closure stability window must be positive (ControllerConfig.WithSettings)")
	}
	if config.ConfirmationInterval < 0 {
		return nil, fmt.Errorf("confirmation interval must not be negative")
	}
	if err := config.GateConfirmation.validate(); err != nil {
		return nil, err
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
	tracker := NewFindingStateTracker()
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
	var startup *startupPeriod
	if config.StartupBound > 0 && !config.ExternalIncidentsOnly {
		startup = newStartupPeriod(start.UTC(), start.UTC().Add(config.StartupBound))
	}
	return &Controller{
		signals: newIncidentSignals(),
		config:  config, codePath: resolvedPath, startedAt: start, scheduler: NewScheduler(detectors, start, config.Namespace), detectors: byID,
		tracker: tracker, startup: startup,
		gateClears: make(map[string]*gateClear), gateProbes: make(map[string]time.Time),
		batcher: NewDebouncedBatcher(config.BatchDebounce), provider: provider, dispatcher: dispatcher,
		results: make(chan dispatchCompletion, 1), healthDetectorIDs: healthDetectorIDs,
		detectorSpecs:    detectorSpecs,
		workspaceResults: make(chan workspaceCompletion, 1), closureResults: make(chan closureCompletion, 1), closurePrepared: make(chan struct{}, 1),
		acknowledgmentResults: make(chan acknowledgmentCompletion, 1),
		reflectionResults:     make(chan reflectionCompletion, 1), reflections: reflectionQueueState{rescan: true},
		dispatchJitter: lifecycleio.DispatchJitter,
	}, nil
}

func (c *Controller) Step(ctx context.Context, now time.Time, event *sdk.WatchKind) error {
	if event == nil {
		return c.StepEvents(ctx, now, nil)
	}
	return c.StepEvents(ctx, now, []sdk.WatchKind{*event})
}

// beatLocked stamps the heartbeat. It runs first in every step pass once
// observationMu is held, before anything that can fail or block, so only a
// blocked step loop stops it (evaluation_iteration also stops on a failed
// snapshot, an idle pass, or long detector intervals).
func (c *Controller) beatLocked() {
	c.mu.Lock()
	c.stepCount++
	c.mu.Unlock()
}

// DrainObservation returns once every pass that was observing has finished. A
// maintenance pause is acknowledged only after it: an evaluation that began
// before the pause still completes (and may open its incident) first, and none
// starts after, because the run loop observes nothing while paused.
func (c *Controller) DrainObservation() {
	c.lockObservation("pause_barrier")()
}

// stepPaused advances only work belonging to an already accepted closure. A
// maintenance pause suspends observation/admission, not the report deadline or
// outcome and reflection bookkeeping. Persist before exposing broker effects.
func (c *Controller) stepPaused(ctx context.Context, now time.Time) error {
	defer c.lockObservation("step_paused")()
	c.beatLocked()
	if err := c.requireAdoptedHealth(ctx); err != nil {
		return err
	}
	before, err := c.ExportState()
	if err != nil {
		return err
	}
	// Harvest a queued responder result before retirement can judge its
	// incident ownerless. Ownerless work is bookkeeping, not observation: a
	// pause must not hold it.
	c.processDispatchCompletions(now)
	if err := c.retireExpiredIncident(ctx, now); err != nil {
		return err
	}
	c.finishEarlyReport(ctx, now)
	c.processBrokerCompletions()
	if err := c.driveQueuedReflections(ctx, now); err != nil {
		return err
	}
	after, err := c.ExportState()
	if err != nil {
		return err
	}
	c.mu.Lock()
	// A failed ACTIVE save can enter maintenance with no confirmed state.
	retry := !c.pausedEffectRetryAt.IsZero() || (c.stateStore != nil && c.durable == nil)
	c.mu.Unlock()
	if proto.Equal(before, after) && !retry {
		return nil
	}
	if err := c.PersistState(ctx); err != nil {
		c.mu.Lock()
		c.pausedEffectRetryAt = now.Add(time.Second)
		c.mu.Unlock()
		c.reportBrokerError("persist paused controller state", err)
		return nil
	}
	c.mu.Lock()
	c.pausedEffectRetryAt = time.Time{}
	c.mu.Unlock()
	return nil
}

// StepEvents evaluates, against one snapshot, every detector that is due or
// watches any of the coalesced watch events.
func (c *Controller) StepEvents(ctx context.Context, now time.Time, events []sdk.WatchKind) error {
	defer c.lockObservation("step_events")()
	c.beatLocked()
	if err := c.requireAdoptedHealth(ctx); err != nil {
		return err
	}
	// Harvest a queued responder result before retirement can judge its
	// incident ownerless.
	c.processDispatchCompletions(now)
	if err := c.retireExpiredIncident(ctx, now); err != nil {
		return err
	}
	c.finishEarlyReport(ctx, now)
	c.processBrokerCompletions()
	if err := c.driveQueuedReflections(ctx, now); err != nil {
		return err
	}
	c.driveHeadPoll(ctx, now)
	if c.config.ExternalIncidentsOnly {
		c.maybeCloseIncident(ctx, now, nil)
		if c.ExternalIncidents != nil {
			alert, err := c.ExternalIncidents(ctx)
			if err != nil {
				return err
			}
			if alert != nil {
				if err := c.openExternalIncident(now, *alert); err != nil && !errors.Is(err, ErrExternalIncidentBusy) {
					return err
				}
			}
		}
		c.refreshIncidentView(now, false)
		return nil
	}
	if err := c.prepareSourceInventory(ctx); err != nil {
		return err
	}
	deferInventory := c.pendingOnboarding != nil
	if deferInventory {
		c.observePendingOnboarding(ctx, now)
		// Persist known contradictions before snapshot acquisition or an idle
		// return: either may fail, and a restart must not erase this evidence.
		if c.onboardingReachability != nil && c.onboardingReachability.invalid {
			if err := c.rejectPendingOnboarding(ctx, c.onboardingRejectionReason(false)); err != nil {
				return err
			}
			deferInventory = false
			c.observeSourceInventory(now)
		}
	} else {
		c.observeSourceInventory(now)
	}
	detectors := c.enabledSetupDetectors(c.scheduler.SelectEvents(now, events))
	probes := c.dueGateProbes(now, detectors)
	var objectChanges []ObjectChange
	if c.drift != nil {
		if c.drift.Events != nil {
			objectChanges = c.drift.Events.TakeObjectChanges()
		}

	}
	if len(detectors) == 0 && len(probes) == 0 && len(objectChanges) == 0 && !c.adoptionNeeded && !c.hasRetainedDrift() {
		c.forgetGate()
		c.settleGateProbes(now)
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
	c.signals.Observe(now, snapshot)

	// Startup's full batch may precede confirmation. Once the observation
	// windows are ready, evaluate every detector together on this snapshot;
	// event-driven partial batches must not strand the install opportunity.
	setupSweep := c.namespaceOnboarding != nil && c.setupEvaluatedRevision != c.config.SourceCommit && c.setupIncidentFree(false)
	onboardingDetectors := detectors
	if c.pendingOnboarding != nil || setupSweep {
		onboardingDetectors = c.detectorList()
	}
	unreferencedSnapshot := snapshot
	snapshot = c.prepareOnboardingReachability(ctx, now, snapshot, onboardingDetectors)
	var onboardingCandidate *contracts.NamespaceOnboarding
	confirmed := detectorsConfirmed(onboardingDetectors, snapshot)
	if !confirmed && c.pendingOnboarding != nil {
		var reasons []string
		for _, detector := range onboardingDetectors {
			if !detectorConfirmed(detector, snapshot) {
				reasons = append(reasons, detector.Spec().ID+": "+detectorConfirmationReason(detector, snapshot))
			}
		}
		c.waitForOnboarding(ctx, strings.Join(reasons, "; "))
	}
	confirmationExpired := c.onboardingConfirmationExpired(now, snapshot)
	if c.pendingOnboarding != nil && (confirmationExpired || c.onboardingReachability != nil && c.onboardingReachability.invalid) {
		if err := c.rejectPendingOnboarding(ctx, c.onboardingRejectionReason(confirmationExpired)); err != nil {
			return err
		}
	}
	if c.pendingOnboarding != nil || setupSweep {
		if len(detectors) != len(onboardingDetectors) && (setupSweep || detectorsNeedConfirmation(onboardingDetectors)) && (confirmed || confirmationExpired) {
			// This extra install sweep must not postpone ordinary fault checks.
			deadlines := c.scheduler.Deadlines()
			c.scheduler.ExpediteAll(now)
			detectors = c.enabledSetupDetectors(c.scheduler.SelectEvents(now, nil))
			c.scheduler.RestoreDeadlines(deadlines)
		}
	}
	if confirmed || confirmationExpired {
		onboardingCandidate = c.beginOnboarding(ctx, detectors, now)
	}
	// An earlier claimed/crashed attempt cannot lend provisional exclusions to
	// this process. A successfully published record uses its durable reference.
	if onboardingCandidate == nil && c.pendingOnboarding == nil && c.namespaceOnboarding == nil {
		snapshot = unreferencedSnapshot
		if c.onboardingReachability != nil {
			c.onboardingReachability.candidate = nil
		}
	}
	setupResults, setupHealthy := c.setupHealth(ctx, snapshot, detectors, onboardingCandidate)
	sampleFindings := make([]sdk.Finding, 0)
	errored := confirmationExpired
	c.mu.Lock()
	c.evaluationIteration++
	c.evaluationAt = now.UTC()
	c.mu.Unlock()
	// Evaluate the batch concurrently and apply each result as its detector
	// returns, so a hung or slow detector never delays another's finding. Each
	// detector's tracker state is its own; the one cross-detector step, opening
	// an incident early, is gated by earlyDispatchSafe.
	skipLearned := onboardingCandidate == nil && (c.pendingOnboarding != nil || setupSweep && !confirmed && !confirmationExpired)
	var fresh []sdk.Detector
	for _, detector := range detectors {
		spec := detector.Spec()
		if skipLearned && learnedIncidentDetector(spec) {
			continue
		}
		if _, cached := setupResults[spec.ID]; !cached {
			fresh = append(fresh, detector)
		}
	}
	var dispatchErr error // first early-dispatch error; the step still finishes
	pending := make(map[string]time.Duration, len(fresh))
	for _, detector := range fresh {
		pending[detector.Spec().ID] = detector.Spec().Interval
	}
	// earlyDispatchSafe reports that only slow detectors are still running and
	// none of them holds a firing opener a clean result could still clear, so
	// opening the incident now cannot differ from opening it at the end.
	earlyDispatchSafe := func() bool {
		for id, interval := range pending {
			if interval < slowDetectorInterval || c.openerFiring(id) {
				return false
			}
		}
		return true
	}
	apply := func(detector sdk.Detector, result setupDetection) error {
		spec := detector.Spec()
		delete(pending, spec.ID)
		findings, status, detectErr := validateDetectorObservation(detector, snapshot, result.findings, result.err)
		if detectErr != nil {
			c.resetGate(spec.ID)
			c.recordDetectorError(spec.ID, now, detectErr)
			errored = true
			return nil
		}
		if status == DetectorEvaluationPending {
			c.resetGate(spec.ID)
			c.history = append(c.history, DetectorEvaluation{DetectorID: spec.ID, EvaluatedAt: now.UTC(), Status: DetectorEvaluationPending, Reason: detectorConfirmationReason(detector, snapshot)})
			if len(c.history) > 100 {
				c.history = append([]DetectorEvaluation(nil), c.history[len(c.history)-100:]...)
			}
			c.trackClearStreak(spec.ID, now, false)
			return nil
		}
		validFindings := findings
		if setupHealthy && learnedIncidentDetector(spec) && c.disableSetupDetector(spec, validFindings, now, onboardingCandidate) {
			return nil
		}
		changes := c.tracker.Observe(now, spec.ID, validFindings)
		c.observeGate(spec.ID, now, validFindings)
		sampleFindings = append(sampleFindings, validFindings...)
		c.recordEvaluation(spec.ID, now, validFindings, c.recoveryScope())
		c.history[len(c.history)-1].Reason = detectorConfirmationReason(detector, snapshot)
		c.noteStartupEvaluation(spec.ID)
		c.batcher.RemoveKeys(changes.Cleared)
		if c.config.ConfirmationInterval > 0 && c.tracker.HasPendingDetector(spec.ID) {
			c.scheduler.Expedite(spec.ID, now.Add(c.config.ConfirmationInterval))
		}
		c.mu.Lock()
		if len(c.recoveryScopeLocked().blocking(changes.Activated)) > 0 {
			c.closureStableSince = time.Time{}
		}
		c.mu.Unlock()
		for _, finding := range changes.Activated {
			c.admitActivation(ctx, finding, now, c.detectorSpecs[spec.ID])
		}
		c.recordFiring(spec, now, validFindings, changes)
		if len(changes.Activated) == 0 || len(pending) == 0 || !earlyDispatchSafe() {
			return nil
		}
		// Every fast detector is applied and only slow ones remain: the
		// incident opens now rather than after the slowest returns.
		return c.dispatchReady(ctx, now, snapshot)
	}
	// Results cached by setup health apply first, in batch order; the rest
	// apply in completion order, so a fast detector's finding never waits for
	// a slow one. Each detector owns its own tracker state, so the order
	// between detectors changes no decision; sampleFindings is sorted below.
	for _, detector := range detectors {
		spec := detector.Spec()
		if skipLearned && learnedIncidentDetector(spec) {
			continue
		}
		if result, cached := setupResults[spec.ID]; cached {
			if err := apply(detector, result); err != nil && dispatchErr == nil {
				dispatchErr = err
			}
		}
	}
	evaluateDetectorsEach(ctx, fresh, snapshot, func(detector sdk.Detector, result setupDetection) {
		if err := apply(detector, result); err != nil && dispatchErr == nil {
			dispatchErr = err
		}
	})
	if setupHealthy {
		c.mu.Lock()
		c.setupEvaluatedRevision = c.config.SourceCommit
		c.mu.Unlock()
	}
	if err := c.finishOnboarding(ctx, onboardingCandidate, sampleFindings, errored, now); err != nil {
		return err
	}
	if deferInventory && c.pendingOnboarding == nil {
		c.observeSourceInventory(now)
	}
	c.probeGate(ctx, now, snapshot, probes)
	c.forgetGate()
	c.settleGateProbes(now)
	if len(objectChanges) > 0 {
		c.resetEarlyStability()
	}
	if len(detectors) > 0 {
		sort.Slice(sampleFindings, func(left int, right int) bool {
			return sampleFindings[left].Fingerprint < sampleFindings[right].Fingerprint
		})
		if c.OnEvaluation != nil {
			c.OnEvaluation(sampleFindings)
		}
		healthy := !errored && c.pendingOnboarding == nil && (onboardingCandidate == nil || c.namespaceOnboarding != nil) && c.quiet() && c.completeHealthyEvaluation()
		if c.Baseline != nil {
			c.Baseline.Observe(now, healthy)
		}
		c.captureHealthyMemoryBaseline(ctx, now, snapshot, healthy)
		c.promotePodReadBaseline(now, snapshot, healthy)
		c.observeAppState(now, healthy)
		c.promoteObservationBaselines(now, snapshot, healthy)
		c.observeStartup(now)
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
	if err == nil {
		err = dispatchErr
	}
	return err
}

// slowDetectorInterval is the interval from which a detector counts as slow:
// the controller opens an incident without waiting for it.
const slowDetectorInterval = 30 * time.Second

// quiet reports that nothing is wrong or pending: no incident or closure, no
// active or confirming opener, and nothing waiting in the batcher. Clues do
// not count: a persistent clue must not stall the healthy baselines. Only a
// quiet state may become the configuration baseline.
func (c *Controller) quiet() bool {
	c.mu.Lock()
	busy := c.incidentOpen || c.closureBlocksNextLocked()
	c.mu.Unlock()
	_, batched := c.batcher.Deadline()
	return !busy && !batched && !c.openerFiring("")
}

// openerFiring reports whether the tracker holds an active or confirming
// opener, of detectorID or, when it is empty, of any detector.
func (c *Controller) openerFiring(detectorID string) bool {
	for _, state := range c.tracker.Snapshot() {
		if (state.Active || state.FiringCount > 0) && (detectorID == "" || state.DetectorID == detectorID) && c.opens(state.Finding) {
			return true
		}
	}
	return false
}

// EvaluateAll evaluates every detector against one fresh snapshot, for example
// when observation resumes after a planned maintenance window.
func (c *Controller) EvaluateAll(ctx context.Context, now time.Time) error {
	c.adoptionNeeded = c.drift != nil && c.drift.Source != nil && c.drift.Client != nil
	c.scheduler.ExpediteAll(now)
	return c.StepEvents(ctx, now, nil)
}

// admitActivation is the one entry for a newly active finding. A finding
// already in the open incident is refreshed; a clue joins the incident before
// launch, or as a learned playbook's activation after it, and otherwise stays
// tracked only. An opener joins the incident's opening alerts before launch
// and otherwise waits in the batcher for the next incident.
func (c *Controller) admitActivation(ctx context.Context, finding sdk.Finding, now time.Time, spec sdk.DetectorSpec) {
	if c.mergeIntoOpenIncident(finding) {
		return
	}
	if !c.opens(finding) {
		if !c.attachBeforeLaunch([]sdk.Finding{finding}, false) {
			c.attachLearnedActivation(ctx, finding)
		}
		return
	}
	// A later opener never joins a launched incident: it opens the next one.
	if c.attachBeforeLaunch([]sdk.Finding{finding}, true) {
		return
	}
	if spec.Class == "" {
		c.batcher.AddAt(finding, now)
		return
	}
	c.batcher.AddAtWithDebounce(finding, now, spec.Batching.Debounce)
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
// Openers also join the incident's opening alerts; clues never do.
func (c *Controller) attachBeforeLaunch(findings []sdk.Finding, opening bool) bool {
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
		key := FindingStateKey(finding.DetectorID, finding.Fingerprint)
		if opening && !slices.Contains(request.OpeningKeys, key) {
			request.OpeningKeys = append(request.OpeningKeys, key)
			sort.Strings(request.OpeningKeys)
		}
		if _, ok := existing[key]; ok {
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
	incidentOpen := c.incidentOpen || c.earlyReportPendingLocked()
	closurePending := c.closureBlocksNextLocked() && c.broker != nil
	c.mu.Unlock()
	if incidentOpen {
		c.mu.Lock()
		attachable := c.canAttachBeforeLaunchLocked()
		c.mu.Unlock()
		if attachable && c.batcher.Ready(now) {
			c.attachBeforeLaunch(c.batcher.DrainReady(now), true)
		}
		return nil
	}
	if closurePending {
		return nil
	}
	// A new install opens nothing until it is seen healthy (startupPeriod).
	if c.startupHolds(now) {
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
	// The batch holds only openers. The clues active now join as evidence.
	opening := findingKeys(batch)
	clues := c.activeClues(batch)
	batch = append(batch, clues...)
	sortFindings(batch)
	request := c.incidentRequest(now, batch)
	request.OpeningKeys = opening
	request.StandingKeys = findingKeys(clues)
	request.PassingTests = passingTests(snapshot)
	request.ObservationGeneration = observationGeneration(snapshot)
	if c.Baseline != nil {
		request.StateChanges = cloneStateChanges(c.Baseline.Changes(now))
	}
	request.StateChanges = withStoreChanges(request.StateChanges, snapshot, now)
	request.DetectedAt = now.UTC()
	if reads, ok := snapshot.(detectReads); ok {
		signals, appState := c.IncidentSources()
		request.RecentChanges = incidentCandidateBlock(incidentPageInput{Request: request, Findings: batch, View: reads, Signals: signals, AppState: appState, Now: now.UTC()})
	}
	c.beginIncident(now, request, batch)
	// The batcher is drained, so the incident is open before any file I/O: an
	// observer exporting state mid-capture reads the work, never an idle gap.
	// The responder starts only after this step returns, so it still finds the
	// capture in place.
	if c.config.CorpusSessionRoot != "" {
		if err := c.bindIncidentHealthyBaseline(request.IncidentID); err != nil {
			c.reportHealthyCaptureError(err)
		}
		c.captureOpenDetect(ctx, snapshot, request)
	}
	return nil
}

// beginIncident records intent before workspace/dispatch effects may run.
func (c *Controller) beginIncident(now time.Time, request IncidentRequest, batch []sdk.Finding) {
	c.mu.Lock()
	c.cachedVerification = nil
	c.cachedIncidentID = ""
	c.closureStableSince = time.Time{}
	c.closureMutationRevision = ""
	c.recoverySafetyLost = false
	c.earlyLastMutationAt = time.Time{}
	c.responderLoop = nil
	c.midStage = midIncidentStage{}
	c.incidentOpen = true
	c.setupEvaluatedRevision = c.config.SourceCommit
	c.responderDone = false
	request.DetectedAt = now.UTC()
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
	c.lateJoinedKeys = nil
	c.noteBatchedLocked(request.IncidentID, batch)
	c.pruneTimelineLocked(batch)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseOpened, RecordedAt: contractTimestamp(c.incidentDetectedAt), IncidentId: request.IncidentID,
		FindingKeys: append([]string(nil), c.incidentFindingKeys...), SurfacedPlaybooks: playbookIDs(request.SurfacedPlaybooks),
	})
	c.mu.Unlock()
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
	c.goBackground(func() {
		budget, cancel := context.WithTimeout(ctx, sourceComparisonCaptureBudget)
		comparisons := boundaryObservations(budget, snapshot, view)
		cancel()
		if err := memory.AppendCapturedRecordings(c.config.CorpusSessionRoot, request.IncidentID, "open@detect", comparisons); err != nil {
			report(fmt.Errorf("capture incident source comparisons: %w", err))
		}
		if err := captureRoutingEvidence(ctx, c.config.CorpusSessionRoot, request.IncidentID, "open@detect", request.Findings, view); err != nil {
			report(fmt.Errorf("capture incident routing evidence: %w", err))
		}
	})
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

// goBackground runs work on a goroutine that Wait joins. Every goroutine an
// effect starts goes through here, so no write outlives the caller.
func (c *Controller) goBackground(work func()) {
	c.background.Add(1)
	go func() {
		defer c.background.Done()
		work()
	}()
}

// Wait joins the controller's background goroutines. Their contexts derive
// from the caller's, so cancel that first; they write under the session root
// until they return.
func (c *Controller) Wait() {
	c.background.Wait()
}

func (c *Controller) NextWake() time.Time {
	at, _ := c.nextWake()
	return at
}

// nextWake is NextWake with the name of the item that set the time. The run
// loop reports the name when it makes no progress (see wakeSpinGuard).
func (c *Controller) nextWake() (time.Time, string) {
	var next time.Time
	why := ""
	consider := func(at time.Time, reason string) {
		if !at.IsZero() && (next.IsZero() || at.Before(next)) {
			next, why = at, reason
		}
	}
	consider(c.scheduler.NextRun(), "detector schedule")
	c.mu.Lock()
	if c.head.poller != nil && !c.head.running {
		consider(c.head.nextAt, "head poll")
	}
	// A batch deadline only matters when the batch can be dispatched or
	// attached. Otherwise an expired deadline would spin the runtime loop for
	// the whole incident.
	startupHolding := c.startup.holding(c.clock())
	batchActionable := !startupHolding && (!c.incidentOpen && (!c.closureBlocksNextLocked() || c.broker == nil)) ||
		c.canAttachBeforeLaunchLocked()
	c.mu.Unlock()
	if deadline, ok := c.batcher.Deadline(); ok && batchActionable {
		consider(deadline, "finding batch deadline")
	}
	// Only a bound still ahead: a past one would spin the loop.
	if startupHolding {
		consider(c.startup.bound, "start-up bound")
	}
	c.mu.Lock()
	verificationPending := c.incidentOpen && c.responderDone && c.escalatedAt.IsZero() && c.currentIncidentRequest != nil
	verificationDeadline := time.Time{}
	if verificationPending {
		verificationDeadline = c.recoveryGraceEndsLocked()
		if c.earlyStopRetryAt.After(verificationDeadline) {
			verificationDeadline = c.earlyStopRetryAt
		}
		// A running stability window outlives the deadline; its own wake
		// below covers it, and an expired deadline would spin the loop.
		if !verificationDeadline.After(c.clock()) && c.recoveryWindowRunningLocked(c.clock()) {
			verificationDeadline = time.Time{}
		}
	}
	earlyDeadline := time.Time{}
	earlyReason := "closure stability window"
	if c.incidentOpen && !c.closureStableSince.IsZero() {
		earlyDeadline = c.closureStableSince.Add(c.config.ClosureStabilityWindow)
	}
	if c.earlyReportPendingLocked() {
		earlyDeadline = c.earlyReportWakeLocked()
		earlyReason = "early report"
	}
	externalCompleted := c.config.ExternalIncidentsOnly && c.incidentOpen && c.responderDone
	retireDeadline := c.retirementWakeLocked(c.clock())
	c.mu.Unlock()
	if externalCompleted {
		consider(c.clock(), "external responder completion")
	}
	consider(retireDeadline, "incident retirement")
	consider(earlyDeadline, earlyReason)
	if verificationPending {
		consider(verificationDeadline, "verification deadline")
	}
	consider(c.nextGateProbe(), "gate probe")
	if retry := c.dispatchRetryWake(next); !retry.Equal(next) {
		next, why = retry, "dispatch retry"
	}
	if retry := c.closureRetryWake(next); !retry.Equal(next) {
		next, why = retry, "closure retry"
	}
	return next, why
}

func (c *Controller) IncidentOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.incidentOpen
}

// recordEvaluation records one detector evaluation. Only active findings the
// recovery scope holds make it firing; the tracker still follows the others.
func (c *Controller) recordEvaluation(detectorID string, now time.Time, findings []sdk.Finding, scope recoveryScope) {
	status := DetectorEvaluationClear
	fingerprints := make([]string, 0, len(findings))
	for _, finding := range scope.blocking(findings) {
		status = DetectorEvaluationFiring
		fingerprints = append(fingerprints, finding.Fingerprint)
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
	return IncidentRequest{Telemetry: c.config.Telemetry,
		SchemaVersion: ProtocolSchemaVersion, Application: c.config.Application, Namespace: c.config.Namespace,
		IncidentID: incidentID, Findings: findings,
		PriorResponseProgress:  priorResponseProgress(c.config.RepositoryWorktree, findings),
		DriftEvents:            driftEvents,
		NamespaceOnboarding:    cloneNamespaceOnboarding(c.namespaceOnboarding),
		DisabledSetupDetectors: cloneDisabledSetupDetectors(c.disabledSetupDetectors),
		DetectorHistory:        compactDetectorHistory(c.history), SurfacedPlaybooks: surfacedPlaybooks(findings),
		RelevantOutcomes: relevantOutcomeEvidence(
			c.config.RepositoryWorktree, findings, c.config.SourceCommit, c.detectorOrigins(),
		),
		SourceCommit: c.config.SourceCommit, DeployedCommit: c.config.DeployedCommit,
		DeploymentInFlight: c.deploymentInFlight(),
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
			strings.Join(evaluation.Fingerprints, "\x00") + "\x00" + evaluation.Error + "\x00" + evaluation.Reason
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
	c.responseStagesRunning = false
	if len(completion.priorStages) > 0 {
		c.currentIncidentRequest.PriorStages = mergeLaterStagePasses(completion.priorStages, c.currentIncidentRequest.PriorStages)
	}
	if c.earlyReportPendingLocked() {
		if completion.err == nil {
			snapshot := *c.pendingClosure.Result
			// The early snapshot owns controller evidence; the final report owns
			// diagnosis text. Reconcile resource owners before receipt/usage merging.
			reported := completion.result
			reported.ConfirmedRootCauses = reconcileEarlyCauses(snapshot.ConfirmedRootCauses, reported.ConfirmedRootCauses)
			snapshot.ConfirmedRootCauses = nil
			prior, _ := incidentResultContract(snapshot)
			result := accumulateResponderResult(prior, reported)
			if result.ResponderSessionID == "" {
				result.ResponderSessionID = c.pendingClosure.Result.ResponderSessionID
			}
			c.pendingClosure.Result = cloneIncidentResult(&result)
			c.pendingClosure.ResponderCompletedAt = observedAt.UTC()
		}
		var terminal *ResponderJobFailedError
		if completion.err == nil || errors.As(completion.err, &terminal) {
			c.responderDone = true
			c.emitLifecycleLocked(&contracts.LifecycleEvent{Event: PhaseResponderCompleted, RecordedAt: contractTimestamp(observedAt), IncidentId: completion.incidentID, DispatchError: completion.err != nil})
		}
		c.mu.Unlock()
		return
	}
	var jobFailed *ResponderJobFailedError
	var timedOut *ResponseTimeoutError
	if errors.As(completion.err, &timedOut) {
		// The response timeout is a safety stop, not a failure: the agent never
		// reached Recovered, so the incident escalates and stays open.
		c.responderDone = true
		c.responderCompletedAt = observedAt.UTC()
		if c.responderCompletedAt.Before(c.incidentDispatchedAt) {
			c.responderCompletedAt = c.incidentDispatchedAt
		}
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
		c.dispatchError = ""
		c.currentIncidentResult = &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: completion.incidentID, Status: IncidentCompleted, ResponderSessionID: completion.result.ResponderSessionID, Timing: TimingMetrics{StartedAt: c.incidentDispatchedAt, CompletedAt: c.responderCompletedAt}}
		c.escalateLocked(c.responderCompletedAt)
		c.resetDispatchRetryLocked()
	} else if errors.As(completion.err, &jobFailed) {
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
	if c.config.ExternalIncidentsOnly {
		c.closeExternalIncident(now)
		return
	}
	c.mu.Lock()
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		c.mu.Unlock()
		return
	}
	early := !c.responderDone
	requiresStability := early || c.codePath != nil && c.codePath.Selection.DetectorSource == "shell"
	if early && c.responseStagesRunning || !early && c.closurePreparingLocked() {
		c.mu.Unlock()
		return
	}
	if early && (c.dispatchState != contracts.DispatchState_DISPATCH_STATE_RUNNING || c.incidentDispatchedAt.IsZero()) {
		c.mu.Unlock()
		return
	}
	loop := c.responderLoop
	c.mu.Unlock()
	var guard *apihistory.MutationObservation
	release := func() {}
	safe := true
	var closureGuard *responderClosureGuard
	if early {
		closureGuard, release, safe = acquireClosureGuard(loop)
		if safe {
			guard = closureGuard.observation
		}
	} else if loop != nil {
		var err error
		guard, err = loop.observeMutations()
		if err != nil {
			return
		}
		if guard != nil {
			release = func() { guard.Close() }
		}
	}
	defer release()
	var snapshotErr error
	if snapshot == nil {
		snapshot, snapshotErr = c.provider.Snapshot(ctx)
		if snapshotErr != nil {
			c.resetEarlyStability()
			c.reportBrokerError("verify observation scope", snapshotErr)
			return
		}
	}
	if snapshotErr == nil {
		c.observeRecoveryHealth(ctx, now, snapshot, *c.currentIncidentRequest)
	}
	generation := observationGeneration(snapshot)
	var rolloutRefs []sdk.ObjectRef
	var observedResult *IncidentResult
	if early && safe && loop != nil {
		// acquireClosureGuard holds the responder locks and loads controller-owned
		// external action receipts, including writes before the final worker report.
		c.mu.Lock()
		request := cloneIncidentRequest(c.currentIncidentRequest)
		c.mu.Unlock()
		if request != nil {
			observedResult = closureGuard.resultSnapshot(*request, now)
			for _, action := range observedResult.RepairActions {
				rolloutRefs = append(rolloutRefs, action.Resources...)
			}
		}
	}
	source := closureSourceReport{}
	recovery := recoveryCheck{verdict: recoveryVerified}
	if snapshotErr == nil {
		source = c.verifyClosureSource(ctx, now, snapshot, observedResult, rolloutRefs...)
		c.mu.Lock()
		request, result := cloneIncidentRequest(c.currentIncidentRequest), observedResult
		if result == nil {
			result = cloneIncidentResult(c.currentIncidentResult)
		}
		detected, stability := c.incidentDetectedAt, c.config.ClosureStabilityWindow
		c.mu.Unlock()
		recovery = c.recoveryCheck(ctx, snapshot, *request, result, detected, stability)
	}
	c.mu.Lock()
	// Any observed responder write restarts the stability window, including
	// the last writes before the responder finished.
	if safe && guard != nil && c.closureMutationRevision != guard.Revision {
		c.closureMutationRevision = guard.Revision
		c.closureStableSince = time.Time{}
		c.earlyLastMutationAt = now
		repairs := observedResult
		if repairs == nil {
			repairs = c.currentIncidentResult
		}
		if at, ok := lastRepairCompletion(repairs); ok && !at.After(now) && !at.Before(c.incidentDispatchedAt) {
			c.earlyLastMutationAt = at
		}
	}
	finalStates, healthVerified := c.finalVerificationStates()
	if snapshotErr != nil {
		finalStates, healthVerified = nil, false
	}
	if early && healthVerified && !c.earlyLastMutationAt.IsZero() {
		ids := c.recoveryDetectorIDs(*c.currentIncidentRequest)
		finalStates, healthVerified = c.latestEvaluations(ids, c.earlyLastMutationAt, true)
	}
	verified := healthVerified && len(source.Rollouts) == 0 && len(source.Effects) == 0
	if !safe || guard != nil && guard.Pending {
		c.recoverySafetyLost = true
	}
	// A terminal dispatch failure closes as failed: once health permits it, or
	// at once when the responder is done, because a failed Job leaves no
	// attempt to verify and waiting would only hold still-firing findings back
	// from the next attempt.
	jobFailed := !early && c.dispatchError != ""
	terminalFailure := jobFailed || (healthVerified && c.dispatchError != "")
	if jobFailed && !healthVerified {
		slog.Info("incident_terminal_dispatch_failure", "incident_id", c.currentIncidentRequest.IncidentID, "reason", "responder_job_failed", "active_health_detectors", c.activeHealthDetectors())
	}
	verification := c.continuationVerificationLocked(healthVerified, source)
	if snapshotErr != nil {
		verification.Error = "verification snapshot unavailable: " + snapshotErr.Error()
		verification.Summary += "; " + verification.Error
		for _, state := range verification.DetectorStates {
			verification.VerificationBlockers = append(verification.VerificationBlockers, VerificationBlocker{DetectorID: state.DetectorID, Reason: verification.Error})
		}
	}
	if early {
		seen := map[string]bool{}
		for _, finding := range verification.Findings {
			seen[FindingFingerprint(finding)] = true
		}
		scope := c.recoveryScopeLocked()
		for _, state := range c.tracker.Snapshot() {
			if state.Active && scope.holds(state.Finding) && !strings.HasPrefix(state.DetectorID, "drift.") && !seen[FindingFingerprint(state.Finding)] {
				verification.Findings = append(verification.Findings, state.Finding)
			}
		}
		sortFindings(verification.Findings)
		if len(verification.Findings) > 0 {
			verification.Healthy = false
			verified = false
		}
	}
	verification.Healthy = false // Only the shared recovery proof can grant health.
	c.cacheVerificationLocked(verification)
	if verified && !terminalFailure {
		requiresStability = true
		verified = c.recoveryProofLocked(now, finalStates, safe && (guard == nil || !guard.Pending))
		if verified && recovery.verdict == recoveryPending {
			// Findings clearing is not Recovered: the opening user tests must
			// pass again after SDO's last write settled. The stability window
			// restarts, so the next check has a future wake.
			verified = false
			c.closureStableSince = now
			verification.Summary = recovery.reason
		} else if !verified {
			verification.Summary = recoveryStabilitySummary
		}
		verification.Healthy = verified
		c.cacheVerificationLocked(verification)
	} else {
		c.closureStableSince = time.Time{}
		verification.Healthy = false
		c.cacheVerificationLocked(verification)
		if early {
			c.mu.Unlock()
			return
		}
	}

	if !verified && !terminalFailure {
		if early {
			c.mu.Unlock()
			return
		}
		stageOnly := stageOnlyClosure(IncidentClosure{Request: *c.currentIncidentRequest, Result: c.currentIncidentResult})
		graceOver := !now.Before(c.recoveryGraceEndsLocked())
		switch {
		case stageOnly && !stageEscalatedIn(c.currentIncidentRequest.PriorStages):
			// An automatic stage has no provider session to resume. When its
			// recovery wait ended with the opening alerts still firing, the
			// first agent task takes over through the stage hand-off.
			if now.Before(c.currentIncidentRequest.ResponseDeadline) &&
				stageHandoffWarranted(c.config.AfterPlaybook, c.currentIncidentRequest.PriorStages, c.recoveryScopeLocked(), verification.Findings, graceOver,
					c.tracker.HasActiveKeys(openingKeys(c.currentIncidentRequest)), len(c.activeHealthDetectors()) > 0) {
				request := *cloneIncidentRequest(c.currentIncidentRequest)
				request.Findings = append([]sdk.Finding(nil), verification.Findings...)
				if len(request.Findings) == 0 {
					for _, state := range c.tracker.Snapshot() {
						if state.Active {
							request.Findings = append(request.Findings, state.Finding)
						}
					}
				}
				if playbookApplied(request.PriorStages) {
					// After an applied playbook the agent receives the opening alerts only.
					request.Findings = holdingFindings(c.recoveryScopeLocked(), request.Findings)
				}
				c.mu.Unlock()
				_, err := c.prepareResponseStageHandoff(ctx, request)
				if err != nil {
					c.reportBrokerError("handoff unrecovered automatic stage", err)
					return
				}
				c.mu.Lock()
				c.responderDone = false
				c.responderCompletedAt = time.Time{}
				c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
				c.resetDispatchRetryLocked()
				c.mu.Unlock()
				return
			}
			// The policy withholds the agent after an applied playbook: past
			// the grace, the incident escalates without one.
			if !graceOver || !playbookApplied(c.currentIncidentRequest.PriorStages) || agentMayFollowPlaybook(c.config.AfterPlaybook, c.currentIncidentRequest.PriorStages) {
				c.mu.Unlock()
				return
			}
		case !stageOnly && !graceOver:
			// Recovered may still hold within the grace after the agent returned.
			c.mu.Unlock()
			return
		}
		// The responder returned (or its stage escalated) before Recovered held:
		// the incident escalates. No agent runs again for these symptoms; it
		// stays open until its opening alerts clear or `incidents reset`.
		c.escalateLocked(now)
		c.mu.Unlock()
		return
	}
	if !verified {
		finalStates = nil
	}
	verifiedAt := now.UTC()
	if verifiedAt.Before(c.responderCompletedAt) {
		verifiedAt = c.responderCompletedAt
	}
	var evidence *closureEvidence
	if verified {
		input := closureEvidenceInput{
			IncidentID: c.currentIncidentRequest.IncidentID, Findings: append([]sdk.Finding(nil), c.currentIncidentRequest.Findings...),
			Snapshot: snapshot, Loop: loop, VerifiedAt: verifiedAt, ResponderCompletedAt: c.responderCompletedAt,
		}
		if !early && c.currentIncidentResult != nil {
			input.Result, input.Stages = cloneIncidentResult(c.currentIncidentResult), cloneIncidentRequest(c.currentIncidentRequest).PriorStages
		}
		if early {
			// The closure guard holds the responder's locks, so the capture stays
			// on this pass; settlement joins it below under the same guard.
			c.mu.Unlock()
			err := c.captureClosureEvidence(ctx, input)
			c.mu.Lock()
			if err != nil {
				c.mu.Unlock()
				c.reportBrokerError("capture closure evidence", err)
				return
			}
			if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != input.IncidentID {
				c.mu.Unlock()
				return
			}
		} else if c.closureEvidenceNeeded(input) {
			var ready bool
			if evidence, ready = c.closureEvidenceLocked(ctx, input); !ready {
				c.mu.Unlock()
				return
			}
		}
	}
	if verified && guard != nil {
		if err := guard.Fence(now); err != nil {
			c.mu.Unlock()
			c.reportBrokerError("fence verified responder", err)
			return
		}
	}
	finalChanges := c.finalStateChangesLocked(now)
	var healthClearedAt *time.Time
	if verified {
		healthClearedAt = c.healthClearedAtLocked(finalStates)
		logIncidentVerified(c.currentIncidentRequest.IncidentID, healthClearedAt, c.responderCompletedAt, verifiedAt)
	}
	windowUsed := time.Duration(0)
	if requiresStability {
		windowUsed = c.config.ClosureStabilityWindow
	}
	closure := IncidentClosure{
		EscalatedAt:                   c.escalatedAt,
		VerifiedWhileResponderRunning: early,
		ClosureStabilityWindow:        windowUsed,
		ObservationGeneration:         generation,
		Request:                       *cloneIncidentRequest(c.currentIncidentRequest), Result: cloneIncidentResult(c.currentIncidentResult),
		DispatchError: c.dispatchError, FinalDetectorStates: finalStates,
		IncidentDetectorStates: c.incidentDetectorStates(),
		PartialDetectorStates:  c.reflectionDetectorStates(verified),
		FinalStateChanges:      finalChanges,
		HealthClearedAt:        healthClearedAt,
		DetectedAt:             c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: c.responderCompletedAt, VerifiedAt: verifiedAt,
		CleanedHelpers: append([]string(nil), c.cleanedHelpers...),
	}
	if !verified && !terminalFailure && len(verification.Findings) == 0 {
		closure.UnconfirmedReason = contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE
		if verification.Error != "" {
			closure.UnconfirmedReason = contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVER_UNAVAILABLE
		}
		closure.UnconfirmedDetail = verification.Summary
		// Pending/error states are outcome evidence, not independently cleared
		// causes. Preserve every named blocker through the memory edge.
		for _, blocker := range verification.VerificationBlockers {
			closure.PartialDetectorStates = append(closure.PartialDetectorStates, DetectorEvaluation{DetectorID: blocker.DetectorID, EvaluatedAt: now, Status: DetectorEvaluationPending, Reason: blocker.Reason})
		}
	}
	if early {
		closure.ResponderCompletedAt = verifiedAt
		closure.Result = closureGuard.resultSnapshot(*c.currentIncidentRequest, verifiedAt)
		if len(closure.Request.PriorStages) > 0 {
			merged, mergeErr := responsePipelineResult(&IncidentState{Request: closure.Request, PriorStages: closure.Request.PriorStages, Result: closure.Result}, closure.DispatchedAt, verifiedAt)
			if mergeErr != nil {
				c.mu.Unlock()
				c.reportBrokerError("stage closure evidence", mergeErr)
				return
			}
			closure.Result = &merged
		}
	}
	if early && verified && loop != nil && closure.Result != nil {
		c.mu.Unlock()
		*closure.Result, closure.Request.PriorStages = loop.settleRecoveryApplicationsLocked(ctx, *closure.Result, closure.Request.PriorStages, verifiedAt)
		c.mu.Lock()
	}
	if evidence != nil && evidence.Settled && closure.Result != nil {
		*closure.Result, closure.Request.PriorStages = evidence.Result, evidence.Stages
	}
	if verified {
		// The worker's last self-check precedes the controller's recovery proof
		// (and often a rollout settling). Freeze the independently observed
		// disposition after settlement, retaining the worker report as evidence.
		// Dispatch failures never acquire this authority.
		if closure.DispatchError == "" && closure.Result != nil {
			if closure.Result.Status == IncidentFailed {
				closure.Result.VerificationEvidence = append(closure.Result.VerificationEvidence, VerificationEvidence{
					Name: "responder_report_before_recovery", ObservedAt: closure.ResponderCompletedAt,
					Details: fmt.Sprintf("status=%s; unresolved_findings=%d; error=%s", closure.Result.Status, len(closure.Result.UnresolvedFindings), closure.Result.Error),
				})
			}
			closure.Result.Status = IncidentCompleted
			closure.Result.UnresolvedFindings = nil
		}
		c.retainSourceInventoryAcceptancesLocked(c.currentIncidentResult)
	}
	closure.DetectorTimeline = cloneTimeline(c.timeline)
	sortTimeline(closure.DetectorTimeline)
	closure.IncidentDetectorFiredBeforeDispatch, closure.IncidentDetectorFiredAfterDispatch,
		closure.NoIncidentDetectorFired = timelineSummary(closure.DetectorTimeline)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseClosed, RecordedAt: contractTimestamp(verifiedAt), IncidentId: closure.Request.IncidentID,
		HealthClearedAt:               optionalContractTimestamp(closure.HealthClearedAt),
		DispatchError:                 closure.DispatchError != "",
		VerifiedWhileResponderRunning: early,
		ClosureStabilityWindowSeconds: windowUsed.Seconds(),
		AfterDispatchFindingKeys:      afterDispatchKeys(closure.DetectorTimeline),
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
	if early {
		c.incidentOpen = false
		warning := apihistory.VerificationWarning(verifiedAt)
		verification.IncidentVerified = warning
		c.cacheVerificationLocked(verification)
	} else {
		c.clearIncidentLocked()
	}
	c.mu.Unlock()
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
}

// recoveryGraceEndsLocked is how long Recovered may still hold after a responder
// returned: past it, an unrecovered incident escalates (and an unrecovered
// automatic stage hands its alerts to the first agent task). It never closes an
// incident.
func (c *Controller) recoveryGraceEndsLocked() time.Time {
	bound := c.responderCompletedAt.Add(c.config.VerificationTimeout)
	if response := c.currentIncidentRequest.ResponseDeadline; !response.IsZero() && response.Before(bound) {
		bound = response
	}
	return bound
}

// escalateLocked records that the incident's responder ended before Recovered.
// The first escalation time is kept; a restart rejoins it.
func (c *Controller) escalateLocked(at time.Time) {
	if c.escalatedAt.IsZero() {
		c.escalatedAt = at.UTC()
		slog.Warn("incident_escalated", "incident_id", c.currentIncidentRequest.IncidentID, "active_health_detectors", c.activeHealthDetectors())
	}
}

// activeHealthDetectors lists health detectors with an active finding the
// open incident's recovery scope holds. Caller holds c.mu.
func (c *Controller) activeHealthDetectors() []string {
	active := []string{}
	for _, finding := range c.recoveryScopeLocked().blocking(c.tracker.ActiveFindings(c.healthDetectorIDs, nil)) {
		if !slices.Contains(active, finding.DetectorID) {
			active = append(active, finding.DetectorID)
		}
	}
	sort.Strings(active)
	return active
}

// logIncidentVerified records the verification wait: how long after the
// responder completed the health detectors cleared and the incident verified.
func logIncidentVerified(incidentID string, healthClearedAt *time.Time, responderCompletedAt, verifiedAt time.Time) {
	if responderCompletedAt.IsZero() {
		attrs := []any{"incident_id", incidentID, "verified_at", verifiedAt, "verified_while_responder_running", true}
		if healthClearedAt != nil {
			attrs = append(attrs, "health_cleared_at", *healthClearedAt)
		}
		slog.Info("incident_verified", attrs...)
		return
	}
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
// holds the closure for pending rollouts found by verifyClosureSource; drift is
// reported there but never decides.
func (c *Controller) finalVerificationStates() ([]DetectorEvaluation, bool) {
	request := IncidentRequest{}
	if c.currentIncidentRequest != nil {
		request = *c.currentIncidentRequest
	}
	ids := c.recoveryDetectorIDs(request)
	if !c.recoveryTrackersClear(request) {
		return nil, false
	}
	return c.latestEvaluations(ids, c.recoveryObservationEpochLocked(), true)
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
		DisabledSetupDetectors: cloneDisabledSetupDetectors(c.disabledSetupDetectors), SetupEvaluatedRevision: c.setupEvaluatedRevision,
		Version: RuntimeStateVersion, SchedulerDeadlines: timestampsContract(c.scheduler.Deadlines()), FindingStates: findingStates,
		PendingBatch: &contracts.BatcherState{
			Findings: batch, Deadlines: timestampsContract(pendingBatch.Deadlines), Deadline: optionalTimestamp(pendingBatch.Deadline),
		},
		History: evaluationsContract(history), IncidentOpen: c.incidentOpen, ResponderDone: c.responderDone,
		DispatchError: c.dispatchError, IncidentDetectedAt: optionalTimestamp(c.incidentDetectedAt),
		IncidentDispatchedAt: optionalTimestamp(c.incidentDispatchedAt), ResponderCompletedAt: optionalTimestamp(c.responderCompletedAt),
		EscalatedAt:         optionalTimestamp(c.escalatedAt),
		DetectorClearSince:  timestampsContract(c.detectorClearSince),
		DispatchState:       dispatchState,
		IncidentFindingKeys: append([]string(nil), c.incidentFindingKeys...), ClosureState: c.closureState,
		ClosureReceipt: closureReceiptContract(c.closureReceipt), ClosureFailure: closureFailureContract(c.closureFailure), FailedClosures: cloneFailedClosures(c.failedClosures),
		LastAcknowledgedIncidentId: c.lastAcknowledgedIncidentID, LastAcknowledgedExternalIncidentId: c.lastAcknowledgedExternalIncidentID, QueuedReflectionIncidentIds: append([]string(nil), c.reflections.queued...), QueuedReflectionsUnknown: c.reflections.unknown,
		EvaluationIteration: int32(c.evaluationIteration), StepCount: c.stepCount, DetectorTimeline: timeline, IncidentView: view,
		NamespaceOnboarding: cloneNamespaceOnboarding(c.namespaceOnboarding),
		AcceptedSourceDrift: driftEventsContract(c.acceptedSourceDrift), AcceptedSourceDriftRevision: c.acceptedSourceDriftRevision,
		DriftEvents: driftEventsContract(c.driftEvents), OwnershipAdoption: adoptionContract(c.ownershipAdoption),
		BusyUntil: optionalBusyUntil(c.busyUntilLocked(c.clock())),
		Startup:   c.startup.contract(),
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
	for _, request := range []*contracts.IncidentRequest{state.GetIncidentRequest(), state.GetPendingClosure().GetRequest()} {
		if request != nil && (request.GetExternalMessage() != "") != c.config.ExternalIncidentsOnly {
			return fmt.Errorf("incident admission policy differs from persisted request")
		}
	}
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
	c.scheduler.RestoreDeadlines(timestampsState(state.GetSchedulerDeadlines()))
	// The state was written under an earlier memory revision. Findings of a
	// detector the active revision no longer has (including retired proactive
	// playbooks) are dropped: nothing would ever evaluate or clear them, so
	// they would hold the controller non-quiet and open incidents no detector
	// can verify.
	c.tracker.Restore(findingStatesState(state.GetFindingStates()))
	c.tracker.Retain(c.knownDetector)
	c.batcher.Restore(BatcherState{Findings: findingsState(state.GetPendingBatch().GetFindings()), Deadlines: timestampsState(state.GetPendingBatch().GetDeadlines()), Deadline: controllerTime(state.GetPendingBatch().GetDeadline())})
	c.batcher.Retain(c.knownDetector)
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
	c.disabledSetupDetectors = cloneDisabledSetupDetectors(state.GetDisabledSetupDetectors())
	c.setupEvaluatedRevision = state.GetSetupEvaluatedRevision()
	c.sourceInventory = nil
	c.dispatchError = state.DispatchError
	c.incidentDetectedAt = controllerTime(state.GetIncidentDetectedAt())
	c.incidentDispatchedAt = controllerTime(state.GetIncidentDispatchedAt())
	c.responderCompletedAt = controllerTime(state.GetResponderCompletedAt())
	c.escalatedAt = controllerTime(state.GetEscalatedAt())
	c.detectorClearSince = cloneClearSince(timestampsState(state.GetDetectorClearSince()))
	c.recoveryObserverStarted = c.startedAt
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
	c.lastAcknowledgedExternalIncidentID = state.GetLastAcknowledgedExternalIncidentId()
	c.evaluationIteration = int(state.GetEvaluationIteration())
	c.stepCount = state.GetStepCount()
	c.timeline = cloneTimeline(detectorTimelineState(state.GetDetectorTimeline()))
	c.lateJoinedKeys = lateJoinedFromTimeline(c.timeline)
	if c.startup != nil {
		c.startup = restoredStartupPeriod(state.GetStartup())
	}
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
	if native, ok := broker.(*NativeIncidentBroker); ok && native.SourceGate != nil {
		native.SourceGate.Set(c.sourceDeployBusy)
		c.head = headPollState{poller: native.headPoller()}
	}
	return nil
}

// sourceDeployBusy is the work a source deploy after a closure must not
// overlap: an open incident, or another closure still in flight. The queued
// reflection that carries the publication is not a reason; it is the deploy.
func (c *Controller) sourceDeployBusy() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.incidentOpen:
		return "an incident is open"
	case c.closureBlocksNextLocked():
		return "a closure is in flight"
	}
	return ""
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
	c.checkpoint = nil
	if revision != "" {
		c.durable = proto.Clone(state).(*contracts.ControllerState)
		c.checkpoint = proto.Clone(state).(*contracts.ControllerState)
	}
	c.mu.Unlock()
	return nil
}

func (c *Controller) PersistState(ctx context.Context) error {
	c.persistenceMu.Lock()
	defer c.persistenceMu.Unlock()
	if c.stateStore == nil {
		return nil
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	queued := c.queuedLifecycle()
	state, err := c.ExportState()
	if err == nil {
		err = validateRuntimeState(state)
	}
	if err != nil {
		state, err = c.recoverInvalidState(err)
		if err != nil {
			return fmt.Errorf("snapshot controller state: %w", err)
		}
	}
	if preparer, ok := c.dispatcher.(interface {
		PrepareDispatch(context.Context, IncidentRequest) (IncidentRequest, error)
	}); ok && state.DispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING && state.IncidentRequest != nil && state.IncidentRequest.PreparedAgentTask == "" && !c.automaticResponsePending(state.IncidentRequest) {
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
	c.releaseLifecycle(queued)
	c.releaseStaleStagedTask(stagedIncident)
	c.mu.Lock()
	c.durable = proto.Clone(state).(*contracts.ControllerState)
	c.checkpoint = proto.Clone(state).(*contracts.ControllerState)
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

// deploymentInFlight reports whether an accepted-source rollout has not yet
// converged: a queued reflection awaiting its rollout, or a committed closure
// whose rollout the broker still holds pending. An unreadable ledger counts as
// in flight, so the source comparison is never trusted on a guess.
func (c *Controller) deploymentInFlight() bool {
	c.mu.Lock()
	broker := c.broker
	receipt := c.closureReceipt
	committed := c.closureState == contracts.ClosureState_CLOSURE_STATE_COMMITTED
	queued := append([]string(nil), c.reflections.queued...)
	inFlight := c.reflections.rollout != "" || c.reflections.unknown
	c.mu.Unlock()
	if inFlight || broker == nil {
		return inFlight
	}
	if rolloutBroker, ok := broker.(rolloutPendingBroker); ok && committed && receipt != nil {
		if pending, err := rolloutBroker.RolloutPending(*receipt); err != nil || pending {
			return true
		}
	}
	if queue, ok := broker.(ReflectionQueue); ok {
		for _, incident := range queued {
			if pending, err := queue.QueuedReflectionRolloutPending(incident); err != nil || pending {
				return true
			}
		}
	}
	return false
}

// passingTests lists the user tests every health-probe window judges healthy,
// sorted: what still works when the incident opens.
func passingTests(snapshot sdk.DetectionContext) []string {
	set, ok := snapshot.(traffic.WindowSet)
	if !ok {
		return nil
	}
	var passing []string
	for _, window := range set.TrafficWindows() {
		for _, verdict := range traffic.Judge(window) {
			if verdict.Evaluated && verdict.Healthy {
				passing = append(passing, verdict.ScenarioID)
			}
		}
	}
	sort.Strings(passing)
	return slices.Compact(passing)
}

// IncidentSources are what `sdo changes`, `sdo timeline` and the inline
// changes read besides the snapshot: the signal ring and the app-state source.
func (c *Controller) IncidentSources() (*incidentSignals, AppStateChangeSource) {
	return c.signals, c.config.AppStateChanges
}
