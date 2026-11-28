package runtime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sdo.dev/operator/provenance"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	kclock "k8s.io/utils/clock"

	"sdo.dev/operator/memory/layout"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const DefaultMaxResponseLoops = 10
const maxRecordedToolResponse = 2048

// maxRecordedApplyResponse bounds an apply record that lists the runs of the
// applied plans. The agent records at most MaxBatches (5) applies; the automatic
// stage adds one apply per bound plan, never retries a binding it already
// attempted, and verifies its batch outside the apply records, so its records
// carry no runs. That is a handful of applies at 8 KiB (tens of KiB); the other tool calls are
// 2 KiB each, well under 100 of them (200 KiB). Together they stay far inside
// the 768 KiB state ConfigMap budget. A compact run is about 250 B, so 8 KiB holds about 30 runs,
// several per applied plan.
const maxRecordedApplyResponse = 8192

type ToolRequest struct {
	Workload            string                            `json:"workload,omitempty"`
	Scenarios           []string                          `json:"scenarios,omitempty"`
	SelectionSet        *contracts.MitigationSelectionSet `json:"-"`
	WaitSeconds         float64                           `json:"wait_seconds,omitempty"`
	PreviousFindings    []sdk.Finding                     `json:"previous_findings,omitempty"`
	FindingFingerprints []string                          `json:"finding_fingerprints,omitempty"`
	DetectorIDs         []string                          `json:"detector_ids,omitempty"`
	Finding             string                            `json:"finding,omitempty"`
	// MapView is the `map` subcommand ("", path, diff or near); Name is the
	// operation or component it names, or what `detect` runs or shows: a user
	// test, a detector or an item of the summary.
	MapView string `json:"map_view,omitempty"`
	Name    string `json:"name,omitempty"`
	// Since is how far before onset `changes` and `timeline` reach, a Go
	// duration such as 30m; Limit keeps the newest N changes.
	Since string `json:"since,omitempty"`
	// Components keeps only the rows and columns of these map components in
	// `changes` and `timeline`.
	Components []string `json:"components,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	// Query, Step, Grep, Failing, List, Operation and Route are the inputs of
	// `metrics`, `logs` and `traces`; Name is the component, match or trace ID.
	Query     string `json:"query,omitempty"`
	Step      string `json:"step,omitempty"`
	Grep      string `json:"grep,omitempty"`
	Failing   bool   `json:"failing,omitempty"`
	List      bool   `json:"list,omitempty"`
	Operation string `json:"operation,omitempty"`
	// Structured asks `map` for its answer as JSON in ToolResponse.Data.
	Structured  bool                                   `json:"structured,omitempty"`
	Route       string                                 `json:"route,omitempty"`
	Parameters  map[string]string                      `json:"parameters,omitempty"`
	Selection   *contracts.MitigationSelection         `json:"-"`
	ReportQuery *contracts.MitigationReportQuery       `json:"-"`
	Checkpoint  *contracts.MitigationSessionCheckpoint `json:"-"`
	Command     string                                 `json:"command"`
	Ref         *sdk.ObjectRef                         `json:"ref,omitempty"`
	ProgramID   string                                 `json:"program_id,omitempty"`
	// Targets are the playbooks or built-in actions a mitigate call applies,
	// named as the responder typed them. Go resolves each to its exact selection.
	Targets       []MitigateTarget `json:"targets,omitempty"`
	Rounds        uint32           `json:"rounds,omitempty"`
	Bindings      program.Bindings `json:"bindings,omitempty"`
	PlanIDs       []string         `json:"plan_ids,omitempty"`
	ApplicationID string           `json:"application_id,omitempty"`
	// stage marks a call made by the automatic response stage. It is not
	// decodable from a tool call, so only the stage can set it. A stage call
	// never takes the notes queued for the agent and waits for rollouts inline.
	stage        bool
	isolatedRead bool
	// noSend marks a `detect --wait` poll after the first read: a user test
	// was sent once and the wait reads the scheduled requests.
	noSend bool
	// rolloutTimeout bounds a stage apply's rollout wait; it is not decodable
	// either, and an agent call keeps the executor's own timeout.
	rolloutTimeout time.Duration
	// batchVerify marks a stage apply that is one of a turn's batch: it skips
	// the per-apply verify, and the stage's "stage-verify" call runs one for
	// the whole turn's batchApplications.
	batchVerify       bool
	batchApplications []string
}

// VerificationBlocker names observation work that cannot yet establish health.
type VerificationBlocker struct {
	DetectorID string `json:"detector_id"`
	Reason     string `json:"reason"`
}

type VerificationStatus string

const VerificationAwaitingResponderCompletion VerificationStatus = "awaiting_responder_completion"

type ToolResponse struct {
	Text string `json:"text,omitempty"`
	// Data is a structured answer (`map` with Structured), never prose in JSON.
	Data                json.RawMessage     `json:"data,omitempty"`
	Burst               *prober.BurstResult `json:"burst,omitempty"`
	VerificationStatus  VerificationStatus  `json:"verification_status,omitempty"`
	MemoryCheckReport   json.RawMessage     `json:"memory_check_report,omitempty"`
	MemoryCheckTimedOut bool                `json:"memory_check_timed_out,omitempty"`

	VerificationBlockers []VerificationBlocker `json:"verification_blockers,omitempty"`
	DataEffects          []string              `json:"data_effects,omitempty"`
	ActionReports        []PlanActionReport    `json:"action_reports,omitempty"`
	ProgramRevisions     map[string]string     `json:"program_revisions,omitempty"`
	// Provenance names the repository commit (or working-tree snapshot) a
	// command that runs playbook code ran on, and the operator build.
	Provenance                 *provenance.Record            `json:"provenance,omitempty"`
	OptionReports              []*contracts.MitigationReport `json:"-"`
	UnexplainedRemovalBlockers []UnexplainedRemovalBlocker   `json:"unexplained_removal_blockers,omitempty"`
	Drift                      []sdk.Finding                 `json:"drift,omitempty"`
	BeforeFindings             []sdk.Finding                 `json:"before_findings"`
	ControllerFindings         []sdk.Finding                 `json:"controller_findings"`
	// NewSinceDispatch are the current findings that first activated after
	// this incident's responder launched. The controller runs one incident at
	// a time, so they may be a separate fault; closure still waits on health.
	NewSinceDispatch []sdk.Finding                `json:"new_since_dispatch,omitempty"`
	Progress         *FindingProgress             `json:"progress,omitempty"`
	IncidentVerified *apihistory.IncidentVerified `json:"incident_verified,omitempty"`
	// RecoveryWithdrawn says which alert withdrew a verified recovery, until the next one.
	RecoveryWithdrawn       string                         `json:"recovery_withdrawn,omitempty"`
	FailureReason           string                         `json:"failure_reason,omitempty"`
	Receipt                 *contracts.RepairActionReceipt `json:"-"`
	MitigationRoundsUsed    int                            `json:"mitigation_rounds_used"`
	MitigationRoundsAllowed int                            `json:"mitigation_rounds_allowed"`
	ResponseLoopsUsed       int                            `json:"response_loops_used"`
	ResponseLoopsAllowed    int                            `json:"response_loops_allowed"`
	MitigationHealth        *MitigationHealth              `json:"mitigation_health,omitempty"`
	RolloutPending          *contracts.RolloutPending      `json:"rollout_pending,omitempty"`
	MenuExecution           []MenuRoundExecution           `json:"-"`
	MenuParameters          map[string][]MenuParameter     `json:"-"`
	MenuCommands            map[string]string              `json:"-"`
	Menu                    *contracts.MitigationMenu      `json:"-"`
	Report                  *contracts.MitigationReport    `json:"-"`
	Session                 *contracts.MitigationSession   `json:"-"`
	Command                 string                         `json:"command"`
	Source                  map[string]any                 `json:"source,omitempty"`
	Error                   string                         `json:"error,omitempty"`
	Summary                 string                         `json:"summary"`
	PlanID                  string                         `json:"plan_id,omitempty"`
	ApplicationID           string                         `json:"application_id,omitempty"`
	Patches                 []PlanPatch                    `json:"patches,omitempty"`
	ProgramRuns             []ProgramRun                   `json:"program_runs,omitempty"`
	// ProgramRunsOmitted counts runs a truncated record could not list; the
	// trust reader fails closed on it. ProgramRunsUnrelatedOmitted counts apply
	// runs of programs or bindings the applied plans do not name, which the
	// reader never consults.
	ProgramRunsOmitted          int                     `json:"program_runs_omitted,omitempty"`
	ProgramRunsUnrelatedOmitted int                     `json:"program_runs_unrelated_omitted,omitempty"`
	Findings                    []sdk.Finding           `json:"findings,omitempty"`
	DriftEvents                 []sdk.DriftEvent        `json:"drift_events,omitempty"`
	DetectorStates              []DetectorEvaluation    `json:"detector_states,omitempty"`
	LoopsUsed                   int                     `json:"loops_used"`
	LoopsAllowed                int                     `json:"loops_allowed"`
	TargetedHealthy             bool                    `json:"targeted_healthy,omitempty"`
	Healthy                     bool                    `json:"healthy"`
	Unresolved                  bool                    `json:"unresolved"`
	Trust                       map[string]ProgramTrust `json:"trust,omitempty"`
}

type ToolCall struct {
	Command      string    `json:"command"`
	At           time.Time `json:"at"`
	RequestJSON  string    `json:"request_json"`
	ResponseJSON string    `json:"response_json"`
	Error        string    `json:"error,omitempty"`
	Summary      string    `json:"summary"`
}

type RecordedPlanApplication struct {
	ID            string
	ProgramIDs    []string
	Result        PlanApplicationResult
	Error         string
	StartedAt     time.Time
	CompletedAt   time.Time
	RolledBack    bool
	RollbackError string
	PlanIDs       []string
	Pending       bool
	// RunningCommit is the checkout commit the application ran on (the accepted
	// commit for the automatic stage); RunningDirty marks a differing tree.
	RunningCommit string
	RunningDirty  bool
}

type selectedPlan struct {
	targetFingerprints []string
	revision           string
	diagnoser          program.Diagnoser
	id                 string
	programID          string
	bindings           program.Bindings
	plan               program.Plan
	// snapshot is the one the plan was made on (its view's), nil when unknown.
	snapshot sdk.DetectionContext
}

// ResponderLoop holds one incident's tool state while its responder Job runs.
type ResponderLoop struct {
	// restoreEvidence is what the built-in source-restore playbook classifies with.
	restoreEvidence *sourceRestoreEvidence
	// ExternalClues lets `detect` run for an external incident as the candidate
	// view; check and verify stay without detector authority.
	ExternalClues bool
	// BuiltinRestoreAuto lets the automatic stage apply the built-in
	// source-restore playbook (response.builtin_restore_auto, on by default); off, the responder decides.
	BuiltinRestoreAuto bool

	stageProgress       map[string]responseStageProgress
	dataWorkloads       dataHolders
	dataWorkloadsRead   bool
	applicationBindings map[string][]recoveryBinding
	// WorkingProgramRoot is set by the production loop factory to the mounted
	// proposal checkout. Embedders with an in-memory registry may leave it empty.
	WorkingProgramRoot string
	// RepositoryState reads the HEAD and cleanliness of WorkingProgramRoot;
	// tests replace it. Production reads the checkout.
	RepositoryState func(ctx context.Context, dir string) (provenance.Repository, error)
	workingPrograms []*workingProgram
	pendingWrites   map[string]pendingMitigation
	MutationGate    *apihistory.MutationGate
	Now             func() time.Time
	CachedFindings  func() ToolResponse
	IncidentStatus  func() *apihistory.IncidentVerified
	// RecoveryWaiting is what Recovered waits on, for `sdo detect`; "" when unknown.
	RecoveryWaiting func() string
	// AlertProgress reports whether an alert of the incident's set cleared since detection (the investigator gate).
	AlertProgress func() bool
	// firstCall closes on the responder's first own tool call (firstToolCall); the investigator delay counts from it.
	firstCallInit, firstCallMark sync.Once
	firstCall                    chan struct{}
	WaitClosure                  func(context.Context, time.Duration) ToolResponse
	// WaitClock paces `sdo detect --wait`; nil uses production timers.
	WaitClock kclock.WithTicker
	// effectShown is the newest known change whose result a `sdo detect --wait`
	// returned, so the next wait waits for a newer one.
	effectShown time.Time
	// lastView is the snapshot the last detect read ran on (set on a read copy).
	lastView sdk.DetectionContext
	// handoffKeys are the finding keys the agent was handed at dispatch (nil
	// before one); newShown are the new findings a detect has already shown.
	handoffKeys map[string]bool
	newShown    map[string]bool
	// ExternalReceipts is the write recording the responder's writes are read from; nil when no recording
	// exists, in which case the contradiction notes omit their write clause.
	ExternalReceipts func() ([]RepairActionReceipt, bool, string, error)
	// noteStart and noteLog are the per-session call history behind the verify and detect notes.
	noteStart       time.Time
	noteLog         map[string][]noteObservation
	gateError       error
	closureReceipts []RepairActionReceipt
	continuation    responderContinuation
	MenuConfig      *ResponderMenuConfig
	menu            *contracts.MitigationMenu
	menuEntries     menuCatalog
	menuSession     *contracts.MitigationSession
	menuReports     []*contracts.MitigationReport
	menuExecutions  map[string][]MenuRoundExecution
	menuRepairs     []RepairActionReceipt
	VerifyClosure   func(context.Context) ToolResponse
	VerifyResult    func(context.Context, IncidentResult) ToolResponse
	CurrentEvidence func() *IncidentRequest
	// Investigations holds the incident's SDO-started investigators and their reports; nil when none can start.
	// It is display evidence only and never reaches IncidentResult, closure or escalation.
	Investigations *InvestigationBoard
	// IncidentSources supplies the signal ring and app-state source behind
	// `sdo changes`, `sdo timeline` and the inline changes; nil reads none.
	IncidentSources func() (*incidentSignals, AppStateChangeSource)
	// Telemetry supplies the application-telemetry reads behind `sdo metrics`,
	// `sdo logs` and `sdo traces`: the controller's own bound sources, so what
	// the agent reads is what detectors read. Nil reads none.
	Telemetry         func() ApplicationTelemetry
	Request           IncidentRequest
	Programs          program.ProgramRegistry
	Traffic           ProberAPI
	Provider          SnapshotProvider
	Detectors         []sdk.Detector
	Executor          PlanExecutor
	ReadView          func(sdk.DetectionContext) program.ReadView
	DiagnosisDeadline time.Duration
	// ProgramFollowUpSettle bounds each wait before a program follow-up plan;
	// PendingStallTimeout bounds how long a pending application holds its writes.
	ProgramFollowUpSettle time.Duration
	PendingStallTimeout   time.Duration
	MaxBatches            int
	Trust                 map[string]ProgramTrust
	// Credits are the (symptom, component) pairs each learned playbook earned;
	// a playbook without any keeps the finding binding.
	Credits             ProgramCredits
	CorpusSessionRoot   string
	RecoverySessionRoot string
	capturePhase        string
	sourceCompared      map[string]bool
	timing              toolTiming
	// trace times the phases of the Handle call in progress (nil outside one).
	trace            *phaseTrace
	preApplyCaptured bool
	saveToolState    func(context.Context, []byte) error
	operationMu      sync.RWMutex
	mu               sync.Mutex
	sessionID        string
	plans            map[string]selectedPlan
	applications     map[string]PlanApplicationResult
	applicationsLog  []RecordedPlanApplication
	// journaledApplications is len(applicationsLog), kept by setApplicationsLog.
	journaledApplications atomic.Int32
	// Restored application evidence has no live plan or rollback handles.
	archivedApplications []RecordedPlanApplication
	completedRepairs     []continuationRepair
	failureSignals       []string
	calls                []ToolCall
	runs                 []ProgramRun
	matchedViews         map[string]program.ReadView
	matchedPlans         map[string]PlanApplicationResult
	matchedPlanErrors    map[string]error
	terminalPlanErrors   map[string]*NonExecutablePlanError
	lastDiagnosisView    program.ReadView
	findings             []sdk.Finding
	// GitRead, when set, replaces the git process that reads accepted program
	// sources for revisions; a simulation serves them from memory.
	GitRead func(ctx context.Context, repository string, args ...string) ([]byte, error)
	drift   []sdk.Finding
	// batches counts the agent's response loops, the budget the agent is told.
	// A loop is one act→verify cycle: loopOpen is set by the first write after a
	// verification and cleared by the next one. applySeq numbers every apply,
	// including the automatic stage's, which never spends that budget.
	batches  int
	loopOpen bool
	applySeq int
	// batchVerified holds, per stage application applied with batchVerify,
	// whether its execution succeeded, until verifyStageBatch completes its
	// verification. It is not persisted: a restart leaves the application
	// unverified, which recovery settlement observes afresh.
	batchVerified map[string]bool
}

// ResponderLoopOptions supplies sources that must replay across simulated runs.
type ResponderLoopOptions struct {
	// NewSessionID returns 32 lowercase hexadecimal characters. Nil uses the
	// production cryptographic source; programs never receive the source itself.
	NewSessionID func() (string, error)
}

func NewResponderLoop(request IncidentRequest, programs program.ProgramRegistry, provider SnapshotProvider, detectors []sdk.Detector, executor PlanExecutor, maxBatches int) *ResponderLoop {
	loop, err := NewResponderLoopWithOptions(request, programs, provider, detectors, executor, maxBatches, ResponderLoopOptions{})
	if err != nil {
		panic(err)
	}
	return loop
}

// NewResponderLoopWithOptions creates one incident's replayable tool namespace.
func NewResponderLoopWithOptions(request IncidentRequest, programs program.ProgramRegistry, provider SnapshotProvider, detectors []sdk.Detector, executor PlanExecutor, maxBatches int, options ResponderLoopOptions) (*ResponderLoop, error) {
	if maxBatches <= 0 {
		maxBatches = DefaultMaxResponseLoops
	}
	source := options.NewSessionID
	if source == nil {
		source = lifecycleio.NewSessionID
	}
	session, err := source()
	if err != nil {
		return nil, fmt.Errorf("create responder session ID: %w", err)
	}
	decoded, err := hex.DecodeString(session)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != session {
		return nil, fmt.Errorf("responder session ID must be 32 lowercase hexadecimal characters")
	}
	if request.ExternalMessage != "" {
		detectors = nil
	}
	restoreEvidence := &sourceRestoreEvidence{}
	restoreEvidence.set(request)
	loop := &ResponderLoop{sessionID: session, Request: request, Programs: withSourceRestore(programs, restoreEvidence), restoreEvidence: restoreEvidence, Provider: provider, Detectors: detectors, Executor: executor, MaxBatches: maxBatches, plans: map[string]selectedPlan{}, applications: map[string]PlanApplicationResult{}, matchedViews: map[string]program.ReadView{}, matchedPlans: map[string]PlanApplicationResult{}, findings: append([]sdk.Finding(nil), request.Findings...)}
	priorRecorder := executor.RecordPrimitive
	loop.Executor.RecordPrimitive = func(call sdktest.PrimitiveCall) error {
		if priorRecorder != nil {
			if err := priorRecorder(call); err != nil {
				return err
			}
		}
		if loop.CorpusSessionRoot == "" {
			return nil
		}
		phase := loop.capturePhase
		if phase == "" {
			phase = "open@detect"
			if loop.preApplyCaptured {
				phase = "open@pre-apply"
			}
		}
		return memory.AppendCapturedRecording(loop.CorpusSessionRoot, loop.Request.IncidentID, phase, call)
	}
	return loop, nil
}

// setCredits installs the pairs the learned playbooks earned. The loop may
// already be published to the controller, so it takes the same locks as tools.
func (l *ResponderLoop) setCredits(credits ProgramCredits) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Credits = credits
}

// initializeDispatch may run after LoopFactory has published this loop to the
// controller. It follows the same operation/state lock order as tools/restore.
func (l *ResponderLoop) initializeDispatch(trust map[string]ProgramTrust, maxBatches int, runs []ProgramRun) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Trust = trust
	l.handoffKeys = map[string]bool{}
	for _, finding := range l.Request.Findings {
		l.handoffKeys[findingKey(finding)] = true
	}
	for _, key := range alertKeys(&l.Request) {
		l.handoffKeys[key] = true
	}
	if maxBatches > 0 {
		l.MaxBatches = maxBatches
	}
	l.runs = append([]ProgramRun(nil), runs...)
}

func (l *ResponderLoop) continuationAttempts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.continuation.Attempts
}

// setApplicationsLog is the one write of the plan application journal; it
// keeps the journaled count controller observation reads without the lock.
// The caller holds l.mu.
func (l *ResponderLoop) setApplicationsLog(log []RecordedPlanApplication) {
	l.applicationsLog = log
	l.journaledApplications.Store(int32(len(log)))
}

// hasJournaledPlans reports whether this loop has journaled a plan
// application, including one whose apply is still running (it is journaled
// before it writes). Controller observation calls it: it takes no lock, since
// an apply holds l.mu for as long as its script or rollout poll runs.
func (l *ResponderLoop) hasJournaledPlans() bool {
	return l.journaledApplications.Load() > 0
}

// tryRecordedWrites reads the agent's write recording if the loop is free:
// readable is false when the recording is off or a loop operation holds the
// loop (the read's pre-apply capture must not race the loop's own).
func (l *ResponderLoop) tryRecordedWrites() (readable, wrote bool) {
	if l.ExternalReceipts == nil {
		return false, false
	}
	if !l.mu.TryLock() {
		return false, false
	}
	defer l.mu.Unlock()
	receipts, _, _, err := l.ExternalReceipts()
	return true, err == nil && len(receipts) > 0
}

func (l *ResponderLoop) observeMutations() (*apihistory.MutationObservation, error) {
	// A cancelled turn may still be unwinding a tool. Controller observation
	// retries on contention instead of waiting behind responder-owned work.
	gate, err := func() (*apihistory.MutationGate, error) {
		if !l.mu.TryLock() {
			return nil, errors.New("responder mutation observation is busy")
		}
		defer l.mu.Unlock()
		return l.MutationGate, nil
	}()
	if err != nil {
		return nil, err
	}
	if gate == nil {
		return nil, nil
	}
	observation, err := gate.TryObserve()
	if err == nil && observation == nil {
		err = errors.New("responder mutation observation is unavailable")
	}
	return observation, err
}

func (l *ResponderLoop) ToolCalls() []ToolCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ToolCall(nil), l.calls...)
}
func (l *ResponderLoop) ProgramRuns() []ProgramRun {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ProgramRun(nil), l.runs...)
}

// PlanApplications returns only applications observed in this process. Restarted
// evidence cannot be used as live execution state.
func (l *ResponderLoop) PlanApplications() []RecordedPlanApplication {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RecordedPlanApplication(nil), l.applicationsLog...)
}

// ApplicationEvidence returns historical and live facts for outcome/learning
// attribution only. It confers no execution or rollback authority.
func (l *ResponderLoop) ApplicationEvidence() []RecordedPlanApplication {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.applicationEvidenceLocked()
}

// Caller holds l.mu. Live observations supersede the archived receipt for the
// same application; unrelated restored receipts must survive subsequent saves.
func (l *ResponderLoop) applicationEvidenceLocked() []RecordedPlanApplication {
	result := append([]RecordedPlanApplication(nil), l.archivedApplications...)
	positions := map[string]int{}
	for i, application := range result {
		positions[application.ID] = i
	}
	for _, application := range l.applicationsLog {
		if i, ok := positions[application.ID]; ok {
			result[i] = application
		} else {
			positions[application.ID] = len(result)
			result = append(result, application)
		}
	}
	return result
}

func (l *ResponderLoop) FailureSignals() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.failureSignals...)
}

func (l *ResponderLoop) InitialDiagnose(ctx context.Context) []ProgramRun {
	ctx, closeMemory := withMemoryScope(ctx)
	defer closeMemory()
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	// With no learned playbook there is nothing to record diagnoses for, and the
	// controller stored open@detect when the incident opened: skip the retake
	// and its source-comparison snapshot, which would delay the agent.
	if l.hasLearnedPlaybooks() {
		l.capturePhase = "open@detect"
	}
	defer func() { l.capturePhase = "" }()
	return l.diagnose(ctx, l.findings, true)
}

// hasLearnedPlaybooks reports whether any registered playbook is not the
// built-in source restore.
func (l *ResponderLoop) hasLearnedPlaybooks() bool {
	if l.Programs == nil {
		return false
	}
	for _, spec := range l.Programs.List() {
		if spec.ID != SourceRestoreID {
			return true
		}
	}
	return false
}

func (l *ResponderLoop) LoopsUsed() int { l.mu.Lock(); defer l.mu.Unlock(); return l.batches }
func (l *ResponderLoop) Exhausted() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.loopsRemaining() == 0 && len(l.findings) > 0
}

// toolTiming accumulates the phase durations of one Handle call. capture is
// the time spent in snapshotView (provider snapshot, corpus capture, routing
// probes) and refresh is the time spent in refresh, which includes capture.
type toolTiming struct{ capture, refresh, persist time.Duration }

func (l *ResponderLoop) Handle(ctx context.Context, request ToolRequest) (response ToolResponse) {
	if !request.isolatedRead {
		l.markFirstToolCall()
	}
	if !request.isolatedRead && concurrentRead(request.Command) {
		return l.handleRead(ctx, request)
	}
	ctx, closeMemory := withMemoryScope(ctx)
	defer closeMemory()
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.startNotes()
	started := l.clock()
	mutating := request.Command == "rollback" || request.Command == "apply" || request.Command == "mitigate" || request.Command == "act-result"
	var before []sdk.Finding
	if mutating && l.CachedFindings != nil {
		before = l.CachedFindings().Findings
	}
	l.timing = toolTiming{}
	trace := newPhaseTrace(l.clock)
	l.trace = trace
	ctx = withPhaseTrace(ctx, trace)
	defer func() {
		total := l.clock().Sub(started)
		slog.Info("responder_tool_timing", append([]any{"incident_id", l.Request.IncidentID, "command", request.Command,
			"capture_ms", l.timing.capture.Milliseconds(), "refresh_ms", l.timing.refresh.Milliseconds(),
			"persist_ms", l.timing.persist.Milliseconds(), "total_ms", total.Milliseconds()}, trace.logAttrs(total)...)...)
		l.trace = nil
	}()
	defer func() { l.capturePhase = "" }()
	if l.CurrentEvidence != nil {
		if current := l.CurrentEvidence(); current != nil && current.IncidentID == l.Request.IncidentID {
			l.Request.Findings = append([]sdk.Finding(nil), current.Findings...)
			l.Request.DriftEvents = append([]sdk.DriftEvent(nil), current.DriftEvents...)
		}
	}
	if len(l.pendingWrites) > 0 {
		l.settlePending(ctx)
	}
	response = ToolResponse{Command: request.Command, LoopsUsed: l.batches, LoopsAllowed: l.batchLimit(),
		MitigationRoundsUsed: l.batches, MitigationRoundsAllowed: l.batchLimit(), ResponseLoopsUsed: l.continuation.Attempts, ResponseLoopsAllowed: l.MaxBatches}
	if l.Request.ExternalMessage != "" && (request.Command == "check" || request.Command == "verify" || request.Command == "detect" && !l.ExternalClues) {
		response.VerificationStatus = VerificationAwaitingResponderCompletion
		response.Summary = "External incident: detectors are disabled. Investigate with live reads and return your completed result when done."
		return response
	}
	if l.Request.MaxResponseLoops > 0 {
		response.ResponseLoopsAllowed = min(response.ResponseLoopsAllowed, l.Request.MaxResponseLoops)
	}
	defer func() {
		if mutating && l.CachedFindings != nil {
			after := l.CachedFindings()
			response.BeforeFindings = append([]sdk.Finding{}, before...)
			response.ControllerFindings = append([]sdk.Finding{}, after.Findings...)
			progress := CompareFindingProgress(before, after.Findings)
			response.Progress = &progress
		}
		// The controller decides; the fence file can outlive a withdrawn recovery.
		switch {
		case l.IncidentStatus != nil:
			response.IncidentVerified = l.IncidentStatus()
		case l.MutationGate != nil:
			response.IncidentVerified, _ = l.MutationGate.Verified()
		}
		response.RecoveryWithdrawn = l.recoveryWithdrawn()
		response.MitigationRoundsUsed = l.batches
		response.MitigationRoundsAllowed = l.batchLimit()
		if response.Error != "" {
			response.Summary = response.Error
		}
		requestJSON, _ := json.Marshal(request)
		if l.Request.ExternalMessage != "" {
			response.Healthy = false
			response.DetectorStates = nil
			response.IncidentVerified = nil
			response.VerificationStatus = VerificationAwaitingResponderCompletion
		}
		response = logicalToolResponse(response)
		var applied []selectedPlan
		if request.Command == "apply" {
			for _, id := range request.PlanIDs {
				if plan, ok := l.plans[id]; ok {
					applied = append(applied, plan)
				}
			}
		}
		if request.Command == "apply" {
			response.ProgramRevisions = map[string]string{}
			for _, plan := range applied {
				response.ProgramRevisions[plan.programID] = plan.revision
			}
		}
		if request.ProgramID != "" && (request.Command == "plan" || request.Command == "diagnose") && response.Error == "" {
			response.ProgramRevisions = map[string]string{request.ProgramID: l.programRevision(request.ProgramID)}
		}
		if request.isolatedRead {
			return
		}
		l.stampProvenance(ctx, request, &response)
		responseJSON := recordedToolResponseJSON(response, applied)
		l.calls = append(l.calls, ToolCall{Command: request.Command, At: l.clock().UTC(), RequestJSON: string(requestJSON), ResponseJSON: string(responseJSON), Error: response.Error, Summary: response.Summary})
		persistStarted := l.clock()
		err := l.persistToolState(ctx)
		l.timing.persist = l.clock().Sub(persistStarted)
		if err != nil {
			response.Error = err.Error()
			response.Summary = response.Error
		}
	}()
	response.DriftEvents = append([]sdk.DriftEvent(nil), l.Request.DriftEvents...)

	if request.Command == "apply" || request.Command == "mitigate" || request.Command == "rollback" {
		if l.gateError != nil {
			response.Error = l.gateError.Error()
			if request.Command == "mitigate" {
				l.refuseSelection(&response, "mutation_gate_unavailable", response.Error)
			}
			return response
		}
		if l.MutationGate != nil {
			lease, err := l.MutationGate.Begin()
			if err != nil {
				response.Error = err.Error()
				response.FailureReason = "execution_failed"
				if response.Error == "incident_verified" {
					response.FailureReason = "incident_verified"
				}
				if request.Command == "mitigate" {
					l.refuseSelection(&response, response.FailureReason, response.Error)
				}
				response.Receipt = l.rejectedReceipt(request.Command, response, started)
				return response
			}
			defer func() {
				if err := lease.Finish(false); err != nil {
					response.Error = err.Error()
				}
			}()
		} else if l.IncidentStatus != nil && l.IncidentStatus() != nil {
			response.Error = "incident_verified"
			response.FailureReason = "incident_verified"
			if request.Command == "mitigate" {
				l.refuseSelection(&response, response.FailureReason, response.Error)
			}
			response.Receipt = l.rejectedReceipt(request.Command, response, started)
			return response
		}
	}
	switch request.Command {
	case "menu", "plan", "diagnose":
		if err := l.loadWorkingPrograms(ctx, request.ProgramID); err != nil {
			response.Error = err.Error()
			return response
		}
		if request.Command == "diagnose" && request.ProgramID == "" {
			if registry, ok := l.Programs.(*workingProgramRegistry); ok {
				for _, message := range registry.failures {
					response.Error = message
					return response
				}
			}
		}
	case "mitigate":
		if err := l.loadTargetPrograms(ctx, request.Targets); err != nil {
			l.refuseSelection(&response, "program_unavailable", err.Error())
			return response
		}
		if err := l.resolveMitigateTargets(&request); err != nil {
			l.refuseSelection(&response, "invalid_selection", err.Error())
			return response
		}
		if err := l.loadSelectedWorkingPrograms(ctx, request); err != nil {
			l.refuseSelection(&response, "program_unavailable", err.Error())
			return response
		}
	}
	switch request.Command {
	case "menu":
		l.listMenu(ctx, request, &response)
	case "mitigate":
		if err := l.validateFindingTargets(request.FindingFingerprints); err != nil {
			l.refuseSelection(&response, "invalid_selection", err.Error())
			break
		}
		l.mitigateSelection(ctx, request, &response)
	case "session-open":
		l.openMenuSession(ctx, &response)
	case "session-checkpoint":
		l.checkpointMenuSession(ctx, request.Checkpoint, &response)
	case "report":
		l.selectionReport(ctx, request.ReportQuery, &response)
	case "detect":
		l.detectNamed(ctx, request, &response)
	case "changes", "timeline":
		l.incidentPage(ctx, request, &response)
	case "map":
		l.applicationMap(ctx, request, &response)
	case "metrics", "logs", "traces":
		l.applicationTelemetry(ctx, request, &response)
	case "status":
		response.DriftEvents = append([]sdk.DriftEvent(nil), l.Request.DriftEvents...)
		response.Trust = l.Trust
		response.Findings, response.ProgramRuns, response.DetectorStates, response.Error = l.refresh(ctx, false)
		response.Drift = append([]sdk.Finding(nil), l.drift...)
		response.Healthy = response.Error == "" && len(l.findings) == 0
		for _, state := range response.DetectorStates {
			response.Healthy = response.Healthy && state.Status == DetectorEvaluationClear
		}
		response.Unresolved = l.loopsRemaining() == 0 && !response.Healthy
		response.Summary = fmt.Sprintf("%d open findings; %d of %d act-verify loops used", len(l.findings), l.batches, l.batchLimit()) + pendingEvaluationSummary(response.DetectorStates)
	case "diagnose":
		if request.ProgramID == "" {
			response.Findings, response.ProgramRuns, response.DetectorStates, response.Error = l.refresh(ctx, true)
			response.Drift = append([]sdk.Finding(nil), l.drift...)
		} else {
			if l.Programs == nil {
				response.Error = "no programs are compiled"
				break
			}
			p, ok := l.Programs.Diagnoser(request.ProgramID)
			if !ok {
				response.Error = fmt.Sprintf("unknown program %q", request.ProgramID)
				break
			}
			if spec, found := l.programSpec(request.ProgramID); found {
				bound, err := diagnoseBindings(spec, request)
				if err != nil {
					response.Error = err.Error()
					break
				}
				request.Bindings = bound
			}
			view, err := l.snapshotView(ctx)
			if err != nil {
				response.Error = err.Error()
				break
			}
			started := l.clock()
			verdict := program.RunDiagnose(ctx, p, view, request.Bindings, l.deadline())
			_, canMitigate := l.Programs.Mitigator(request.ProgramID)
			run := ProgramRun{ProgramRevision: l.programRevision(request.ProgramID), CanMitigate: &canMitigate, ProgramID: request.ProgramID, Bindings: request.Bindings, Verdict: verdict, Duration: l.clock().Sub(started)}
			if canMitigate && verdict.Result == program.Match {
				run.MitigateCommand = mitigateCommand(request.ProgramID, request.Bindings)
			}
			response.ProgramRuns = []ProgramRun{run}
			l.runs = append(l.runs, run)
		}
		response.Summary = fmt.Sprintf("%d program diagnoses", len(response.ProgramRuns))
	case "plan":
		l.plan(ctx, request, &response)
	case "source":
		if request.Ref == nil || l.Executor.Source == nil || l.Request.renderedRevision() == "" {
			response.Error = "source requires a resource and accepted source"
			break
		}
		if err := validatePlanRef(*request.Ref); err != nil {
			response.Error = err.Error()
			break
		}
		if request.Ref.Namespace != l.Request.Namespace && request.Ref.Namespace != "" {
			response.Error = "source resource is outside incident namespace"
			break
		}
		if l.CorpusSessionRoot != "" {
			l.capturePhase = "open@detect"
			view, err := l.snapshotView(ctx)
			if err != nil {
				response.Error = err.Error()
				break
			}
			// An object without an acceptance record compares as unknown;
			// accepted source itself is still shown.
			l.recordSourceComparisons(ctx, l.capturePhase, viewSnapshot(view), *request.Ref)
		}
		object, err := acceptedIncidentManifest(ctx, l.Executor.Source, l.Request.renderedRevision(), l.Request.Namespace, *request.Ref)
		if errors.Is(err, ErrAcceptedManifestAbsent) {
			response.Summary = "resource is absent from accepted source"
			break
		}
		if err != nil {
			response.Error = err.Error()
			break
		}
		if object == nil || !samePlanRef(object, *request.Ref) {
			response.Error = "accepted source object does not match requested resource"
			break
		}
		response.Source = object.Object
		response.Summary = fmt.Sprintf("rendered source for %s/%s", request.Ref.Kind, request.Ref.Name)
	case "apply":
		l.apply(ctx, request, &response)
		if result, ok := l.applications[response.ApplicationID]; ok {
			response.MitigationHealth = result.MitigationHealth
			response.TargetedHealthy = result.VerificationPassed
		}
	case "stage-verify":
		// Internal to the automatic stage; a tool call cannot set stage.
		if !request.stage {
			response.Error = fmt.Sprintf("unknown sdo command %q", request.Command)
			break
		}
		l.verifyStageBatch(ctx, request.batchApplications, &response)
	case "verify":
		if l.Traffic == nil && (request.Workload != "" || len(request.Scenarios) > 0) {
			response.Error = "bounded traffic verification is unavailable"
			break
		}
		var burst *prober.BurstResult
		if l.Traffic != nil {
			result, err := l.Traffic.Burst(ctx, prober.BurstRequest{Workload: request.Workload, Scenarios: request.Scenarios})
			if err != nil {
				response.Error = err.Error()
				break
			}
			burst = &result
		}
		if l.VerifyClosure != nil {
			// The controller may persist continuation state while observing.
			// A read-only closure check must not hold responder locks while
			// waiting for that observation to finish.
			var closure ToolResponse
			// With no recorded write since the incident opened there is nothing to settle.
			waitForSettle := request.WaitSeconds > 0 && l.WaitClosure != nil && l.hasWrites()
			if err := withoutResponderLocks(l, "verify", true, func() {
				if request.WaitSeconds < 0 || request.WaitSeconds > 86400 {
					closure = ToolResponse{Command: "verify", Error: "wait seconds must be between 0 and 86400"}
				} else if waitForSettle {
					closure = l.WaitClosure(ctx, time.Duration(request.WaitSeconds*float64(time.Second)))
				} else {
					closure = l.VerifyClosure(ctx)
				}
			}); err != nil {
				response = ToolResponse{Command: "verify", Error: err.Error()}
				break
			}
			response = closure
			if request.PreviousFindings != nil {
				progress := CompareFindingProgress(request.PreviousFindings, response.Findings)
				response.Progress = &progress
			}
			l.findings = append([]sdk.Finding(nil), response.Findings...)
			response.LoopsAllowed = l.batchLimit()
		} else {
			l.verify(ctx, nil, &response, request.stage)
		}
		if burst != nil {
			response.Burst = burst
			if !burst.Healthy {
				response.Healthy = false
				response.Summary = "bounded traffic verification is unhealthy"
			}
		}
		if response.Error == "" {
			l.closeLoop()
			response.DataEffects = l.incidentDataEffects(ctx)
			appendNote(&response, l.verifyNote(response.Findings))
			l.appendReportNotes(&response)
		}
	case "rollback":
		l.rollbackApplication(ctx, request, &response)
	default:
		response.Error = fmt.Sprintf("unknown sdo command %q", request.Command)
	}
	response.LoopsUsed = l.batches
	return response
}

// truncatedToolResponse is the over-cap record shape. It embeds ToolResponse so
// the identifiers and compact program runs that program trust reads share the
// writer's definition; everything bulky is reduced to a count.
type truncatedToolResponse struct {
	ToolResponse
	VerificationSummary   *VerificationSummary `json:"verification_summary,omitempty"`
	Truncated             bool                 `json:"truncated"`
	FindingsCount         int                  `json:"findings_count"`
	ProgramRunsCount      int                  `json:"program_runs_count"`
	ProgramRevisionsCount int                  `json:"program_revisions_count"`
	DetectorStatesCount   int                  `json:"detector_states_count"`
	PatchesCount          int                  `json:"patches_count"`
}

// recordedToolResponseJSON caps the stored response. An apply record keeps every
// run of the applied plans' (program, bindings) — all program trust reads — and
// counts the runs of other programs separately, since a real apply response
// carries a run for every installed program. Other commands drop runs from the
// end; the reader fails closed on any such omission.
func recordedToolResponseJSON(response ToolResponse, applied []selectedPlan) []byte {
	responseJSON, _ := json.Marshal(response)
	if len(responseJSON) <= maxRecordedToolResponse {
		return responseJSON
	}
	record := truncatedToolResponse{
		ToolResponse: ToolResponse{Command: response.Command, Summary: boundedToolText(response.Summary), Error: boundedToolText(response.Error), PlanID: response.PlanID, ApplicationID: response.ApplicationID, LoopsUsed: response.LoopsUsed, LoopsAllowed: response.LoopsAllowed, Healthy: response.Healthy, TargetedHealthy: response.TargetedHealthy, Unresolved: response.Unresolved, RolloutPending: compactRolloutPending(response.RolloutPending), Provenance: response.Provenance},
		Truncated:    true, ProgramRevisionsCount: len(response.ProgramRevisions), FindingsCount: len(response.Findings), ProgramRunsCount: len(response.ProgramRuns), DetectorStatesCount: len(response.DetectorStates), PatchesCount: len(response.Patches),
	}
	if response.Command == "verify" {
		record.VerificationSummary = compactVerification(response)
		candidate, _ := json.Marshal(record)
		if len(candidate) > maxRecordedToolResponse {
			record.VerificationSummary = nil
		}
	}
	limit := maxRecordedToolResponse
	isApply := response.Command == "apply"
	if isApply {
		limit = maxRecordedApplyResponse
	}
	for _, run := range response.ProgramRuns {
		if isApply && !runOfAppliedPlan(run, applied) {
			record.ProgramRunsUnrelatedOmitted++
			continue
		}
		record.ProgramRuns = append(record.ProgramRuns, compactProgramRun(run))
	}
	// Find the longest prefix within budget in logarithmic encodes. Removing
	// one run and re-encoding the entire response is quadratic on large menus.
	runs := record.ProgramRuns
	omitted := record.ProgramRunsOmitted
	low, high := 0, len(runs)
	for low < high {
		mid := low + (high-low+1)/2
		record.ProgramRuns = runs[:mid]
		record.ProgramRunsOmitted = omitted + len(runs) - mid
		candidate, _ := json.Marshal(record)
		if len(candidate) <= limit {
			low = mid
		} else {
			high = mid - 1
		}
	}
	record.ProgramRuns = runs[:low]
	record.ProgramRunsOmitted = omitted + len(runs) - low
	responseJSON, _ = json.Marshal(record)
	return responseJSON
}

func runOfAppliedPlan(run ProgramRun, applied []selectedPlan) bool {
	for _, plan := range applied {
		if run.ProgramID == plan.programID && run.Bindings.Equal(plan.bindings) {
			return true
		}
	}
	return false
}

// compactProgramRun keeps what program trust binds to: identity, revision,
// bindings, result and the diagnosis error (the evidence entry without a
// resource kind).
func compactProgramRun(run ProgramRun) ProgramRun {
	compact := ProgramRun{ProgramID: run.ProgramID, ProgramRevision: run.ProgramRevision, Bindings: run.Bindings, Verdict: program.Verdict{Result: run.Verdict.Result}}
	for _, evidence := range run.Verdict.Evidence {
		if evidence.Resource.Kind == "" {
			compact.Verdict.Evidence = []program.Evidence{{Observed: boundedToolText(evidence.Observed)}}
			break
		}
	}
	return compact
}

func boundedToolText(value string) string {
	if len(value) <= 256 {
		return value
	}
	return value[:256] + " [truncated]"
}

func (l *ResponderLoop) deadline() time.Duration {
	if l.DiagnosisDeadline > 0 {
		return l.DiagnosisDeadline
	}
	return DefaultDiagnosisDeadline
}

// phase begins the named phase of the Handle call in progress and returns its
// end; outside a call it measures nothing. The caller holds l.mu, as l.trace is
// set and cleared under it.
func (l *ResponderLoop) phase(name string) func() { return l.trace.begin(name) }

func (l *ResponderLoop) snapshotView(ctx context.Context) (program.ReadView, error) {
	started := l.clock()
	defer func() { l.timing.capture += l.clock().Sub(started) }()
	if l.Provider == nil {
		return nil, errors.New("snapshot provider is unavailable")
	}
	endSnapshot := l.phase("snapshot")
	snapshot, err := l.Provider.Snapshot(ctx)
	endSnapshot()
	if err != nil {
		return nil, err
	}
	if l.ReadView == nil {
		return nil, errors.New("live read view is unavailable")
	}
	if l.CorpusSessionRoot != "" && l.capturePhase != "" {
		endCapture := l.phase("corpus_capture")
		err := memory.CaptureIncidentSnapshot(l.CorpusSessionRoot, l.Request.IncidentID, l.capturePhase, snapshot, l.Request.Namespace, findingCaptureRefs(l.Request.Findings)...)
		endCapture()
		if err != nil {
			return nil, err
		}
	}
	l.restoreEvidence.set(l.Request)
	// Captures are immutable historical evidence. Every operational diagnosis,
	// including a resumed initial diagnosis, uses this attempt's fresh snapshot.
	view := NewCycleReadView(l.ReadView(snapshot))
	view.snapshot = snapshot
	if l.CorpusSessionRoot != "" && l.capturePhase != "" {
		phase := l.capturePhase
		return &sdktest.RecordingReadView{ReadView: view, OnRecord: func(call sdktest.PrimitiveCall) {
			_ = memory.AppendCapturedRecording(l.CorpusSessionRoot, l.Request.IncidentID, phase, call)
		}}, nil
	}
	return view, nil
}

// RefreshDiagnose diagnoses the open findings against a fresh snapshot without
// retaking a capture: the opening captures stay what the incident opened with.
func (l *ResponderLoop) RefreshDiagnose(ctx context.Context, findings []sdk.Finding, attempted map[string]bool) ([]ProgramRun, error) {
	ctx, closeMemory := withMemoryScope(ctx)
	defer closeMemory()
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.capturePhase = ""
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil, err
	}
	registry, deadline := l.Programs, l.deadline()
	keyed := l.keyedMatching(view, findings, true)
	keyed.Attempted = attempted
	var runs []ProgramRun
	err = withoutResponderLocks(l, "diagnose", true, func() {
		runs = SelectKeyedProgramCandidates(ctx, registry, view, findings, deadline, keyed)
	})
	l.stampProgramRuns(runs)
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	return runs, err
}

// RediagnosePrograms diagnoses again, against a fresh snapshot and without a
// capture, only the candidates of the named programs. The opening capture and
// the view coverage relates findings through stay what the incident opened with.
func (l *ResponderLoop) RediagnosePrograms(ctx context.Context, ids map[string]bool) []ProgramRun {
	ctx, closeMemory := withMemoryScope(ctx)
	defer closeMemory()
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Programs == nil || len(ids) == 0 {
		return nil
	}
	l.capturePhase = ""
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	runs := SelectKeyedProgramCandidates(ctx, onlyPrograms{ProgramRegistry: l.Programs, ids: ids}, view, l.findings, l.deadline(), l.keyedMatching(view, l.findings, true))
	l.stampProgramRuns(runs)
	l.runs = append(l.runs, runs...)
	return runs
}

// onlyPrograms narrows a registry to the named programs.
type onlyPrograms struct {
	program.ProgramRegistry
	ids map[string]bool
}

func (r onlyPrograms) List() []program.ProgramSpec {
	var specs []program.ProgramSpec
	for _, spec := range r.ProgramRegistry.List() {
		if r.ids[spec.ID] {
			specs = append(specs, spec)
		}
	}
	return specs
}

func (r onlyPrograms) Diagnoser(id string) (program.Diagnoser, bool) {
	if !r.ids[id] {
		return nil, false
	}
	return r.ProgramRegistry.Diagnoser(id)
}

func (r onlyPrograms) Mitigator(id string) (program.Mitigator, bool) {
	if !r.ids[id] {
		return nil, false
	}
	return r.ProgramRegistry.Mitigator(id)
}

// scanPage diagnoses every learned playbook whose parameters bind without a
// finding. It only reads.
func (l *ResponderLoop) scanPage(ctx context.Context) []ProgramRun {
	ctx, closeMemory := withMemoryScope(ctx)
	defer closeMemory()
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.hasLearnedPlaybooks() {
		return nil
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	runs := SelectKeyedProgramCandidates(ctx, l.Programs, view, nil, l.deadline(), KeyedMatching{Scan: true, Credits: l.Credits})
	l.stampProgramRuns(runs)
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	return runs
}

// selectCandidates diagnoses the candidates for findings. stage is the
// automatic stage, which ranks a playbook's credited pairs before its
// finding-bound bindings and keeps one Match per credited playbook.
func (l *ResponderLoop) selectCandidates(ctx context.Context, view program.ReadView, findings []sdk.Finding, stage bool) []ProgramRun {
	return SelectKeyedProgramCandidates(ctx, l.Programs, view, findings, l.deadline(), l.keyedMatching(view, findings, stage))
}

// keyedMatching is how the incident's candidates for findings bind against
// view: its credits and the suspects the findings and view point at.
func (l *ResponderLoop) keyedMatching(view program.ReadView, findings []sdk.Finding, stage bool) KeyedMatching {
	set := buildSuspectSet(detectInput{Request: l.Request, View: view, Findings: findings})
	return KeyedMatching{Credits: l.Credits, OpeningKeys: l.Request.OpeningKeys, Stage: stage, Restore: set.restoreCandidates(), Suspect: set.suspects(view)}
}

func (l *ResponderLoop) diagnose(ctx context.Context, findings []sdk.Finding, stage bool) []ProgramRun {
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	runs := l.selectCandidates(ctx, view, findings, stage)
	l.stampProgramRuns(runs)
	// Automatic planning follows candidate filtering and a fresh diagnosis.
	if !stage {
		l.rememberMatchedViews(ctx, runs, view)
	}
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	if l.capturePhase != "" {
		l.recordSourceComparisons(ctx, l.capturePhase, viewSnapshot(view), findingCaptureRefs(findings)...)
	}
	return runs
}

func (l *ResponderLoop) refresh(ctx context.Context, diagnose bool) ([]sdk.Finding, []ProgramRun, []DetectorEvaluation, string) {
	started := l.clock()
	defer func() { l.timing.refresh += l.clock().Sub(started) }()
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil, nil, nil, err.Error()
	}
	findings := make([]sdk.Finding, 0)
	states := make([]DetectorEvaluation, 0, len(l.Detectors))
	endDetectors := l.phase("detectors")
	results := evaluateAdmittedDetectors(ctx, l.Detectors, view)
	endDetectors()
	for _, detector := range l.Detectors {
		result := results[detector.Spec().ID]
		observed, err := result.findings, result.err
		state := DetectorEvaluation{DetectorID: detector.Spec().ID, EvaluatedAt: l.clock().UTC(), Status: DetectorEvaluationClear, Reason: detectorConfirmationReason(detector, view)}
		if err == nil && len(observed) == 0 && !detectorConfirmed(detector, view) {
			state.Status = DetectorEvaluationPending
		}
		// A finding the controller refuses fails only its detector, as in the
		// controller; the other detectors' findings are still the read.
		var refused findingAdmissionError
		if errors.As(err, &refused) {
			state.Status, state.Error = DetectorEvaluationError, err.Error()
			states = append(states, state)
			continue
		}
		if err != nil {
			state.Status, state.Error = DetectorEvaluationError, err.Error()
			states = append(states, state)
			return nil, nil, states, fmt.Sprintf("detector %s: %v", detector.Spec().ID, err)
		}
		for _, finding := range observed {
			if finding.Status == sdk.FindingActive {
				findings = append(findings, finding)
				state.Status = DetectorEvaluationFiring
				state.Fingerprints = append(state.Fingerprints, FindingFingerprint(finding))
			}
		}
		states = append(states, state)
	}
	l.drift = retainedDrift(l.Request.Findings, view)
	l.findings = findings
	if !diagnose {
		return findings, nil, states, ""
	}
	endDiagnose := l.phase("diagnose")
	runs := l.selectCandidates(ctx, view, findings, false)
	endDiagnose()
	l.stampProgramRuns(runs)
	l.runs = append(l.runs, runs...)
	return findings, runs, states, ""
}

func matchedViewKey(id string, bindings program.Bindings) string {
	return id + "\x00" + bindingKey(bindings)
}

func (l *ResponderLoop) rememberMatchedViews(ctx context.Context, runs []ProgramRun, view program.ReadView) {
	if l.matchedPlanErrors == nil {
		l.matchedPlanErrors = make(map[string]error)
	}
	for _, run := range runs {
		if run.Verdict.Result == program.Match {
			key := matchedViewKey(run.ProgramID, run.Bindings)
			// The view and plan belong to the same preparation attempt, including
			// failures. Never leave an earlier plan attached to a replacement view.
			delete(l.matchedPlans, key)
			delete(l.matchedPlanErrors, key)
			l.matchedViews[key] = view
			mitigator, ok := l.Programs.Mitigator(run.ProgramID)
			if !ok {
				continue
			}
			plan, err := l.runMitigator(ctx, run.ProgramID, mitigator, view, run.Bindings)
			if err != nil {
				l.matchedPlanErrors[key] = err
				continue
			}
			preview, err := l.Executor.Apply(ctx, l.Request, plan, true)
			if err != nil {
				l.matchedPlanErrors[key] = err
				continue
			}
			l.matchedPlans[key] = preview
		}
	}
}

func (l *ResponderLoop) recoveryWithdrawn() string {
	if l.MutationGate == nil {
		return ""
	}
	reason, _ := l.MutationGate.Withdrawn()
	return reason
}

func logicalToolResponse(response ToolResponse) ToolResponse {
	for index := range response.Findings {
		finding := &response.Findings[index]
		copyFinding := *finding
		copyFinding.Playbooks = make([]string, 0, len(finding.Playbooks))
		for _, reference := range finding.Playbooks {
			id, err := layout.PlaybookID(reference)
			if err == nil {
				copyFinding.Playbooks = append(copyFinding.Playbooks, id)
			}
		}
		response.Findings[index] = copyFinding
	}
	return response
}

func (l *ResponderLoop) plan(ctx context.Context, request ToolRequest, response *ToolResponse) {
	if err := l.validateFindingTargets(request.FindingFingerprints); err != nil {
		response.Error = err.Error()
		return
	}
	if l.Programs == nil {
		response.Error = "no programs are compiled"
		return
	}
	mitigator, ok := l.Programs.Mitigator(request.ProgramID)
	if !ok {
		response.Error = fmt.Sprintf("program %q has no mitigator", request.ProgramID)
		return
	}
	if err := program.ValidateBindings(mitigator.Spec(), request.Bindings); err != nil {
		response.Error = err.Error()
		return
	}
	view := l.matchedViews[matchedViewKey(request.ProgramID, request.Bindings)]
	if view == nil || l.appliedAny() {
		var err error
		view, err = l.snapshotView(ctx)
		if err != nil {
			response.Error = err.Error()
			return
		}
	}
	key := matchedViewKey(request.ProgramID, request.Bindings)
	preview, matched := l.matchedPlans[key]
	plan := preview.Plan
	if !matched || l.appliedAny() {
		var err error
		plan, err = l.runMitigator(ctx, request.ProgramID, mitigator, view, request.Bindings)
		if err != nil {
			response.Error = err.Error()
			response.FailureReason = ""
			return
		}
	}
	if err := plan.Validate(); err != nil {
		l.rememberTerminalPlanError(request.ProgramID, l.programRevision(request.ProgramID), request.Bindings, &NonExecutablePlanError{Cause: err})
		response.Error = err.Error()
		return
	}
	if len(plan.Actions) == 0 {
		response.Error = "program returned an empty plan"
		return
	}
	if request.stage {
		for _, action := range plan.Actions {
			if hasUnencodableAction(historicalActionProto(action)) {
				err := &NonExecutablePlanError{Cause: fmt.Errorf("cannot encode %T in the automatic plan", action)}
				l.rememberTerminalPlanError(request.ProgramID, l.programRevision(request.ProgramID), request.Bindings, err)
				response.Error = err.Error()
				return
			}
		}
	}
	preview, err := l.Executor.Apply(ctx, l.Request, plan, true)
	if err != nil {
		response.Error = err.Error()
		return
	}
	id := fmt.Sprintf("plan-%s-%d", l.sessionID, len(l.plans)+1)
	diagnoser, _ := l.Programs.Diagnoser(request.ProgramID)
	l.plans[id] = selectedPlan{revision: l.programRevision(request.ProgramID), diagnoser: diagnoser, targetFingerprints: append([]string(nil), request.FindingFingerprints...), id: id, programID: request.ProgramID, bindings: cloneProgramBindings(request.Bindings), plan: plan, snapshot: viewSnapshot(view)}
	response.PlanID, response.Patches = id, preview.Patches
	response.Summary = fmt.Sprintf("%s: %d dry-run patches", id, len(preview.Patches))
}

func (l *ResponderLoop) apply(ctx context.Context, request ToolRequest, response *ToolResponse) {

	for _, entry := range l.applicationsLog {
		for _, consumed := range entry.PlanIDs {
			for _, requested := range request.PlanIDs {
				if consumed == requested {
					response.ApplicationID = entry.ID
					response.Error = "prepared plan already consumed by " + entry.ID
					response.Unresolved = l.loopsRemaining() == 0
					return
				}
			}
		}
	}
	if !request.stage && l.loopsRemaining() == 0 {
		response.Error = "response loop budget exhausted"
		response.Unresolved = true
		return
	}
	if len(request.PlanIDs) == 0 {
		response.Error = "apply requires at least one plan ID"
		return
	}
	batch := program.Plan{}
	selected := make([]selectedPlan, 0, len(request.PlanIDs))
	for _, id := range request.PlanIDs {
		plan, ok := l.plans[id]
		if !ok {
			response.Error = fmt.Sprintf("unknown plan %q", id)
			return
		}
		selected = append(selected, plan)
		batch.Actions = append(batch.Actions, plan.plan.Actions...)
	}

	refs, refErr := l.menuPlanWriteRefs(batch)
	if l.heldPendingWrites() > 0 && refErr != nil {
		l.refuseSelection(response, "write_set_unknown", refErr.Error())
		return
	}
	if l.pendingConflict(ctx, refs, response) {
		return
	}
	// A missing capture is an evidence gap; it must not block the repair. The
	// automatic stage plans and applies in one step with nothing written
	// between, so its plan's snapshot is the pre-write state; an agent may
	// have written between its plan and apply, so its apply reads afresh.
	var preWrite sdk.DetectionContext
	if request.stage && len(selected) == 1 {
		preWrite = selected[0].snapshot
	}
	endPreApply := l.phase("preapply_capture")
	_ = l.capturePreApply(ctx, preWrite, planCaptureRefs(batch)...)
	endPreApply()
	l.applySeq++
	if !request.stage {
		l.spendLoop()
	}
	started := l.clock().UTC()
	id := fmt.Sprintf("application-%s-%d", l.sessionID, l.applySeq)
	programIDs := make([]string, 0, len(selected))
	for _, plan := range selected {
		programIDs = append(programIDs, plan.programID)
	}
	entry := RecordedPlanApplication{ID: id, ProgramIDs: programIDs, PlanIDs: append([]string(nil), request.PlanIDs...), Result: PlanApplicationResult{Plan: batch}, StartedAt: started, Pending: true}
	endCheckout := l.phase("checkout_state")
	entry.RunningCommit, entry.RunningDirty, _ = l.checkoutState(ctx, request.stage, request.Command)
	endCheckout()
	if l.applicationBindings == nil {
		l.applicationBindings = map[string][]recoveryBinding{}
	}
	actionStart := 0
	for _, plan := range selected {
		l.applicationBindings[id] = append(l.applicationBindings[id], recoveryBinding{ProgramID: plan.programID, ProgramRevision: plan.revision, Bindings: cloneProgramBindings(plan.bindings), FindingFingerprints: append([]string(nil), plan.targetFingerprints...), ActionStart: actionStart, ActionCount: len(plan.plan.Actions)})
		actionStart += len(plan.plan.Actions)
	}
	l.setApplicationsLog(append(l.applicationsLog, entry))
	endPersist := l.phase("persist_application")
	persistErr := l.persistToolState(ctx)
	endPersist()
	if persistErr != nil {
		// Nothing was written: leave no started record behind.
		l.setApplicationsLog(l.applicationsLog[:len(l.applicationsLog)-1])
		delete(l.applicationBindings, id)
		response.Error = persistErr.Error()
		return
	}
	executor := l.Executor
	executor.ApplicationID = id
	// Both automatic and responder applications observe once. A rollout may
	// depend on another matched binding that has not been written yet.
	executor.PollRollouts = true
	if request.stage && request.rolloutTimeout > 0 {
		executor.RolloutTimeout = request.rolloutTimeout
	}
	executor.batchVerify = request.stage && request.batchVerify
	endExecutor := l.phase("executor_apply")
	result, err := executor.Apply(ctx, l.mitigationIncident(selected), batch, false)
	endExecutor()
	response.ActionReports = result.ActionReports
	completed := l.clock().UTC()
	if result.RolloutPending != nil {
		l.reservePending(id, result, "", "", program.Bindings{}, request.FindingFingerprints)
	}
	if result.Applied || result.PendingAction != nil {
		l.applications[id] = result
		response.ApplicationID = id
	}
	if err != nil {
		response.Error = err.Error()
	}
	response.RolloutPending = result.RolloutPending
	if request.batchVerify {
		if result.Applied && result.RolloutPending == nil {
			if l.batchVerified == nil {
				l.batchVerified = map[string]bool{}
			}
			l.batchVerified[id] = err == nil
		}
		result.VerificationPassed = false
	} else if result.RolloutPending == nil {
		endVerify := l.phase("verify")
		l.verify(ctx, selected, response, request.stage)
		endVerify()
		// apply verifies its own batch: one complete act→verify cycle.
		if !request.stage && response.Error == "" {
			l.closeLoop()
		}
	}
	// The application is recorded as observed and left in place; whether to roll
	// it back, apply again or edit is the responder's decision.
	if result.Applied && result.RolloutPending == nil && !request.batchVerify {
		// Application credit is binding-scoped; closure still independently
		// requires global health. A different open fault is not a failed repair.
		result.VerificationPassed = err == nil && response.Error == "" && result.VerificationCompleted &&
			boundPlansNoMatch(selected, response.ProgramRuns) &&
			targetedFindingsClear(l.mitigationIncident(selected).Findings, response.Findings) &&
			result.VerificationPassed
		// Observation is not execution failure. Typed immutable action reports
		// and rollback facts persist with the application; recovery settlement
		// owns later credit. Do not publish a second health-as-failure signal.

	}
	if response.Error != "" {
		response.Healthy = false
	}
	entry.Result = result
	entry.CompletedAt = completed
	entry.Pending = planApplicationPending(result)
	entry.RolledBack = result.RolledBack
	entry.RollbackError = result.RollbackError
	// The application's error is its execution's. A failed observation in the
	// verification above (an unavailable detector read, a snapshot error) stays
	// on the response and its detector states: the write's outcome is not yet
	// observed, which recovery settlement keeps observing, not a failed write.
	entry.Error = ""
	if err != nil {
		entry.Error = err.Error()
	}
	if result.Applied || result.PendingAction != nil {
		l.applications[id] = result
	}
	for i := range l.applicationsLog {
		if l.applicationsLog[i].ID == id {
			l.applicationsLog[i] = entry
		}
	}
	if response.Error == "" {
		response.Summary = fmt.Sprintf("%s applied; %d open findings", id, len(response.Findings))
	}
	response.Unresolved = l.loopsRemaining() == 0 && !response.Healthy
}

// verifyStageBatch runs the one verification of a stage turn's applications,
// each applied with batchVerify: one post-apply Diagnose per applied plan on one
// fresh view, and one detector round. Each application's verification passes
// on its own evidence: its execution, its plans' own Diagnose and its targets.
func (l *ResponderLoop) verifyStageBatch(ctx context.Context, ids []string, response *ToolResponse) {
	var selected []selectedPlan
	plansOf := map[string][]selectedPlan{}
	for _, entry := range l.applicationsLog {
		if !slices.Contains(ids, entry.ID) || entry.Result.RolloutPending != nil || !entry.Result.Applied {
			continue
		}
		for _, planID := range entry.PlanIDs {
			if plan, ok := l.plans[planID]; ok {
				plansOf[entry.ID] = append(plansOf[entry.ID], plan)
				selected = append(selected, plan)
			}
		}
	}
	endVerify := l.phase("verify")
	l.verify(ctx, selected, response, true)
	endVerify()
	for i := range l.applicationsLog {
		entry := &l.applicationsLog[i]
		executed, ok := l.batchVerified[entry.ID]
		if !ok || !slices.Contains(ids, entry.ID) {
			continue
		}
		delete(l.batchVerified, entry.ID)
		plans := plansOf[entry.ID]
		entry.Result.VerificationCompleted = response.Error == ""
		entry.Result.VerificationPassed = executed && response.Error == "" && len(plans) > 0 &&
			boundPlansNoMatch(plans, response.ProgramRuns) &&
			targetedFindingsClear(l.mitigationIncident(plans).Findings, response.Findings)
		if _, live := l.applications[entry.ID]; live {
			l.applications[entry.ID] = entry.Result
		}
	}
}

// verify observes fresh findings. stage is true for the automatic stage's own
// observations, which are not the agent's ad hoc fixes.
func (l *ResponderLoop) verify(ctx context.Context, applied []selectedPlan, response *ToolResponse, stage bool) {
	previous := append([]sdk.Finding(nil), l.findings...)
	view, err := l.snapshotView(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	for _, plan := range applied {
		if l.Programs == nil {
			break
		}
		p, ok := plan.diagnoser, plan.diagnoser != nil
		if !ok {
			p, ok = l.Programs.Diagnoser(plan.programID)
		}
		if !ok {
			continue
		}
		started := l.clock()
		verdict := program.RunDiagnose(ctx, p, view, plan.bindings, l.deadline())
		run := ProgramRun{ProgramRevision: plan.revision, ProgramID: plan.programID, Bindings: plan.bindings, Verdict: verdict, Duration: l.clock().Sub(started)}
		response.ProgramRuns = append(response.ProgramRuns, run)
		l.runs = append(l.runs, run)
	}
	findings := make([]sdk.Finding, 0)
	states := make([]DetectorEvaluation, 0, len(l.Detectors))
	endDetectors := l.phase("detectors")
	results := evaluateAdmittedDetectors(ctx, l.Detectors, view)
	endDetectors()
	for _, detector := range l.Detectors {
		result := results[detector.Spec().ID]
		observed, err := result.findings, result.err
		state := DetectorEvaluation{DetectorID: detector.Spec().ID, EvaluatedAt: l.clock().UTC(), Status: DetectorEvaluationClear, Reason: detectorConfirmationReason(detector, view)}
		if err == nil && len(observed) == 0 && !detectorConfirmed(detector, view) {
			state.Status = DetectorEvaluationPending
		}
		var refused findingAdmissionError
		if err != nil {
			state.Status, state.Error = DetectorEvaluationError, err.Error()
			// A refused finding fails its detector, not the write.
			if !errors.As(err, &refused) {
				response.Error = err.Error()
			}
		}
		for _, finding := range observed {
			if finding.Status == sdk.FindingActive {
				findings = append(findings, finding)
				state.Status = DetectorEvaluationFiring
			}
		}
		states = append(states, state)
	}
	l.drift = retainedDrift(l.Request.Findings, view)
	l.findings = findings
	// Typed diagnoses and findings retain observation history. They are not
	// durable execution failures; only actual ad hoc work needs a novelty flag.
	if len(applied) == 0 && !stage && !l.appliedAny() && len(previous) > 0 && len(findings) == 0 {
		matched := false
		for _, run := range l.runs {
			if run.Verdict.Result == program.Match {
				matched = true
				break
			}
		}
		if matched {
			l.addFailureSignal("adhoc_fixed_matched_program")
		} else {
			l.addFailureSignal("adhoc_coverage_gap")
		}
	}
	response.Findings, response.DetectorStates = append([]sdk.Finding(nil), findings...), states
	response.Drift = append([]sdk.Finding(nil), l.drift...)
	if len(findings) > 0 {
		runs := l.selectCandidates(ctx, view, findings, false)
		l.stampProgramRuns(runs)
		response.ProgramRuns = append(response.ProgramRuns, runs...)
		l.runs = append(l.runs, runs...)
	}
	response.Healthy = response.Error == "" && len(findings) == 0 && (len(l.Detectors) > 0 || hasRetainedDrift(l.Request.Findings)) && detectorsConfirmed(l.Detectors, view)
	response.Unresolved = l.loopsRemaining() == 0 && !response.Healthy
	if response.Error == "" {
		response.Summary = fmt.Sprintf("verification: %d open findings", len(findings)) + pendingEvaluationSummary(states)
		switch {
		case response.Healthy && len(response.Drift) > 0:
			response.Summary = fmt.Sprintf("verification: health is clear, but %d object(s) still differ from the accepted source; restore them or explain why they should differ, then return the structured incident result", len(response.Drift))
		case response.Healthy:
			response.Summary = "verification: healthy; return the structured incident result immediately"
		}
	}
}

func (l *ResponderLoop) addFailureSignal(signal string) {
	for _, existing := range l.failureSignals {
		if existing == signal {
			return
		}
	}
	l.failureSignals = append(l.failureSignals, signal)
}

// Every selected binding needs a conclusive post-apply diagnosis.
func boundPlansNoMatch(selected []selectedPlan, runs []ProgramRun) bool {
	for _, plan := range selected {
		clear := false
		for _, run := range runs {
			if run.ProgramID != plan.programID || !run.Bindings.Equal(plan.bindings) {
				continue
			}
			if run.Verdict.Result != program.NoMatch {
				return false
			}
			clear = true
		}
		if !clear {
			return false
		}
	}
	return true
}

func (l *ResponderLoop) rollbackApplication(ctx context.Context, request ToolRequest, response *ToolResponse) {
	application, ok := l.applications[request.ApplicationID]
	if !ok {
		response.Error = fmt.Sprintf("unknown application %q", request.ApplicationID)
		return
	}
	if l.pendingConflict(ctx, planCaptureRefs(application.Plan), response) {
		return
	}
	executor := l.Executor
	reconciled, err := executor.Reconcile(ctx, application)
	if err != nil {
		response.Error = err.Error()
		return
	}
	application = reconciled
	rollbackErr := executor.rollback(ctx, &application)
	if rollbackErr != nil {
		application.RollbackError = rollbackErr.Error()
	}
	l.applications[request.ApplicationID] = application
	l.updateMenuRollback(request.ApplicationID, application)
	for i := range l.applicationsLog {
		if l.applicationsLog[i].ID == request.ApplicationID {
			l.applicationsLog[i].Result = application
			l.applicationsLog[i].Pending = planApplicationPending(application)
			l.applicationsLog[i].RolledBack = application.RolledBack
			l.applicationsLog[i].RollbackError = application.RollbackError
		}
	}
	if rollbackErr != nil {
		response.Error = rollbackErr.Error()
		return
	}
	l.verify(ctx, nil, response, request.stage)
	if response.Error == "" {
		response.Summary = "application rolled back and verified"
	}
}

// appliedAny reports whether any plan, from the stage or the agent, has been
// applied: later plans must then be built from a fresh view.
func (l *ResponderLoop) appliedAny() bool { return l.batches > 0 || l.applySeq > 0 }

// Menu rounds and provider continuations have separate budgets. Advertise and
// enforce the same incident-wide write-batch limit used by menu execution.
func (l *ResponderLoop) batchLimit() int {
	if l.MenuConfig != nil {
		return min(5, l.MaxBatches)
	}
	return l.MaxBatches
}

// spendLoop charges a write to the open act→verify cycle, opening one if the
// last verification closed it. Every write path calls it only once a write is
// about to run, so a call that writes nothing spends no loop.
func (l *ResponderLoop) spendLoop() {
	if !l.loopOpen {
		l.batches++
		l.loopOpen = true
	}
}

// closeLoop ends the open cycle: a verification observed its writes.
func (l *ResponderLoop) closeLoop() { l.loopOpen = false }

// loopsRemaining counts the cycles that may still write, including the open one.
func (l *ResponderLoop) loopsRemaining() int {
	remaining := max(0, l.batchLimit()-l.batches)
	if l.loopOpen {
		remaining++
	}
	return remaining
}

// clock shares the injected clock across diagnosis, apply receipts and timing.
func (l *ResponderLoop) clock() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	if l.Executor.Now != nil {
		return l.Executor.Now()
	}
	return lifecycleio.Now()
}

func (l *ResponderLoop) gitRead() gitRead {
	if l.GitRead != nil {
		return l.GitRead
	}
	return lifecycleio.GitOutput
}

// hasWrites reports whether the responder has changed anything since the incident opened: a plan application
// of this loop, or a write in the recording of the agent's Kubernetes calls. A recording that cannot be read
// yet counts as a write, so the wait is only skipped when none is certain.
func (l *ResponderLoop) hasWrites() bool {
	if len(l.applicationsLog) > 0 || l.ExternalReceipts == nil {
		return true
	}
	receipts, pending, _, err := l.ExternalReceipts()
	return err != nil || pending || len(receipts) > 0
}

// rejectedReceipt records a mutation SDO refused before it reached the cluster.
func (l *ResponderLoop) rejectedReceipt(command string, response ToolResponse, started time.Time) *contracts.RepairActionReceipt {
	return &contracts.RepairActionReceipt{ActionId: "sdo-rejected-" + l.sessionID, Kind: command, Target: "incident/" + l.Request.IncidentID, FailureReason: response.FailureReason, Summary: "incident is closed", Details: "refused before any write: " + response.Error, StartedAt: contractTimestamp(started.UTC()), CompletedAt: contractTimestamp(l.clock().UTC())}
}
