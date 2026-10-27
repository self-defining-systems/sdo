package runtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sdo.dev/operator/memory/layout"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const DefaultMaxResponseLoops = 10
const maxRecordedToolResponse = 2048

// maxRecordedApplyResponse bounds an apply record that lists the runs of the
// applied plans. The agent records at most MaxBatches (5) applies; the automatic
// stage adds one apply per pass over all its matched plans, and a pass never
// retries a binding it already attempted, so a restart adds at most one more.
// That is a handful of applies at 8 KiB (tens of KiB); the other tool calls are
// 2 KiB each, well under 100 of them (200 KiB). Together they stay far inside
// the 768 KiB state ConfigMap budget. A compact run is about 250 B, so 8 KiB holds about 30 runs,
// several per applied plan.
const maxRecordedApplyResponse = 8192

type ToolRequest struct {
	SelectionSet        *contracts.MitigationSelectionSet      `json:"-"`
	WaitSeconds         float64                                `json:"wait_seconds,omitempty"`
	PreviousFindings    []sdk.Finding                          `json:"previous_findings,omitempty"`
	FindingFingerprints []string                               `json:"finding_fingerprints,omitempty"`
	DetectorIDs         []string                               `json:"detector_ids,omitempty"`
	Refs                []sdk.ObjectRef                        `json:"refs,omitempty"`
	Parameters          map[string]string                      `json:"parameters,omitempty"`
	Selection           *contracts.MitigationSelection         `json:"-"`
	ReportQuery         *contracts.MitigationReportQuery       `json:"-"`
	Checkpoint          *contracts.MitigationSessionCheckpoint `json:"-"`
	Command             string                                 `json:"command"`
	Ref                 *sdk.ObjectRef                         `json:"ref,omitempty"`
	ProgramID           string                                 `json:"program_id,omitempty"`
	Match               bool                                   `json:"match,omitempty"`
	Bindings            program.Bindings                       `json:"bindings,omitempty"`
	PlanIDs             []string                               `json:"plan_ids,omitempty"`
	ApplicationID       string                                 `json:"application_id,omitempty"`
	// stage marks an apply made by the automatic response stage. It is not
	// decodable from a tool call, so only the stage can set it.
	stage bool
}

type ToolResponse struct {
	SelectionSchema            *MenuSchema                    `json:"selection_schema,omitempty"`
	SelectionSetSchema         *MenuSchema                    `json:"selection_set_schema,omitempty"`
	ProgramRevisions           map[string]string              `json:"program_revisions,omitempty"`
	OptionReports              []*contracts.MitigationReport  `json:"-"`
	UnexplainedRemovalBlockers []UnexplainedRemovalBlocker    `json:"unexplained_removal_blockers,omitempty"`
	Drift                      []sdk.Finding                  `json:"drift,omitempty"`
	BeforeFindings             []sdk.Finding                  `json:"before_findings"`
	ControllerFindings         []sdk.Finding                  `json:"controller_findings"`
	Progress                   *FindingProgress               `json:"progress,omitempty"`
	IncidentVerified           *apihistory.IncidentVerified   `json:"incident_verified,omitempty"`
	FailureReason              string                         `json:"failure_reason,omitempty"`
	Receipt                    *contracts.RepairActionReceipt `json:"-"`
	MitigationRoundsUsed       int                            `json:"mitigation_rounds_used"`
	MitigationRoundsAllowed    int                            `json:"mitigation_rounds_allowed"`
	ResponseLoopsUsed          int                            `json:"response_loops_used"`
	ResponseLoopsAllowed       int                            `json:"response_loops_allowed"`
	MitigationHealth           *MitigationHealth              `json:"mitigation_health,omitempty"`
	RolloutPending             *contracts.RolloutPending      `json:"rollout_pending,omitempty"`
	MenuExecution              []MenuRoundExecution           `json:"-"`
	MenuParameters             map[string][]MenuParameter     `json:"-"`
	Menu                       *contracts.MitigationMenu      `json:"-"`
	Report                     *contracts.MitigationReport    `json:"-"`
	Session                    *contracts.MitigationSession   `json:"-"`
	Command                    string                         `json:"command"`
	Source                     map[string]any                 `json:"source,omitempty"`
	Error                      string                         `json:"error,omitempty"`
	Summary                    string                         `json:"summary"`
	PlanID                     string                         `json:"plan_id,omitempty"`
	ApplicationID              string                         `json:"application_id,omitempty"`
	Patches                    []PlanPatch                    `json:"patches,omitempty"`
	ProgramRuns                []ProgramRun                   `json:"program_runs,omitempty"`

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
}

type selectedPlan struct {
	targetFingerprints []string
	revision           string
	diagnoser          program.Diagnoser
	id                 string
	programID          string
	bindings           program.Bindings
	plan               program.Plan
}

// ResponderLoop holds one incident's tool state while its responder Job runs.
type ResponderLoop struct {
	stageProgress        map[string]responseStageProgress
	waitForStageRollouts bool
	// WorkingProgramRoot is set by the production loop factory to the mounted
	// proposal checkout. Embedders with an in-memory registry may leave it empty.
	WorkingProgramRoot string
	workingPrograms    []*workingProgram
	pendingWrites      map[string]pendingMitigation
	MutationGate       *apihistory.MutationGate
	Now                func() time.Time
	CachedFindings     func() ToolResponse
	IncidentStatus     func() *apihistory.IncidentVerified
	WaitClosure        func(context.Context, time.Duration) ToolResponse
	ExternalReceipts   func() ([]RepairActionReceipt, bool, string, error)
	gateError          error
	closureReceipts    []RepairActionReceipt
	continuation       responderContinuation
	MenuConfig         *ResponderMenuConfig
	menu               *contracts.MitigationMenu
	menuEntries        menuCatalog
	menuSession        *contracts.MitigationSession
	menuReports        []*contracts.MitigationReport
	menuExecutions     map[string][]MenuRoundExecution
	menuRepairs        []RepairActionReceipt
	VerifyClosure      func(context.Context) ToolResponse
	VerifyResult       func(context.Context, IncidentResult) ToolResponse
	CurrentEvidence    func() *IncidentRequest
	Request            IncidentRequest
	Programs           program.ProgramRegistry
	Provider           SnapshotProvider
	Detectors          []sdk.Detector
	Executor           PlanExecutor
	ReadView           func(sdk.DetectionContext) program.ReadView
	DiagnosisDeadline  time.Duration
	// ProgramFollowUpSettle bounds each wait before a program follow-up plan;
	// PendingStallTimeout bounds how long a pending application holds its writes.
	ProgramFollowUpSettle time.Duration
	PendingStallTimeout   time.Duration
	MaxBatches            int
	Trust                 map[string]ProgramTrust
	CorpusSessionRoot     string
	capturePhase          string
	sourceCompared        map[string]bool
	timing                toolTiming
	preApplyCaptured      bool
	saveToolState         func(context.Context, []byte) error
	operationMu           sync.Mutex
	mu                    sync.Mutex
	sessionID             string
	plans                 map[string]selectedPlan
	applications          map[string]PlanApplicationResult
	applicationsLog       []RecordedPlanApplication
	completedRepairs      []continuationRepair
	failureSignals        []string
	calls                 []ToolCall
	runs                  []ProgramRun
	matchedViews          map[string]program.ReadView
	matchedPlans          map[string]PlanApplicationResult
	matchedPlanErrors     map[string]error
	lastDiagnosisView     program.ReadView
	findings              []sdk.Finding
	drift                 []sdk.Finding
	// batches counts the agent's mitigation rounds, the budget the agent is
	// told. applySeq numbers every apply, including the automatic stage's, which
	// never spends that budget.
	batches  int
	applySeq int
}

func NewResponderLoop(request IncidentRequest, programs program.ProgramRegistry, provider SnapshotProvider, detectors []sdk.Detector, executor PlanExecutor, maxBatches int) *ResponderLoop {
	if maxBatches <= 0 {
		maxBatches = DefaultMaxResponseLoops
	}
	session := make([]byte, 16)
	if _, err := rand.Read(session); err != nil {
		panic(err)
	}
	loop := &ResponderLoop{sessionID: fmt.Sprintf("%x", session), Request: request, Programs: programs, Provider: provider, Detectors: detectors, Executor: executor, MaxBatches: maxBatches, plans: map[string]selectedPlan{}, applications: map[string]PlanApplicationResult{}, matchedViews: map[string]program.ReadView{}, matchedPlans: map[string]PlanApplicationResult{}, findings: append([]sdk.Finding(nil), request.Findings...)}
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
	return loop
}

// initializeDispatch may run after LoopFactory has published this loop to the
// controller. It follows the same operation/state lock order as tools/restore.
func (l *ResponderLoop) initializeDispatch(trust map[string]ProgramTrust, maxBatches int, runs []ProgramRun) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Trust = trust
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

func (l *ResponderLoop) observeMutations() (*apihistory.MutationObservation, error) {
	var gate *apihistory.MutationGate
	l.locked(func() { gate = l.MutationGate })
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
func (l *ResponderLoop) PlanApplications() []RecordedPlanApplication {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RecordedPlanApplication(nil), l.applicationsLog...)
}
func (l *ResponderLoop) FailureSignals() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.failureSignals...)
}

func (l *ResponderLoop) InitialDiagnose(ctx context.Context) []ProgramRun {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.capturePhase = "open@detect"
	defer func() { l.capturePhase = "" }()
	return l.diagnose(ctx, l.findings)
}

func (l *ResponderLoop) LoopsUsed() int { l.mu.Lock(); defer l.mu.Unlock(); return l.batches }
func (l *ResponderLoop) Exhausted() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.batches >= l.batchLimit() && len(l.findings) > 0
}

// toolTiming accumulates the phase durations of one Handle call. capture is
// the time spent in snapshotView (provider snapshot, corpus capture, routing
// probes) and refresh is the time spent in refresh, which includes capture.
type toolTiming struct{ capture, refresh, persist time.Duration }

func (l *ResponderLoop) Handle(ctx context.Context, request ToolRequest) (response ToolResponse) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	started := time.Now()
	mutating := request.Command == "rollback" || request.Command == "apply" || request.Command == "mitigate" || request.Command == "act-result"
	var before []sdk.Finding
	if mutating && l.CachedFindings != nil {
		before = l.CachedFindings().Findings
	}
	l.timing = toolTiming{}
	defer func() {
		slog.Info("responder_tool_timing", "incident_id", l.Request.IncidentID, "command", request.Command,
			"capture_ms", l.timing.capture.Milliseconds(), "refresh_ms", l.timing.refresh.Milliseconds(),
			"persist_ms", l.timing.persist.Milliseconds(), "total_ms", time.Since(started).Milliseconds())
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
		if l.IncidentStatus != nil {
			response.IncidentVerified = l.IncidentStatus()
		}
		if response.IncidentVerified == nil && l.MutationGate != nil {
			response.IncidentVerified, _ = l.MutationGate.Verified()
		}
		response.MitigationRoundsUsed = l.batches
		response.MitigationRoundsAllowed = l.batchLimit()
		if response.Error != "" {
			response.Summary = response.Error
		}
		requestJSON, _ := json.Marshal(request)
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
		responseJSON := recordedToolResponseJSON(response, applied)
		l.calls = append(l.calls, ToolCall{Command: request.Command, At: time.Now().UTC(), RequestJSON: string(requestJSON), ResponseJSON: string(responseJSON), Error: response.Error, Summary: response.Summary})
		persistStarted := time.Now()
		err := l.persistToolState(ctx)
		l.timing.persist = time.Since(persistStarted)
		if err != nil {
			response.Error = err.Error()
			response.Summary = response.Error
		}
	}()
	response.DriftEvents = append([]sdk.DriftEvent(nil), l.Request.DriftEvents...)

	if request.Command == "apply" || request.Command == "mitigate" || request.Command == "rollback" || request.Command == "pre-apply" {
		if l.gateError != nil {
			response.Error = l.gateError.Error()
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
				response.Receipt = &contracts.RepairActionReceipt{FailureReason: response.FailureReason, ActionId: "sdo-rejected-" + l.sessionID, Kind: request.Command, Summary: "incident is closed", StartedAt: contractTimestamp(started.UTC()), CompletedAt: contractTimestamp(time.Now().UTC())}
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
			response.Receipt = &contracts.RepairActionReceipt{FailureReason: "incident_verified", ActionId: "sdo-rejected-" + l.sessionID, Kind: request.Command, Summary: "incident is closed", StartedAt: contractTimestamp(started.UTC()), CompletedAt: contractTimestamp(time.Now().UTC())}
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
		if err := l.loadSelectedWorkingPrograms(ctx, request); err != nil {
			response.Error = err.Error()
			return response
		}
	}
	switch request.Command {
	case "menu":
		l.listMenu(ctx, request, &response)
	case "mitigate":
		if err := l.validateFindingTargets(request.FindingFingerprints); err != nil {
			response.Error = err.Error()
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
		l.detect(ctx, request.DetectorIDs, &response)
	case "status":
		response.DriftEvents = append([]sdk.DriftEvent(nil), l.Request.DriftEvents...)
		response.Trust = l.Trust
		response.Findings, response.ProgramRuns, response.DetectorStates, response.Error = l.refresh(ctx)
		response.Drift = append([]sdk.Finding(nil), l.drift...)
		response.Healthy = response.Error == "" && len(l.findings) == 0
		for _, state := range response.DetectorStates {
			response.Healthy = response.Healthy && state.Status == DetectorEvaluationClear
		}
		response.Unresolved = l.batches >= l.batchLimit() && !response.Healthy
		response.Summary = fmt.Sprintf("%d open findings; %d of %d apply batches used", len(l.findings), l.batches, l.batchLimit()) + pendingEvaluationSummary(response.DetectorStates)
	case "diagnose":
		if request.ProgramID == "" {
			response.Findings, response.ProgramRuns, response.DetectorStates, response.Error = l.refresh(ctx)
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
			view, err := l.snapshotView(ctx)
			if err != nil {
				response.Error = err.Error()
				break
			}
			started := time.Now()
			verdict := program.RunDiagnose(ctx, p, view, request.Bindings, l.deadline())
			_, canMitigate := l.Programs.Mitigator(request.ProgramID)
			run := ProgramRun{ProgramRevision: l.programRevision(request.ProgramID), CanMitigate: &canMitigate, ProgramID: request.ProgramID, Bindings: request.Bindings, Verdict: verdict, Duration: time.Since(started)}
			response.ProgramRuns = []ProgramRun{run}
			l.runs = append(l.runs, run)
		}
		response.Summary = fmt.Sprintf("%d program diagnoses", len(response.ProgramRuns))
	case "plan":
		l.plan(ctx, request, &response)
	case "source":
		if request.Ref == nil || l.Executor.Source == nil || l.Request.SourceCommit == "" {
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
			if _, err := l.snapshotView(ctx); err != nil {
				response.Error = err.Error()
				break
			}
			// An object without an acceptance record compares as unknown;
			// accepted source itself is still shown.
			l.recordSourceComparisons(ctx, l.capturePhase, *request.Ref)
		}
		object, err := acceptedIncidentManifest(ctx, l.Executor.Source, l.Request.SourceCommit, l.Request.Namespace, *request.Ref)
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
	case "pre-apply":
		if l.pendingConflict(request.Refs, &response) {
			break
		}
		if l.CachedFindings != nil {
			response.Findings = l.CachedFindings().Findings
		}
		if err := l.capturePreApply(ctx, request.Refs...); err != nil {
			response.Error = err.Error()
			break
		}
		response.Summary = "first-mutation snapshot captured"
	case "verify":
		if l.VerifyClosure != nil {
			// The controller may persist continuation state while observing.
			// A read-only closure check must not hold responder locks while
			// waiting for that observation to finish.
			var closure ToolResponse
			if err := withoutResponderLocks(l, "verify", true, func() {
				if request.WaitSeconds < 0 || request.WaitSeconds > 86400 {
					closure = ToolResponse{Command: "verify", Error: "wait seconds must be between 0 and 86400"}
				} else if request.WaitSeconds > 0 && l.WaitClosure != nil {
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
			l.verify(ctx, nil, &response)
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
		ToolResponse: ToolResponse{Command: response.Command, Summary: boundedToolText(response.Summary), Error: boundedToolText(response.Error), PlanID: response.PlanID, ApplicationID: response.ApplicationID, LoopsUsed: response.LoopsUsed, LoopsAllowed: response.LoopsAllowed, Healthy: response.Healthy, TargetedHealthy: response.TargetedHealthy, Unresolved: response.Unresolved, RolloutPending: compactRolloutPending(response.RolloutPending)},
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
	for {
		responseJSON, _ = json.Marshal(record)
		if len(responseJSON) <= limit || len(record.ProgramRuns) == 0 {
			return responseJSON
		}
		record.ProgramRuns = record.ProgramRuns[:len(record.ProgramRuns)-1]
		record.ProgramRunsOmitted++
	}
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

func (l *ResponderLoop) snapshotView(ctx context.Context) (program.ReadView, error) {
	started := time.Now()
	defer func() { l.timing.capture += time.Since(started) }()
	if l.Provider == nil {
		return nil, errors.New("snapshot provider is unavailable")
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if l.ReadView == nil {
		return nil, errors.New("live read view is unavailable")
	}
	if l.CorpusSessionRoot != "" && l.capturePhase != "" {
		if err := memory.CaptureIncidentSnapshot(l.CorpusSessionRoot, l.Request.IncidentID, l.capturePhase, snapshot, l.Request.Namespace, findingCaptureRefs(l.Request.Findings)...); err != nil {
			return nil, err
		}
	}
	// Captures are immutable historical evidence. Every operational diagnosis,
	// including a resumed initial diagnosis, uses this attempt's fresh snapshot.
	view := NewCycleReadView(l.ReadView(snapshot))
	if l.CorpusSessionRoot != "" && l.capturePhase != "" {
		phase := l.capturePhase
		return &sdktest.RecordingReadView{ReadView: view, OnRecord: func(call sdktest.PrimitiveCall) {
			_ = memory.AppendCapturedRecording(l.CorpusSessionRoot, l.Request.IncidentID, phase, call)
		}}, nil
	}
	return view, nil
}

func (l *ResponderLoop) diagnose(ctx context.Context, findings []sdk.Finding) []ProgramRun {
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	runs := SelectProgramCandidates(ctx, l.Programs, view, findings, l.deadline())
	l.stampProgramRuns(runs)
	l.rememberMatchedViews(ctx, runs, view)
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	if l.capturePhase != "" {
		l.recordSourceComparisons(ctx, l.capturePhase, findingCaptureRefs(findings)...)
	}
	return runs
}

func (l *ResponderLoop) refresh(ctx context.Context) ([]sdk.Finding, []ProgramRun, []DetectorEvaluation, string) {
	started := time.Now()
	defer func() { l.timing.refresh += time.Since(started) }()
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil, nil, nil, err.Error()
	}
	findings := make([]sdk.Finding, 0)
	states := make([]DetectorEvaluation, 0, len(l.Detectors))
	for _, detector := range l.Detectors {
		observed, err := sdk.DetectObserved(ctx, detector, view)
		state := DetectorEvaluation{DetectorID: detector.Spec().ID, EvaluatedAt: l.clock().UTC(), Status: DetectorEvaluationClear, Reason: detectorConfirmationReason(detector, view)}
		if err == nil && len(observed) == 0 && !detectorConfirmed(detector, view) {
			state.Status = DetectorEvaluationPending
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
	runs := SelectProgramCandidates(ctx, l.Programs, view, findings, l.deadline())
	l.stampProgramRuns(runs)
	l.rememberMatchedViews(ctx, runs, view)
	l.lastDiagnosisView = view
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
			return
		}
	}
	if err := plan.Validate(); err != nil {
		response.Error = err.Error()
		return
	}
	if len(plan.Actions) == 0 {
		response.Error = "program returned an empty plan"
		return
	}
	preview, err := l.Executor.Apply(ctx, l.Request, plan, true)
	if err != nil {
		response.Error = err.Error()
		return
	}
	id := fmt.Sprintf("plan-%s-%d", l.sessionID, len(l.plans)+1)
	diagnoser, _ := l.Programs.Diagnoser(request.ProgramID)
	l.plans[id] = selectedPlan{revision: l.programRevision(request.ProgramID), diagnoser: diagnoser, targetFingerprints: append([]string(nil), request.FindingFingerprints...), id: id, programID: request.ProgramID, bindings: cloneProgramBindings(request.Bindings), plan: plan}
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
					response.Unresolved = l.batches >= l.batchLimit()
					return
				}
			}
		}
	}
	if !request.stage && l.batches >= l.batchLimit() {
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
	if l.pendingConflict(refs, response) {
		return
	}
	// A missing capture is an evidence gap; it must not block the repair.
	_ = l.capturePreApply(ctx, planCaptureRefs(batch)...)
	l.applySeq++
	if !request.stage {
		l.batches++
	}
	started := time.Now().UTC()
	id := fmt.Sprintf("application-%s-%d", l.sessionID, l.applySeq)
	programIDs := make([]string, 0, len(selected))
	for _, plan := range selected {
		programIDs = append(programIDs, plan.programID)
	}
	entry := RecordedPlanApplication{ID: id, ProgramIDs: programIDs, PlanIDs: append([]string(nil), request.PlanIDs...), Result: PlanApplicationResult{Plan: batch}, StartedAt: started, Pending: true}
	l.applicationsLog = append(l.applicationsLog, entry)
	if err := l.persistToolState(ctx); err != nil {
		response.Error = err.Error()
		return
	}
	executor := l.Executor
	executor.ApplicationID = id
	executor.PollRollouts = !l.waitForStageRollouts
	result, err := executor.Apply(ctx, l.mitigationIncident(selected), batch, false)
	completed := time.Now().UTC()
	if result.RolloutPending != nil {
		l.reservePending(id, batch, "", "", program.Bindings{}, request.FindingFingerprints)
	}
	if result.Applied || result.PendingAction != nil {
		l.applications[id] = result
		response.ApplicationID = id
	}
	if err != nil {
		response.Error = err.Error()
	}
	response.RolloutPending = result.RolloutPending
	if result.RolloutPending == nil {
		l.verify(ctx, selected, response)
	}
	// The application is recorded as observed and left in place; whether to roll
	// it back, apply again or edit is the responder's decision.
	if result.Applied && result.RolloutPending == nil {
		// Application credit is binding-scoped; closure still independently
		// requires global health. A different open fault is not a failed repair.
		result.VerificationPassed = err == nil && response.Error == "" && result.VerificationCompleted &&
			boundPlansNoMatch(selected, response.ProgramRuns) &&
			targetedFindingsClear(l.mitigationIncident(selected).Findings, response.Findings) &&
			result.VerificationPassed
		if !result.VerificationPassed {
			// Program trust counts an unverified application against the program's
			// current revision, durably, even if the responder is restarted.
			failedRuns := make([]ProgramRun, 0, len(programIDs))
			for _, plan := range selected {
				failedRuns = append(failedRuns, ProgramRun{ProgramID: plan.programID, ProgramRevision: plan.revision})
			}
			fillProgramRevisions(ctx, failedRuns, l.Request.RepositoryWorktree, l.Request.SourceCommit)
			for _, run := range failedRuns {
				signal := "application_verification_failure:" + run.ProgramID
				if run.ProgramRevision != "" {
					signal += "@" + run.ProgramRevision
				}
				l.addFailureSignal(signal)
			}
			if persistErr := l.persistToolState(ctx); persistErr != nil {
				response.Error = persistErr.Error()
			}
		}
	}
	if response.Error != "" {
		response.Healthy = false
	}
	entry.Result = result
	entry.CompletedAt = completed
	entry.Pending = planApplicationPending(result)
	entry.RolledBack = result.RolledBack
	entry.RollbackError = result.RollbackError
	entry.Error = response.Error
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
	response.Unresolved = l.batches >= l.batchLimit() && !response.Healthy
}

func (l *ResponderLoop) verify(ctx context.Context, applied []selectedPlan, response *ToolResponse) {
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
		started := time.Now()
		verdict := program.RunDiagnose(ctx, p, view, plan.bindings, l.deadline())
		run := ProgramRun{ProgramRevision: plan.revision, ProgramID: plan.programID, Bindings: plan.bindings, Verdict: verdict, Duration: time.Since(started)}
		response.ProgramRuns = append(response.ProgramRuns, run)
		l.runs = append(l.runs, run)
		if verdict.Result == program.Match {
			l.addFailureSignal("applied_diagnosis_still_match:" + plan.programID)
		}
	}
	findings := make([]sdk.Finding, 0)
	states := make([]DetectorEvaluation, 0, len(l.Detectors))
	for _, detector := range l.Detectors {
		observed, err := sdk.DetectObserved(ctx, detector, view)
		state := DetectorEvaluation{DetectorID: detector.Spec().ID, EvaluatedAt: time.Now().UTC(), Status: DetectorEvaluationClear, Reason: detectorConfirmationReason(detector, view)}
		if err == nil && len(observed) == 0 && !detectorConfirmed(detector, view) {
			state.Status = DetectorEvaluationPending
		}
		if err != nil {
			state.Status, state.Error = DetectorEvaluationError, err.Error()
			response.Error = err.Error()
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
	if len(applied) > 0 {
		old := map[string]bool{}
		for _, finding := range previous {
			old[FindingFingerprint(finding)] = true
		}
		for _, finding := range findings {
			if !old[FindingFingerprint(finding)] && !onlyUnrelatedFindings(applied, []sdk.Finding{finding}) {
				l.addFailureSignal("new_finding_after_apply:" + FindingFingerprint(finding))
			}
		}
	} else if !l.waitForStageRollouts && !l.appliedAny() && len(previous) > 0 && len(findings) == 0 {
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
		runs := SelectProgramCandidates(ctx, l.Programs, view, findings, l.deadline())
		l.stampProgramRuns(runs)
		response.ProgramRuns = append(response.ProgramRuns, runs...)
		l.runs = append(l.runs, runs...)
	}
	response.Healthy = response.Error == "" && len(findings) == 0 && (len(l.Detectors) > 0 || hasRetainedDrift(l.Request.Findings)) && detectorsConfirmed(l.Detectors, view)
	response.Unresolved = l.batches >= l.batchLimit() && !response.Healthy
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

// Only explicit findings on resources outside the applied bindings can explain
// why global health is still red after those bound diagnoses have cleared.
func onlyUnrelatedFindings(applied []selectedPlan, findings []sdk.Finding) bool {
	if len(applied) == 0 || len(findings) == 0 {
		return false
	}
	for _, finding := range findings {
		if finding.PrimaryResource.Kind == "" || finding.PrimaryResource.Name == "" {
			return false
		}
		for _, plan := range applied {
			for _, bound := range plan.bindings.Objects {
				if sameResource(finding.PrimaryResource, bound) {
					return false
				}
				for _, ref := range finding.ParameterBindings {
					if sameResource(ref, bound) {
						return false
					}
				}
			}
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
	if l.pendingConflict(planCaptureRefs(application.Plan), response) {
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
	l.verify(ctx, nil, response)
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

func (l *ResponderLoop) clock() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}
