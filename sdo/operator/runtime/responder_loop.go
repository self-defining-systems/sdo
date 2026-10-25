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

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const DefaultMaxResponseLoops = 10
const maxRecordedToolResponse = 2048

// maxRecordedApplyResponse bounds an apply record that lists the runs of the
// applied plans. Each incident records at most MaxBatches (5) applies, so they
// cost at most 5 x 8 KiB = 40 KiB; the other tool calls are 2 KiB each, well
// under 100 of them (200 KiB). Together they stay far inside the 768 KiB state
// ConfigMap budget. A compact run is about 250 B, so 8 KiB holds about 30 runs,
// several per applied plan.
const maxRecordedApplyResponse = 8192

type ToolRequest struct {
	SelectionSet        *contracts.MitigationSelectionSet      `json:"-"`
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
	Bindings            program.Bindings                       `json:"bindings,omitempty"`
	PlanIDs             []string                               `json:"plan_ids,omitempty"`
	ApplicationID       string                                 `json:"application_id,omitempty"`
}

type ToolResponse struct {
	OptionReports           []*contracts.MitigationReport `json:"-"`
	MitigationRoundsUsed    int                           `json:"mitigation_rounds_used"`
	MitigationRoundsAllowed int                           `json:"mitigation_rounds_allowed"`
	ResponseLoopsUsed       int                           `json:"response_loops_used"`
	ResponseLoopsAllowed    int                           `json:"response_loops_allowed"`
	MitigationHealth        *MitigationHealth             `json:"mitigation_health,omitempty"`
	RolloutPending          *contracts.RolloutPending     `json:"rollout_pending,omitempty"`
	MenuExecution           []MenuRoundExecution          `json:"-"`
	MenuParameters          map[string][]MenuParameter    `json:"-"`
	Menu                    *contracts.MitigationMenu     `json:"-"`
	Report                  *contracts.MitigationReport   `json:"-"`
	Session                 *contracts.MitigationSession  `json:"-"`
	Command                 string                        `json:"command"`
	Source                  map[string]any                `json:"source,omitempty"`
	Error                   string                        `json:"error,omitempty"`
	Summary                 string                        `json:"summary"`
	PlanID                  string                        `json:"plan_id,omitempty"`
	ApplicationID           string                        `json:"application_id,omitempty"`
	Patches                 []PlanPatch                   `json:"patches,omitempty"`
	ProgramRuns             []ProgramRun                  `json:"program_runs,omitempty"`
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
	id                 string
	programID          string
	bindings           program.Bindings
	plan               program.Plan
}

// ResponderLoop holds one incident's tool state while its responder Job runs.
type ResponderLoop struct {
	pendingWrites     map[string]pendingMitigation
	continuation      responderContinuation
	MenuConfig        *ResponderMenuConfig
	menu              *contracts.MitigationMenu
	menuEntries       map[string]menuEntry
	menuSession       *contracts.MitigationSession
	menuReports       []*contracts.MitigationReport
	menuExecutions    map[string][]MenuRoundExecution
	menuRepairs       []RepairActionReceipt
	VerifyClosure     func(context.Context) ToolResponse
	VerifyResult      func(context.Context, IncidentResult) ToolResponse
	CurrentEvidence   func() *IncidentRequest
	Request           IncidentRequest
	Programs          program.ProgramRegistry
	Provider          SnapshotProvider
	Detectors         []sdk.Detector
	Executor          PlanExecutor
	ReadView          func(sdk.DetectionContext) program.ReadView
	DiagnosisDeadline time.Duration
	MaxBatches        int
	Trust             map[string]ProgramTrust
	CorpusSessionRoot string
	capturePhase      string
	timing            toolTiming
	preApplyCaptured  bool
	saveToolState     func(context.Context, []byte) error
	operationMu       sync.Mutex
	mu                sync.Mutex
	sessionID         string
	plans             map[string]selectedPlan
	applications      map[string]PlanApplicationResult
	applicationsLog   []RecordedPlanApplication
	completedRepairs  []continuationRepair
	failureSignals    []string
	calls             []ToolCall
	runs              []ProgramRun
	matchedViews      map[string]program.ReadView
	matchedPlans      map[string]PlanApplicationResult
	matchedPlanErrors map[string]error
	lastDiagnosisView program.ReadView
	findings          []sdk.Finding
	batches           int
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
		response.Findings, response.ProgramRuns, response.Error = l.refresh(ctx)
		response.Healthy = response.Error == "" && len(l.findings) == 0
		response.Unresolved = l.batches >= l.batchLimit() && !response.Healthy
		response.Summary = fmt.Sprintf("%d open findings; %d of %d apply batches used", len(l.findings), l.batches, l.batchLimit())
	case "diagnose":
		if request.ProgramID == "" {
			response.Findings, response.ProgramRuns, response.Error = l.refresh(ctx)
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
			run := ProgramRun{CanMitigate: &canMitigate, ProgramID: request.ProgramID, Bindings: request.Bindings, Verdict: verdict, Duration: time.Since(started)}
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
		if request.Ref.Namespace != l.Request.Namespace {
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
			if _, err := view.DiffAgainstSource(*request.Ref); err != nil {
				response.Error = err.Error()
				break
			}
		}
		object, err := l.Executor.Source.AcceptedManifest(ctx, l.Request.SourceCommit, *request.Ref)
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
			l.mu.Unlock()
			l.operationMu.Unlock()
			response = l.VerifyClosure(ctx)
			l.operationMu.Lock()
			l.mu.Lock()
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
	VerificationSummary *VerificationSummary `json:"verification_summary,omitempty"`
	Truncated           bool                 `json:"truncated"`
	FindingsCount       int                  `json:"findings_count"`
	ProgramRunsCount    int                  `json:"program_runs_count"`
	DetectorStatesCount int                  `json:"detector_states_count"`
	PatchesCount        int                  `json:"patches_count"`
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
		Truncated:    true, FindingsCount: len(response.Findings), ProgramRunsCount: len(response.ProgramRuns), DetectorStatesCount: len(response.DetectorStates), PatchesCount: len(response.Patches),
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
	if l.Request.ObservationGeneration != "" && l.Request.ObservationGeneration != observationGeneration(snapshot) {
		return nil, fmt.Errorf("observation scope changed; incident diagnosis requires fresh investigation")
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
	l.rememberMatchedViews(ctx, runs, view)
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	return runs
}

func (l *ResponderLoop) refresh(ctx context.Context) ([]sdk.Finding, []ProgramRun, string) {
	started := time.Now()
	defer func() { l.timing.refresh += time.Since(started) }()
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil, nil, err.Error()
	}
	findings := make([]sdk.Finding, 0)
	for _, detector := range l.Detectors {
		observed, err := sdk.DetectObserved(ctx, detector, view)
		if err != nil {
			return nil, nil, fmt.Sprintf("detector %s: %v", detector.Spec().ID, err)
		}
		for _, finding := range observed {
			if finding.Status == sdk.FindingActive {
				findings = append(findings, finding)
			}
		}
	}
	drift, _ := checkRetainedDrift(ctx, l.Request.Findings, view)
	findings = append(findings, drift...)
	l.findings = findings
	runs := SelectProgramCandidates(ctx, l.Programs, view, findings, l.deadline())
	l.rememberMatchedViews(ctx, runs, view)
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	return findings, runs, ""
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
	if view == nil || l.batches > 0 {
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
	if !matched || l.batches > 0 {
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
	l.plans[id] = selectedPlan{targetFingerprints: append([]string(nil), request.FindingFingerprints...), id: id, programID: request.ProgramID, bindings: cloneProgramBindings(request.Bindings), plan: plan}
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
	if l.batches >= l.batchLimit() {
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

	if l.pendingConflict(planCaptureRefs(batch), response) {
		return
	}
	// A missing capture is an evidence gap; it must not block the repair.
	_ = l.capturePreApply(ctx, planCaptureRefs(batch)...)
	l.batches++
	started := time.Now().UTC()
	id := fmt.Sprintf("application-%s-%d", l.sessionID, l.batches)
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
	executor.PollRollouts = true
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
			for _, programID := range programIDs {
				failedRuns = append(failedRuns, ProgramRun{ProgramID: programID})
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
	entry.Pending = result.PendingAction != nil || result.RollingBack
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
		p, ok := l.Programs.Diagnoser(plan.programID)
		if !ok {
			continue
		}
		started := time.Now()
		verdict := program.RunDiagnose(ctx, p, view, plan.bindings, l.deadline())
		run := ProgramRun{ProgramID: plan.programID, Bindings: plan.bindings, Verdict: verdict, Duration: time.Since(started)}
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
		state := DetectorEvaluation{DetectorID: detector.Spec().ID, EvaluatedAt: time.Now().UTC(), Status: DetectorEvaluationClear}
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
	drift, driftStates := checkRetainedDrift(ctx, l.Request.Findings, view)
	findings = append(findings, drift...)
	states = append(states, driftStates...)
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
	} else if l.batches == 0 && len(previous) > 0 && len(findings) == 0 {
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
	if len(findings) > 0 {
		runs := SelectProgramCandidates(ctx, l.Programs, view, findings, l.deadline())
		response.ProgramRuns = append(response.ProgramRuns, runs...)
		l.runs = append(l.runs, runs...)
	}
	response.Healthy = response.Error == "" && len(findings) == 0 && (len(l.Detectors) > 0 || len(driftStates) > 0) && detectorsConfirmed(l.Detectors, view)
	response.Unresolved = l.batches >= l.batchLimit() && !response.Healthy
	if response.Error == "" {
		response.Summary = fmt.Sprintf("verification: %d open findings", len(findings))
		if response.Healthy {
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

// Menu rounds and provider continuations have separate budgets. Advertise and
// enforce the same incident-wide write-batch limit used by menu execution.
func (l *ResponderLoop) batchLimit() int {
	if l.MenuConfig != nil {
		return min(5, l.MaxBatches)
	}
	return l.MaxBatches
}
