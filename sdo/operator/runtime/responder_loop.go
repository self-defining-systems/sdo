package runtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/layout"
	"sync"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const DefaultMaxResponseLoops = 5
const DefaultDiagnosisSettleDelay = 15 * time.Second
const maxRecordedToolResponse = 2048

// maxRecordedApplyResponse bounds an apply record that lists the runs of the
// applied plans. Each incident records at most MaxBatches (5) applies, so they
// cost at most 5 x 8 KiB = 40 KiB; the other tool calls are 2 KiB each, well
// under 100 of them (200 KiB). Together they stay far inside the 768 KiB state
// ConfigMap budget. A compact run is about 250 B, so 8 KiB holds about 30 runs,
// several per applied plan.
const maxRecordedApplyResponse = 8192

type ToolRequest struct {
	Command       string           `json:"command"`
	Ref           *sdk.ObjectRef   `json:"ref,omitempty"`
	ProgramID     string           `json:"program_id,omitempty"`
	Bindings      program.Bindings `json:"bindings,omitempty"`
	PlanIDs       []string         `json:"plan_ids,omitempty"`
	ApplicationID string           `json:"application_id,omitempty"`
}

type ToolResponse struct {
	Command       string         `json:"command"`
	Source        map[string]any `json:"source,omitempty"`
	Error         string         `json:"error,omitempty"`
	Summary       string         `json:"summary"`
	PlanID        string         `json:"plan_id,omitempty"`
	ApplicationID string         `json:"application_id,omitempty"`
	Patches       []PlanPatch    `json:"patches,omitempty"`
	ProgramRuns   []ProgramRun   `json:"program_runs,omitempty"`
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
	id        string
	programID string
	bindings  program.Bindings
	plan      program.Plan
}

// ResponderLoop holds one incident's tool state while its responder Job runs.
type ResponderLoop struct {
	CurrentEvidence                 func() *IncidentRequest
	Request                         IncidentRequest
	Programs                        program.ProgramRegistry
	Provider                        SnapshotProvider
	Detectors                       []sdk.Detector
	Executor                        PlanExecutor
	ReadView                        func(sdk.DetectionContext) program.ReadView
	DiagnosisDeadline               time.Duration
	DiagnosisSettleDelay            time.Duration
	CandidateSweepLimit             int
	MaxBatches                      int
	Trust                           map[string]ProgramTrust
	CorpusReplayPassed              bool
	BeforeFastPathApply             func(context.Context, []ProgramRun) error
	CorpusSessionRoot               string
	capturePhase                    string
	preApplyCaptured                bool
	saveToolState                   func(context.Context, []byte) error
	operationMu                     sync.Mutex
	mu                              sync.Mutex
	sessionID                       string
	plans                           map[string]selectedPlan
	applications                    map[string]PlanApplicationResult
	applicationsLog                 []RecordedPlanApplication
	failureSignals                  []string
	calls                           []ToolCall
	runs                            []ProgramRun
	matchedViews                    map[string]program.ReadView
	matchedPlans                    map[string]PlanApplicationResult
	matchedPlanErrors               map[string]error
	lastDiagnosisView               program.ReadView
	findings                        []sdk.Finding
	batches                         int
	fastPathPrograms                []string
	fastPathRelationCoveredFindings []RelationCoveredFinding
	fastPathSkippedReason           FastPathSkipReason
}

func NewResponderLoop(request IncidentRequest, programs program.ProgramRegistry, provider SnapshotProvider, detectors []sdk.Detector, executor PlanExecutor, maxBatches int) *ResponderLoop {
	if maxBatches <= 0 {
		maxBatches = DefaultMaxResponseLoops
	}
	session := make([]byte, 16)
	if _, err := rand.Read(session); err != nil {
		panic(err)
	}
	return &ResponderLoop{sessionID: fmt.Sprintf("%x", session), Request: request, Programs: programs, Provider: provider, Detectors: detectors, Executor: executor, MaxBatches: maxBatches, plans: map[string]selectedPlan{}, applications: map[string]PlanApplicationResult{}, matchedViews: map[string]program.ReadView{}, matchedPlans: map[string]PlanApplicationResult{}, findings: append([]sdk.Finding(nil), request.Findings...)}
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

// RetryInitialDiagnose refreshes inconclusive initial verdicts after the cluster settles.
// The caller starts this while the responder Job is being dispatched.
func (l *ResponderLoop) RetryInitialDiagnose(ctx context.Context) {
	l.mu.Lock()
	inconclusive := false
	for _, run := range l.runs {
		if run.Verdict.Result == program.NoMatch || run.Verdict.Result == program.Unknown {
			inconclusive = true
			break
		}
	}
	delay := l.DiagnosisSettleDelay
	l.mu.Unlock()
	if delay <= 0 {
		delay = DefaultDiagnosisSettleDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.batches == 0 {
		l.capturePhase = "open@settle"
	}
	if inconclusive {
		_, _, _ = l.refresh(ctx)
	} else {
		_, _ = l.snapshotView(ctx)
	}
	l.capturePhase = ""
}

func (l *ResponderLoop) LoopsUsed() int { l.mu.Lock(); defer l.mu.Unlock(); return l.batches }
func (l *ResponderLoop) Exhausted() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.batches >= l.MaxBatches && len(l.findings) > 0
}

func (l *ResponderLoop) Handle(ctx context.Context, request ToolRequest) (response ToolResponse) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	defer func() { l.capturePhase = "" }()
	if l.CurrentEvidence != nil {
		if current := l.CurrentEvidence(); current != nil && current.IncidentID == l.Request.IncidentID {
			l.Request.Findings = append([]sdk.Finding(nil), current.Findings...)
			l.Request.DriftEvents = append([]sdk.DriftEvent(nil), current.DriftEvents...)
		}
	}
	response = ToolResponse{Command: request.Command, LoopsUsed: l.batches, LoopsAllowed: l.MaxBatches}
	defer func() {
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
		if err := l.persistToolState(ctx); err != nil {
			response.Error = err.Error()
			response.Summary = response.Error
		}
	}()
	response.DriftEvents = append([]sdk.DriftEvent(nil), l.Request.DriftEvents...)
	switch request.Command {
	case "status":
		l.capturePreApply()
		response.DriftEvents = append([]sdk.DriftEvent(nil), l.Request.DriftEvents...)
		response.Trust = l.Trust
		response.Findings, response.ProgramRuns, response.Error = l.refresh(ctx)
		response.Healthy = response.Error == "" && len(l.findings) == 0
		response.Unresolved = l.batches >= l.MaxBatches && !response.Healthy
		response.Summary = fmt.Sprintf("%d open findings; %d of %d apply batches used", len(l.findings), l.batches, l.MaxBatches)
	case "diagnose":
		l.capturePreApply()
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
			if l.preApplyCaptured {
				l.capturePhase = "open@pre-apply"
			}
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
		l.capturePreApply()
		if l.capturePhase != "" {
			if _, err := l.snapshotView(ctx); err != nil {
				response.Error = err.Error()
				break
			}
		}
		l.apply(ctx, request, &response)
	case "verify":
		l.verify(ctx, nil, &response)
	case "rollback":
		application, ok := l.applications[request.ApplicationID]
		if !ok {
			response.Error = fmt.Sprintf("unknown application %q", request.ApplicationID)
			break
		}
		executor := l.Executor
		reconciled, err := executor.Reconcile(ctx, application)
		if err != nil {
			response.Error = err.Error()
			break
		}
		application = reconciled
		rollbackErr := executor.rollback(ctx, &application)
		if rollbackErr != nil {
			application.RollbackError = rollbackErr.Error()
		}
		l.applications[request.ApplicationID] = application
		for i := range l.applicationsLog {
			if l.applicationsLog[i].ID == request.ApplicationID {
				l.applicationsLog[i].Result = application
				l.applicationsLog[i].RolledBack = application.RolledBack
				l.applicationsLog[i].RollbackError = application.RollbackError
			}
		}
		if rollbackErr != nil {
			response.Error = rollbackErr.Error()
			break
		}
		l.verify(ctx, nil, &response)
		if response.Error == "" {
			response.Summary = "application rolled back and verified"
		}
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
	Truncated           bool `json:"truncated"`
	FindingsCount       int  `json:"findings_count"`
	ProgramRunsCount    int  `json:"program_runs_count"`
	DetectorStatesCount int  `json:"detector_states_count"`
	PatchesCount        int  `json:"patches_count"`
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
		ToolResponse: ToolResponse{Command: response.Command, Summary: boundedToolText(response.Summary), Error: boundedToolText(response.Error), PlanID: response.PlanID, ApplicationID: response.ApplicationID, LoopsUsed: response.LoopsUsed, LoopsAllowed: response.LoopsAllowed, Healthy: response.Healthy, Unresolved: response.Unresolved},
		Truncated:    true, FindingsCount: len(response.Findings), ProgramRunsCount: len(response.ProgramRuns), DetectorStatesCount: len(response.DetectorStates), PatchesCount: len(response.Patches),
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
		if run.ProgramID == plan.programID && maps.Equal(run.Bindings, plan.bindings) {
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
		if err := memory.CaptureIncidentSnapshot(l.CorpusSessionRoot, l.Request.IncidentID, l.capturePhase, snapshot, l.Request.Namespace); err != nil {
			return nil, err
		}
		if l.capturePhase == "open@pre-apply" {
			l.preApplyCaptured = true
		}
	}
	if l.CorpusSessionRoot != "" && l.capturePhase == "open@detect" {
		dir, err := memory.IncidentCorpusDir(l.CorpusSessionRoot, l.Request.IncidentID)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
		if err != nil {
			return nil, err
		}
		var captured sdktest.Snapshot
		if err := json.Unmarshal(data, &captured); err != nil {
			return nil, err
		}
		snapshot = captured
	}
	view := l.ReadView(snapshot)
	if l.CorpusSessionRoot != "" && l.capturePhase != "" {
		if err := captureRoutingEvidence(ctx, l.CorpusSessionRoot, l.Request.IncidentID, l.capturePhase, l.Request.Findings, view); err != nil {
			return nil, err
		}
	}
	if l.CorpusSessionRoot != "" && l.capturePhase != "" {
		phase := l.capturePhase
		return &sdktest.RecordingReadView{ReadView: view, OnRecord: func(call sdktest.PrimitiveCall) {
			_ = memory.AppendCapturedRecording(l.CorpusSessionRoot, l.Request.IncidentID, phase, call)
		}}, nil
	}
	return view, nil
}

func (l *ResponderLoop) capturePreApply() {
	if l.preApplyCaptured || l.CorpusSessionRoot == "" {
		return
	}
	l.capturePhase = "open@pre-apply"
}

func (l *ResponderLoop) diagnose(ctx context.Context, findings []sdk.Finding) []ProgramRun {
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil
	}
	runs := SelectProgramCandidatesWithOptions(ctx, l.Programs, view, findings, CandidateOptions{Deadline: l.deadline(), SweepLimit: l.CandidateSweepLimit})
	l.rememberMatchedViews(ctx, runs, view)
	l.lastDiagnosisView = view
	l.runs = append(l.runs, runs...)
	return runs
}

func (l *ResponderLoop) refresh(ctx context.Context) ([]sdk.Finding, []ProgramRun, string) {
	view, err := l.snapshotView(ctx)
	if err != nil {
		return nil, nil, err.Error()
	}
	findings := make([]sdk.Finding, 0)
	for _, detector := range l.Detectors {
		observed, err := detector.Detect(ctx, view)
		if err != nil {
			return nil, nil, fmt.Sprintf("detector %s: %v", detector.Spec().ID, err)
		}
		for _, finding := range observed {
			if finding.Status == sdk.FindingActive {
				findings = append(findings, finding)
			}
		}
	}
	drift, _, driftRuns := checkRetainedDrift(ctx, l.Request.Findings, view, l.Programs, l.deadline(), nil)
	findings = append(findings, drift...)
	l.runs = append(l.runs, driftRuns...)
	l.findings = findings
	runs := SelectProgramCandidatesWithOptions(ctx, l.Programs, view, findings, CandidateOptions{Deadline: l.deadline(), SweepLimit: l.CandidateSweepLimit})
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
	l.plans[id] = selectedPlan{id: id, programID: request.ProgramID, bindings: cloneProgramBindings(request.Bindings), plan: plan}
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
					response.Unresolved = l.batches >= l.MaxBatches
					return
				}
			}
		}
	}
	if l.batches >= l.MaxBatches {
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
	result, err := executor.Apply(ctx, l.Request, batch, false)
	completed := time.Now().UTC()
	if result.Applied || result.PendingAction != nil {
		l.applications[id] = result
		response.ApplicationID = id
	}
	if err != nil {
		response.Error = err.Error()
	}
	l.verify(ctx, selected, response)
	if result.Applied && (err != nil || !result.VerificationPassed || !response.Healthy || !boundPlansNoMatch(selected, response.ProgramRuns)) {
		result.VerificationPassed = false
		if response.Error == "" {
			response.Error = "plan verification failed"
		}
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		// Preserve the failed revision independently of the in-process application
		// log, including when rollback succeeds or the process stops during it.
		failedRuns := make([]ProgramRun, 0, len(programIDs))
		for _, programID := range programIDs {
			failedRuns = append(failedRuns, ProgramRun{ProgramID: programID})
		}
		fillProgramRevisions(recoveryCtx, failedRuns, l.Request.RepositoryWorktree, l.Request.SourceCommit)
		for _, run := range failedRuns {
			signal := "application_verification_failure:" + run.ProgramID
			if run.ProgramRevision != "" {
				signal += "@" + run.ProgramRevision
			}
			l.addFailureSignal(signal)
		}
		if persistErr := l.persistToolState(recoveryCtx); persistErr != nil {
			response.Error += ": " + persistErr.Error()
		}
		rollbackErr := executor.rollback(recoveryCtx, &result)
		cancel()
		if rollbackErr != nil {
			result.RollbackError = rollbackErr.Error()
			response.Error += ": rollback failed: " + rollbackErr.Error()
			for _, id := range programIDs {
				l.addFailureSignal("rollback_failure:" + id)
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
	response.Unresolved = l.batches >= l.MaxBatches && !response.Healthy
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
		observed, err := detector.Detect(ctx, view)
		state := DetectorEvaluation{DetectorID: detector.Spec().ID, EvaluatedAt: time.Now().UTC(), Status: DetectorEvaluationClear}
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
	drift, driftStates, driftRuns := checkRetainedDrift(ctx, l.Request.Findings, view, l.Programs, l.deadline(), nil)
	findings = append(findings, drift...)
	states = append(states, driftStates...)
	response.ProgramRuns = append(response.ProgramRuns, driftRuns...)
	l.runs = append(l.runs, driftRuns...)
	l.findings = findings
	if len(applied) > 0 {
		old := map[string]bool{}
		for _, finding := range previous {
			old[FindingFingerprint(finding)] = true
		}
		for _, finding := range findings {
			if !old[FindingFingerprint(finding)] {
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
	if len(applied) > 0 && len(findings) > 0 {
		for _, run := range response.ProgramRuns {
			if run.Verdict.Result == program.NoMatch {
				l.addFailureSignal("diagnosis_clear_detectors_firing:" + run.ProgramID)
			}
		}
	}
	response.Findings, response.DetectorStates = append([]sdk.Finding(nil), findings...), states
	if len(findings) > 0 {
		runs := SelectProgramCandidatesWithOptions(ctx, l.Programs, view, findings, CandidateOptions{Deadline: l.deadline(), SweepLimit: l.CandidateSweepLimit})
		response.ProgramRuns = append(response.ProgramRuns, runs...)
		l.runs = append(l.runs, runs...)
	}
	response.Healthy = response.Error == "" && len(findings) == 0 && (len(l.Detectors) > 0 || len(driftStates) > 0)
	response.Unresolved = l.batches >= l.MaxBatches && !response.Healthy
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
			if run.ProgramID != plan.programID || !maps.Equal(run.Bindings, plan.bindings) {
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
