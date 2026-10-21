package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type ToolRequest struct {
	Command       string           `json:"command"`
	Ref           *sdk.ObjectRef   `json:"ref,omitempty"`
	ProgramID     string           `json:"program_id,omitempty"`
	Bindings      program.Bindings `json:"bindings,omitempty"`
	PlanIDs       []string         `json:"plan_ids,omitempty"`
	ApplicationID string           `json:"application_id,omitempty"`
}

type ToolResponse struct {
	Command        string                  `json:"command"`
	Source         map[string]any          `json:"source,omitempty"`
	Error          string                  `json:"error,omitempty"`
	Summary        string                  `json:"summary"`
	PlanID         string                  `json:"plan_id,omitempty"`
	ApplicationID  string                  `json:"application_id,omitempty"`
	Patches        []PlanPatch             `json:"patches,omitempty"`
	ProgramRuns    []ProgramRun            `json:"program_runs,omitempty"`
	Findings       []sdk.Finding           `json:"findings,omitempty"`
	DriftEvents    []sdk.DriftEvent        `json:"drift_events,omitempty"`
	DetectorStates []DetectorEvaluation    `json:"detector_states,omitempty"`
	LoopsUsed      int                     `json:"loops_used"`
	LoopsAllowed   int                     `json:"loops_allowed"`
	Healthy        bool                    `json:"healthy"`
	Unresolved     bool                    `json:"unresolved"`
	Trust          map[string]ProgramTrust `json:"trust,omitempty"`
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
	BeforeFastPathApply             func(context.Context, []string, []PlanPreImage) error
	CorpusSessionRoot               string
	capturePhase                    string
	preApplyCaptured                bool
	mu                              sync.Mutex
	plans                           map[string]selectedPlan
	applications                    map[string]PlanApplicationResult
	applicationsLog                 []RecordedPlanApplication
	failureSignals                  []string
	calls                           []ToolCall
	runs                            []ProgramRun
	matchedViews                    map[string]program.ReadView
	matchedPlans                    map[string]PlanApplicationResult
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
	return &ResponderLoop{Request: request, Programs: programs, Provider: provider, Detectors: detectors, Executor: executor, MaxBatches: maxBatches, plans: map[string]selectedPlan{}, applications: map[string]PlanApplicationResult{}, matchedViews: map[string]program.ReadView{}, matchedPlans: map[string]PlanApplicationResult{}, findings: append([]sdk.Finding(nil), request.Findings...)}
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
	l.mu.Lock()
	defer l.mu.Unlock()
	l.capturePhase = "open@detect"
	defer func() { l.capturePhase = "" }()
	return l.diagnose(ctx, l.findings)
}

// RestoreInitialMatches prepares plans from the dispatch capture when the
// responder loop is recreated after the initial diagnosis was persisted.
func (l *ResponderLoop) RestoreInitialMatches(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.CorpusSessionRoot == "" || l.Programs == nil {
		return nil
	}
	matched := make([]ProgramRun, 0)
	for _, run := range l.Request.InitialProgramRuns {
		if run.Verdict.Result == program.Match {
			matched = append(matched, run)
		}
	}
	if len(matched) == 0 {
		return nil
	}
	dir, err := memory.IncidentCorpusDir(l.CorpusSessionRoot, l.Request.IncidentID)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
	if err != nil {
		return fmt.Errorf("read dispatch snapshot: %w", err)
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode dispatch snapshot: %w", err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		return fmt.Errorf("read dispatch recordings: %w", err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(data, &calls); err != nil {
		return fmt.Errorf("decode dispatch recordings: %w", err)
	}
	for _, run := range matched {
		view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
		l.rememberMatchedViews(ctx, []ProgramRun{run}, view)
	}
	return nil
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
		responseJSON, _ := json.Marshal(response)
		if len(responseJSON) > maxRecordedToolResponse {
			responseJSON, _ = json.Marshal(map[string]any{"command": response.Command, "summary": boundedToolText(response.Summary), "error": boundedToolText(response.Error), "healthy": response.Healthy, "unresolved": response.Unresolved, "loops_used": response.LoopsUsed, "loops_allowed": response.LoopsAllowed, "findings_count": len(response.Findings), "program_runs_count": len(response.ProgramRuns), "detector_states_count": len(response.DetectorStates), "patches_count": len(response.Patches), "truncated": true})
		}
		l.calls = append(l.calls, ToolCall{Command: request.Command, At: time.Now().UTC(), RequestJSON: string(requestJSON), ResponseJSON: string(responseJSON), Error: response.Error, Summary: response.Summary})
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
			run := ProgramRun{ProgramID: request.ProgramID, Bindings: request.Bindings, Verdict: verdict, Duration: time.Since(started)}
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
		if err := l.Executor.Rollback(ctx, application); err != nil {
			response.Error = err.Error()
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
	for _, run := range runs {
		if run.Verdict.Result == program.Match {
			key := matchedViewKey(run.ProgramID, run.Bindings)
			l.matchedViews[key] = view
			mitigator, ok := l.Programs.Mitigator(run.ProgramID)
			if !ok {
				continue
			}
			plan, err := mitigator.Mitigate(ctx, view, run.Bindings)
			if err != nil || plan.Validate() != nil || len(plan.Actions) == 0 {
				continue
			}
			l.matchedPlans[key] = PlanApplicationResult{Plan: plan}
			preview, err := l.Executor.Apply(ctx, l.Request, plan, true)
			if err == nil {
				l.matchedPlans[key] = preview
			}
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
		plan, err = mitigator.Mitigate(ctx, view, request.Bindings)
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
	id := fmt.Sprintf("plan-%d", len(l.plans)+1)
	l.plans[id] = selectedPlan{id: id, programID: request.ProgramID, bindings: cloneProgramBindings(request.Bindings), plan: plan}
	response.PlanID, response.Patches = id, preview.Patches
	response.Summary = fmt.Sprintf("%s: %d dry-run patches", id, len(preview.Patches))
}

func (l *ResponderLoop) apply(ctx context.Context, request ToolRequest, response *ToolResponse) {
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
	result, err := l.Executor.Apply(ctx, l.Request, batch, false)
	completed := time.Now().UTC()
	id := fmt.Sprintf("application-%d", len(l.applications)+1)
	if result.Applied {
		l.applications[id] = result
		response.ApplicationID = id
	}
	programIDs := make([]string, 0, len(selected))
	for _, plan := range selected {
		programIDs = append(programIDs, plan.programID)
	}
	entry := RecordedPlanApplication{ID: id, ProgramIDs: programIDs, Result: result, StartedAt: started, CompletedAt: completed, RolledBack: result.RolledBack, RollbackError: result.RollbackError}
	if err != nil {
		entry.Error = err.Error()
	}
	l.applicationsLog = append(l.applicationsLog, entry)
	if err != nil {
		response.Error = err.Error()
	}
	l.verify(ctx, selected, response)
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
