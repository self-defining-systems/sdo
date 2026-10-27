package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/worker_registry"
)

type ReflectionTaskExecutor interface {
	Execute(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)
}

// RegisteredReflectionWorker dispatches a typed task through an
// implementation-neutral executor. Production supplies KubernetesExecutor,
// which mounts only the broker-prepared proposal checkout.
type RegisteredReflectionWorker struct {
	TaskStore   *lifecycleio.TaskStore
	Executor    ReflectionTaskExecutor
	AgentID     string
	TaskType    string
	Outcomes    memory.Reader
	Catalog     memory.CatalogReader
	Timeout     time.Duration
	SessionMode string
	Guidance    string
	// SessionExecutor supplies the narrow per-incident session transport for resume.
	SessionExecutor      func(incidentID string) (ReflectionTaskExecutor, error)
	FreshSessionExecutor func(incidentID string) (ReflectionTaskExecutor, error)
	UsageLogPath         func(incidentID string) string
	CorpusSessionRoot    string
}

var resumableSessionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func hasResponderSession(closure IncidentClosure) bool {
	return closure.Result != nil && resumableSessionID.MatchString(closure.Result.ResponderSessionID)
}

func (w RegisteredReflectionWorker) Reflect(ctx context.Context, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, proposalCommit, outcomeCommit string, feedback ReflectionFeedback) (ReflectionDecision, error) {
	if w.Executor == nil || w.Outcomes == nil || w.AgentID == "" || w.TaskType == "" || w.Timeout <= 0 {
		return ReflectionDecision{}, errors.New("registered reflection worker requires executor, outcome store, agent ID, task type, and timeout")
	}
	if workspace.IncidentID != closure.Request.IncidentID || workspace.Worktree == "" || workspace.BaseCommit == "" || outcomeCommit == "" {
		return ReflectionDecision{}, errors.New("reflection workspace and outcome commit are required")
	}
	if w.CorpusSessionRoot != "" && outcome != nil && (outcome.GetClassification() == "success" || outcome.GetClassification() == "cleared_without_action") && !closure.VerifiedAt.IsZero() {
		if err := memory.MarkIncidentAPIRecovery(w.CorpusSessionRoot, workspace.IncidentID, closure.VerifiedAt); err != nil {
			log.Printf("API history recovery marker unavailable for incident %s: %v", workspace.IncidentID, err)
		}
	}
	if w.CorpusSessionRoot != "" && outcome != nil {
		if err := memory.CaptureRepairReceipts(w.CorpusSessionRoot, workspace.IncidentID, outcome); err != nil {
			return ReflectionDecision{}, fmt.Errorf("capture original repair receipts: %w", err)
		}
	}
	attempt := max(uint32(1), feedback.Attempt)
	proposalID := lifecyclecore.ReflectionTaskID(workspace.IncidentID, outcomeCommit, attempt)
	var task *contracts.AgentTask
	if w.TaskStore != nil {
		var err error
		task, _, err = w.TaskStore.Load(proposalID)
		if err != nil {
			return ReflectionDecision{}, err
		}
		if task != nil && transportSize(task) > worker_registry.MaxRequestBytes {
			// Frozen by a build whose request had no transport limit: it never ran, so freeze it again within the limit.
			if err := w.TaskStore.Discard(proposalID); err != nil {
				return ReflectionDecision{}, err
			}
			task = nil
		}
		if task != nil && (task.GetAgentId() != w.AgentID || task.GetTaskType() != w.TaskType || task.GetWorkspace().GetCheckoutPath() != workspace.Worktree || task.GetWorkspace().GetBaseCommit() != workspace.BaseCommit) {
			return ReflectionDecision{}, errors.New("frozen reflection task differs from registered worker or workspace")
		}
	}
	mode := w.SessionMode
	if mode == "" {
		mode = "resume"
	}
	guidance := w.Guidance
	if guidance == "" {
		guidance = "baseline"
	}
	if mode == "resume" && stageOnlyClosure(closure) {
		mode = "fresh"
	}
	if mode == "resume" && !hasResponderSession(closure) {
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "no_session"}, nil
	}
	if task != nil {
		frozen := &contracts.ReflectionRequest{}
		if err := task.GetRequest().UnmarshalTo(frozen); err != nil {
			return ReflectionDecision{}, err
		}
		mode = frozen.GetSessionMode()
	}
	executor := w.Executor
	if mode == "fresh" && w.FreshSessionExecutor != nil {
		var err error
		executor, err = w.FreshSessionExecutor(workspace.IncidentID)
		if err != nil {
			return ReflectionDecision{}, fmt.Errorf("prepare fresh reflection session: %w", err)
		}
	}
	if mode == "resume" {
		if w.SessionExecutor == nil {
			return ReflectionDecision{}, errors.New("resumed reflection requires isolated incident session transport")
		}
		var err error
		executor, err = w.SessionExecutor(workspace.IncidentID)
		if err != nil {
			return ReflectionDecision{}, fmt.Errorf("prepare resumed reflection session: %w", err)
		}
		if executor == nil {
			return ReflectionDecision{}, errors.New("resumed reflection session transport is unavailable")
		}
	}
	if task == nil {
		closed, err := toProtoClosure(closure)
		if err != nil {
			return ReflectionDecision{}, err
		}
		if outcome == nil {
			return ReflectionDecision{}, errors.New("reflection outcome is required")
		}
		current := proto.Clone(outcome).(*contracts.OutcomeRecord)
		if current.GetIncidentId() != workspace.IncidentID {
			return ReflectionDecision{}, errors.New("reflection outcome incident differs from proposal")
		}
		prior, _, err := memory.ListOutcomes(ctx, w.Outcomes, "reflection prior outcomes")
		if err != nil {
			return ReflectionDecision{}, err
		}
		history := make([]*contracts.OutcomeRecord, 0, len(prior))
		for _, record := range prior {
			if record.IncidentID != workspace.IncidentID {
				history = append(history, summarizeEarlierOutcome(record.Record))
			}
		}
		sort.SliceStable(history, func(i, j int) bool { return verifiedTime(history[i]).Before(verifiedTime(history[j])) })
		history = history[max(0, len(history)-maxReflectionHistory):]
		trimReflectionClosure(closed)
		request := &contracts.ReflectionRequest{Closure: closed, Outcome: current, PriorOutcomes: history, ProposalCommit: proposalCommit, OutcomeCommit: outcomeCommit, SessionMode: mode, Guidance: guidance, ValidationAttempt: attempt, PreviousDiagnostics: feedback.Diagnostics, CauseWork: feedback.CauseWork, UnassignedWork: feedback.UnassignedWork}
		if w.Catalog != nil {
			snapshot, err := memory.SnapshotAt(ctx, w.Catalog, workspace.BaseCommit)
			if err != nil {
				return ReflectionDecision{}, fmt.Errorf("read logical operational memory: %w", err)
			}
			if snapshot.GetAcceptedCommit() != workspace.BaseCommit {
				return ReflectionDecision{}, errors.New("reflection memory snapshot differs from broker proposal base commit")
			}
			request.MemorySnapshot = snapshot
			currentWire, err := wire.Marshal(current)
			if err != nil {
				return ReflectionDecision{}, err
			}
			if snapshot.GetCodePath().GetName() != "shell" && bytes.Contains(currentWire, []byte(layout.PhysicalPathMarker)) {
				return ReflectionDecision{}, errors.New("physical operational memory path in outcome evidence")
			}
			for _, priorOutcome := range history {
				priorWire, err := wire.Marshal(priorOutcome)
				if err != nil {
					return ReflectionDecision{}, err
				}
				if snapshot.GetCodePath().GetName() != "shell" && bytes.Contains(priorWire, []byte(layout.PhysicalPathMarker)) {
					return ReflectionDecision{}, errors.New("physical operational memory path in prior outcome evidence")
				}
			}
			logicalRequest := proto.Clone(request).(*contracts.ReflectionRequest)
			logicalRequest.PreviousDiagnostics = nil
			logicalRequest.ValidationAttempt = 1
			requestWire, err := wire.Marshal(logicalRequest)
			if err != nil {
				return ReflectionDecision{}, err
			}
			if snapshot.GetCodePath().GetName() != "shell" && bytes.Contains(requestWire, []byte(layout.PhysicalPathMarker)) {
				return ReflectionDecision{}, errors.New("physical operational memory path leaked into reflection task")
			}
		}
		dispatchAt := feedback.DispatchAt
		if dispatchAt.IsZero() {
			dispatchAt = closure.VerifiedAt
		}
		if dispatchAt.IsZero() {
			return ReflectionDecision{}, errors.New("reflection dispatch event time is required")
		}
		build := func() (*contracts.AgentTask, error) {
			payload, err := anypb.New(request)
			if err != nil {
				return nil, err
			}
			return &contracts.AgentTask{
				SchemaVersion: worker_registry.SchemaVersion, TaskId: proposalID, AgentId: w.AgentID, TaskType: w.TaskType,
				Request: payload, Workspace: &contracts.ProposalWorkspace{ProposalId: proposalID, CheckoutPath: workspace.Worktree, BaseCommit: workspace.BaseCommit},
				Deadline: lifecyclecore.ReflectionDeadline(dispatchAt, w.Timeout),
			}, nil
		}
		fits, err := fitReflectionRequest(request, build)
		if err != nil {
			return ReflectionDecision{}, err
		}
		if !fits {
			return w.withUsage(ReflectionDecision{LearningDecision: "failed", FailureReason: ReflectionRequestTooLarge, Error: fmt.Sprintf("reflection request is over the %d byte transport limit after %s; reflection skipped, the incident closes on health", worker_registry.MaxRequestBytes, strings.Join(request.Compaction, ", "))}, workspace.IncidentID)
		}
		if err := wire.Validate(request); err != nil {
			return ReflectionDecision{}, err
		}
		task, err = build()
		if err != nil {
			return ReflectionDecision{}, err
		}
		if w.TaskStore != nil {
			if err := w.TaskStore.Save(task); err != nil {
				return ReflectionDecision{}, err
			}
		}
	}
	if task.GetDeadline() == nil || task.GetDeadline().CheckValid() != nil {
		return ReflectionDecision{}, errors.New("frozen reflection task requires a valid deadline")
	}
	deadline := task.GetDeadline().AsTime()
	timeout := func() (ReflectionDecision, error) {
		return w.withUsage(ReflectionDecision{LearningDecision: "failed", FailureReason: "timeout", Error: fmt.Sprintf("reflection task %s exceeded frozen deadline %s", task.GetTaskId(), deadline.UTC().Format(time.RFC3339Nano))}, workspace.IncidentID)
	}
	var result *contracts.AgentResult
	var err error
	recovered := false
	if recovery, ok := executor.(worker_registry.ReflectionResultRecoverer); ok {
		result, recovered, err = recovery.RecoverCompleted(ctx, task)
		if err != nil {
			var terminalTimeout *worker_registry.TaskTimeoutError
			if errors.As(err, &terminalTimeout) || !time.Now().Before(deadline) {
				return timeout()
			}
			return ReflectionDecision{}, err
		}
	}
	if !recovered {
		if !time.Now().Before(deadline) {
			return timeout()
		}
		executionCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		result, err = executor.Execute(executionCtx, task)
		var terminalTimeout *worker_registry.TaskTimeoutError
		if errors.As(err, &terminalTimeout) || !time.Now().Before(deadline) {
			return timeout()
		}
	}
	if err != nil {
		return ReflectionDecision{}, err
	}
	if result == nil || result.GetSchemaVersion() != task.SchemaVersion || result.GetTaskId() != task.TaskId || result.GetProposalId() != proposalID {
		return ReflectionDecision{}, errors.New("reflection worker returned a mismatched or incomplete result")
	}
	if result.GetStatus() == "failed" && result.GetErrorSummary() != "" {
		reason := result.GetFailureReason()
		if reason == "" {
			reason = "agent_failed"
		}
		return w.withUsage(ReflectionDecision{LearningDecision: "failed", FailureReason: reason, Error: result.GetErrorSummary()}, workspace.IncidentID)
	}
	if result.GetStatus() != "completed" {
		return ReflectionDecision{}, errors.New("reflection worker returned a mismatched or incomplete result")
	}
	if err := wire.Validate(result); err != nil {
		return ReflectionDecision{}, err
	}
	if result.GetResponse() == nil {
		return ReflectionDecision{}, errors.New("reflection worker omitted typed result")
	}
	response := &contracts.ReflectionResult{}
	if err := result.GetResponse().UnmarshalTo(response); err != nil {
		return ReflectionDecision{}, fmt.Errorf("reflection result contract: %w", err)
	}
	if encoded, err := protojson.Marshal(result); err != nil {
		return ReflectionDecision{}, err
	} else if len(encoded) > 700<<10 {
		return ReflectionDecision{}, fmt.Errorf("reflection result exceeds transport limit: %d bytes", len(encoded))
	}
	if err := validate.Err(validate.ReflectionResultDiagnostics(response)); err != nil {
		return ReflectionDecision{}, err
	}
	decision := ReflectionDecision{DeclineReason: response.GetDeclineReason(), LearningDecision: response.GetLearningDecision(), NoChangeReason: response.GetNoChangeReason(), PlaybookIDs: append([]string(nil), response.GetProposedArtifactIds()...), SDKGaps: response.GetSdkGaps()}
	return w.withUsage(decision, workspace.IncidentID)
}

// withUsage totals every logged turn of the incident: each resumed reflection turn, retry and
// correction appends one record, and the cost of the logical reflection is all of them.
func (w RegisteredReflectionWorker) withUsage(decision ReflectionDecision, incidentID string) (ReflectionDecision, error) {
	if w.UsageLogPath != nil {
		data, err := os.ReadFile(w.UsageLogPath(incidentID))
		if err == nil {
			for _, line := range bytes.Split(data, []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				var record struct {
					Usage *UsageMetrics `json:"usage"`
				}
				if json.Unmarshal(line, &record) == nil && record.Usage != nil {
					decision.Usage = memory.AddUsage(decision.Usage, record.Usage)
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return ReflectionDecision{}, fmt.Errorf("read reflection usage: %w", err)
		}
	}
	return decision, nil
}

// trimReflectionClosure drops closure fields the request already carries elsewhere: the result's program runs
// are the outcome's, and the frozen dispatch task and initial runs only restate the incident request.
func trimReflectionClosure(closure *contracts.IncidentClosure) {
	if closure.GetResult() != nil {
		closure.Result.ProgramRuns = nil
		// The outcome carries the same raw transcripts; the worker restores them for a fresh-session brief.
		closure.Result.ProgramToolCalls = nil
		closure.Result.PlanApplications = nil
	}
	if closure.GetRequest() != nil {
		closure.Request.PreparedAgentTask = ""
		closure.Request.InitialProgramRuns = nil
	}
}

// maxReflectionHistory is how many earlier incidents the reflection worker shows. It is the one source of that
// limit: the worker's history view shows every incident the request carries.
const maxReflectionHistory = 2

// verifiedTime orders earlier incidents by when they were verified, not by incident ID.
func verifiedTime(record *contracts.OutcomeRecord) time.Time {
	return record.GetTimestamps().GetVerifiedAt().AsTime()
}

// summarizeEarlierOutcome copies an earlier incident without its raw program evaluations, tool transcripts and
// plan applications, which dominate the record and are re-sent with every reflection request. The full prior
// record the worker can open by reference therefore omits them too; the stored outcome keeps them.
func summarizeEarlierOutcome(record *contracts.OutcomeRecord) *contracts.OutcomeRecord {
	earlier := proto.Clone(record).(*contracts.OutcomeRecord)
	earlier.ProgramRuns = nil
	earlier.ProgramToolCalls = nil
	earlier.PlanApplications = nil
	return earlier
}

// ReflectionRequestTooLarge is the typed, non-retryable reason a reflection is skipped when its request does not
// fit the transport after every degrade step. The incident still closes on health.
const ReflectionRequestTooLarge = "request_too_large"

// fitReflectionRequest degrades the request in a fixed order until the frozen task fits the transport limit,
// recording each lossy step on the request: earlier incidents, then memory the incident does not touch, then
// the outcome's raw program runs and tool calls. It reports false when the request still does not fit.
func fitReflectionRequest(request *contracts.ReflectionRequest, build func() (*contracts.AgentTask, error)) (bool, error) {
	size := func() (int, error) {
		task, err := build()
		if err != nil {
			return 0, err
		}
		return wireSize(task)()
	}
	mentions := func() []byte {
		stripped := proto.Clone(request).(*contracts.ReflectionRequest)
		stripped.MemorySnapshot = nil
		text, _ := wire.Marshal(stripped)
		return text
	}
	steps, fits, err := degradeToFit(size, []transportRung{
		{"drop_prior_incident_history", func() bool { changed := len(request.PriorOutcomes) > 0; request.PriorOutcomes = nil; return changed }},
		{"trim_memory_to_touched", func() bool { return trimSnapshotToTouched(request.MemorySnapshot, mentions()) }},
		{"clip_memory_documents", func() bool { return clipSnapshotDocuments(request.MemorySnapshot) }},
		{"cap_program_runs_and_tool_calls", func() bool {
			if request.Outcome == nil {
				return false
			}
			runs, runsChanged := capProgramRuns(request.Outcome.ProgramRuns)
			calls, callsChanged := capToolCalls(request.Outcome.ProgramToolCalls)
			request.Outcome.ProgramRuns, request.Outcome.ProgramToolCalls = runs, calls
			return runsChanged || callsChanged
		}},
	})
	request.Compaction = append(request.Compaction, steps...)
	return fits, err
}

func transportSize(task *contracts.AgentTask) int {
	size, err := wireSize(task)()
	if err != nil {
		return 0
	}
	return size
}
