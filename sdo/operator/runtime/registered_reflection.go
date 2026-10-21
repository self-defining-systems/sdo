package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/worker_registry"
)

type ReflectionTaskExecutor interface {
	Execute(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)
}

// RegisteredReflectionWorker dispatches a typed task through an
// implementation-neutral executor. Production supplies KubernetesExecutor,
// which mounts only the broker-prepared proposal checkout.
type RegisteredReflectionWorker struct {
	Executor    ReflectionTaskExecutor
	AgentID     string
	TaskType    string
	Outcomes    memory.Reader
	Catalog     memory.CatalogReader
	Timeout     time.Duration
	SessionMode string
	Guidance    string
	// SessionExecutor supplies the narrow per-incident session transport only
	// when resume is explicitly selected. Fresh reflection uses Executor.
	SessionExecutor func(incidentID string) (ReflectionTaskExecutor, error)
}

var resumableSessionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func (w RegisteredReflectionWorker) Reflect(ctx context.Context, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, proposalCommit, outcomeCommit string, feedback ReflectionFeedback) (ReflectionDecision, error) {
	if w.Executor == nil || w.Outcomes == nil || w.AgentID == "" || w.TaskType == "" || w.Timeout <= 0 {
		return ReflectionDecision{}, errors.New("registered reflection worker requires executor, outcome store, agent ID, task type, and timeout")
	}
	if workspace.IncidentID != closure.Request.IncidentID || workspace.Worktree == "" || workspace.BaseCommit == "" || outcomeCommit == "" {
		return ReflectionDecision{}, errors.New("reflection workspace and outcome commit are required")
	}
	mode := w.SessionMode
	if mode == "" {
		mode = "fresh"
	}
	guidance := w.Guidance
	if guidance == "" {
		guidance = "baseline"
	}
	if mode == "resume" && (closure.Result == nil || !resumableSessionID.MatchString(closure.Result.ResponderSessionID)) {
		return ReflectionDecision{}, errors.New("resumed reflection requires a valid responder session ID in the accepted closure")
	}
	executor := w.Executor
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
	prior, err := w.Outcomes.List(ctx)
	if err != nil {
		return ReflectionDecision{}, err
	}
	history := make([]*contracts.OutcomeRecord, 0, len(prior))
	for _, record := range prior {
		if record.IncidentID != workspace.IncidentID {
			history = append(history, proto.Clone(record.Record).(*contracts.OutcomeRecord))
		}
	}
	attempt := feedback.Attempt
	if attempt == 0 {
		attempt = 1
	}
	request := &contracts.ReflectionRequest{Closure: closed, Outcome: current, PriorOutcomes: history, ProposalCommit: proposalCommit, OutcomeCommit: outcomeCommit, SessionMode: mode, Guidance: guidance, ValidationAttempt: attempt, PreviousDiagnostics: feedback.Diagnostics}
	if w.Catalog != nil {
		snapshot, err := w.Catalog.Snapshot(ctx)
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
		if bytes.Contains(currentWire, []byte(layout.PhysicalPathMarker)) {
			return ReflectionDecision{}, errors.New("physical operational memory path in outcome evidence")
		}
		for _, priorOutcome := range history {
			priorWire, err := wire.Marshal(priorOutcome)
			if err != nil {
				return ReflectionDecision{}, err
			}
			if bytes.Contains(priorWire, []byte(layout.PhysicalPathMarker)) {
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
		if bytes.Contains(requestWire, []byte(layout.PhysicalPathMarker)) {
			return ReflectionDecision{}, errors.New("physical operational memory path leaked into reflection task")
		}
	}
	if err := wire.Validate(request); err != nil {
		return ReflectionDecision{}, err
	}
	payload, err := anypb.New(request)
	if err != nil {
		return ReflectionDecision{}, err
	}
	identity := fmt.Sprintf("%x", sha256.Sum256([]byte(workspace.IncidentID+"\x00"+outcomeCommit)))
	proposalID := "reflection-" + identity
	if attempt > 1 {
		// A retry is a new task: its Job and request ConfigMap must not collide
		// with the rejected attempt's.
		proposalID = fmt.Sprintf("%s-retry%d", proposalID, attempt-1)
	}
	task := &contracts.AgentTask{
		SchemaVersion: worker_registry.SchemaVersion, TaskId: proposalID, AgentId: w.AgentID, TaskType: w.TaskType,
		Request: payload, Workspace: &contracts.ProposalWorkspace{ProposalId: proposalID, CheckoutPath: workspace.Worktree, BaseCommit: workspace.BaseCommit},
		Deadline: timestamppb.New(time.Now().Add(w.Timeout)),
	}
	result, err := executor.Execute(ctx, task)
	if err != nil {
		return ReflectionDecision{}, err
	}
	if result == nil || result.GetTaskId() != task.TaskId || result.GetProposalId() != proposalID || result.GetStatus() != "completed" {
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
	if err := wire.Validate(response); err != nil {
		return ReflectionDecision{}, err
	}
	// The direct-edit capability never accepts the retired content payload.
	if response.GetArtifactProposal() != nil {
		return ReflectionDecision{}, errors.New("direct-edit reflection rejects deprecated artifact_proposal")
	}
	paths, err := validate.ChangedPaths(ctx, workspace.Worktree, workspace.BaseCommit)
	if err != nil {
		return ReflectionDecision{}, err
	}
	decision := ReflectionDecision{LearningDecision: response.GetLearningDecision(), NoChangeReason: response.GetNoChangeReason(), PlaybookIDs: append([]string(nil), response.GetProposedArtifactIds()...)}
	if err := decision.Validate(paths); err != nil {
		return ReflectionDecision{}, err
	}
	return decision, nil
}

// Validate binds the worker decision to the actual checkout diff. Content and
// ownership rules remain in validate.Check, shared by the CLI and broker.
func (d ReflectionDecision) Validate(paths []string) error {
	switch d.LearningDecision {
	case "updated":
		if len(paths) == 0 {
			return errors.New("updated reflection requires a nonempty diff")
		}
	case "no_change", "sdk_gap":
		if strings.TrimSpace(d.NoChangeReason) == "" {
			return errors.New("no-change or SDK-gap reflection requires a concrete reason")
		}
		if len(paths) != 0 || len(d.PlaybookIDs) != 0 {
			return errors.New("no-change or SDK-gap reflection cannot change files or claim playbook IDs")
		}
	default:
		return errors.New("reflection result requires an explicit learning decision")
	}
	seen := map[string]bool{}
	for _, id := range d.PlaybookIDs {
		if !layout.ValidPlaybookID(id) || seen[id] {
			return fmt.Errorf("invalid or repeated logical playbook ID %q", id)
		}
		seen[id] = true
		changed := false
		for _, path := range paths {
			_, changedID, ok := layout.Classify(path)
			if ok && changedID == id && strings.HasPrefix(path, layout.PlaybooksDir+"/") {
				changed = true
			}
		}
		if !changed {
			return fmt.Errorf("reported playbook %q is absent from the diff", id)
		}
	}
	for _, path := range paths {
		kind, id, ok := layout.Classify(path)
		if ok && (kind == layout.KindPlaybook || kind == layout.KindPlaybookScript) && !seen[id] {
			return fmt.Errorf("changed playbook %q is missing from the result", id)
		}
	}
	return nil
}
