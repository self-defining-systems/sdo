package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
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

func (w RegisteredReflectionWorker) Reflect(ctx context.Context, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, proposalCommit, outcomeCommit string) (ReflectionDecision, error) {
	if w.Executor == nil || w.Outcomes == nil || w.AgentID == "" || w.Timeout <= 0 {
		return ReflectionDecision{}, errors.New("registered reflection worker requires executor, outcome store, agent ID, and timeout")
	}
	if workspace.IncidentID != closure.Request.IncidentID || workspace.Worktree == "" || workspace.BaseCommit == "" || outcomeCommit == "" {
		return ReflectionDecision{}, errors.New("reflection workspace and outcome commit are required")
	}
	mode := w.SessionMode
	if mode == "" {
		mode = "fresh"
	}
	if mode != "fresh" && mode != "resume" {
		return ReflectionDecision{}, fmt.Errorf("unsupported reflection session mode %q", mode)
	}
	guidance := w.Guidance
	if guidance == "" {
		guidance = "baseline"
	}
	if guidance != "baseline" && guidance != "generalize" && guidance != "generalize-spec" {
		return ReflectionDecision{}, fmt.Errorf("unsupported reflection guidance %q", guidance)
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
	request := &contracts.ReflectionRequest{Closure: closed, Outcome: current, PriorOutcomes: history, ProposalCommit: proposalCommit, OutcomeCommit: outcomeCommit, SessionMode: mode, Guidance: guidance}
	if w.Catalog != nil {
		snapshot, err := w.Catalog.Snapshot(ctx)
		if err != nil {
			return ReflectionDecision{}, fmt.Errorf("read logical operational memory: %w", err)
		}
		if snapshot.GetAcceptedCommit() != workspace.BaseCommit {
			return ReflectionDecision{}, errors.New("reflection memory snapshot differs from broker proposal base commit")
		}
		request.MemorySnapshot = snapshot
		currentWire, err := protojson.Marshal(current)
		if err != nil {
			return ReflectionDecision{}, err
		}
		if bytes.Contains(currentWire, []byte(layout.PhysicalPathMarker)) {
			return ReflectionDecision{}, errors.New("physical operational memory path in outcome evidence")
		}
		for _, priorOutcome := range history {
			wire, err := protojson.Marshal(priorOutcome)
			if err != nil {
				return ReflectionDecision{}, err
			}
			if bytes.Contains(wire, []byte(layout.PhysicalPathMarker)) {
				return ReflectionDecision{}, errors.New("physical operational memory path in prior outcome evidence")
			}
		}
		wire, err := protojson.Marshal(request)
		if err != nil {
			return ReflectionDecision{}, err
		}
		if bytes.Contains(wire, []byte(layout.PhysicalPathMarker)) {
			return ReflectionDecision{}, errors.New("physical operational memory path leaked into reflection task")
		}
	}
	payload, err := anypb.New(request)
	if err != nil {
		return ReflectionDecision{}, err
	}
	identity := fmt.Sprintf("%x", sha256.Sum256([]byte(workspace.IncidentID+"\x00"+outcomeCommit)))
	proposalID := "reflection-" + identity
	task := &contracts.AgentTask{
		SchemaVersion: "sdo.dev/v1alpha1", TaskId: proposalID, AgentId: w.AgentID, TaskType: "outcome-reflection",
		Request: payload, Workspace: &contracts.ProposalWorkspace{ProposalId: proposalID, CheckoutPath: workspace.Worktree, BaseCommit: workspace.BaseCommit},
		Deadline: timestamppb.New(time.Now().Add(w.Timeout)),
	}
	result, err := executor.Execute(ctx, task)
	if err != nil {
		return ReflectionDecision{}, err
	}
	if result == nil || result.GetSchemaVersion() != task.SchemaVersion || result.GetTaskId() != task.TaskId || result.GetProposalId() != proposalID || result.GetStatus() != "completed" {
		return ReflectionDecision{}, errors.New("reflection worker returned a mismatched or incomplete result")
	}
	if result.GetResponse() == nil {
		return ReflectionDecision{}, errors.New("reflection worker omitted typed result")
	}
	response := &contracts.ReflectionResult{}
	if err := result.GetResponse().UnmarshalTo(response); err != nil {
		return ReflectionDecision{}, fmt.Errorf("reflection result contract: %w", err)
	}
	if response.GetLearningDecision() != "updated" && response.GetLearningDecision() != "no_change" {
		return ReflectionDecision{}, errors.New("reflection result requires an explicit learning decision")
	}
	if response.GetLearningDecision() == "no_change" && response.GetNoChangeReason() == "" {
		return ReflectionDecision{}, errors.New("no-change reflection requires a concrete reason")
	}
	if w.Catalog != nil {
		if err := memory.RequireCleanProposalCheckout(ctx, workspace.Worktree); err != nil {
			return ReflectionDecision{}, err
		}
		if response.GetLearningDecision() == "updated" {
			if response.GetArtifactProposal() == nil {
				return ReflectionDecision{}, errors.New("updated reflection omitted typed operational artifacts")
			}
			proposal := proto.Clone(response.GetArtifactProposal()).(*contracts.OperationalArtifactProposal)
			for _, detector := range proposal.GetDetectors() {
				if detector.GetOriginatingCommit() == "" {
					detector.OriginatingCommit = outcomeCommit
				}
			}
			if err := memory.MaterializeArtifacts(workspace.Worktree, proposal, memory.ResponderArtifacts); err != nil {
				return ReflectionDecision{}, fmt.Errorf("materialize reflection artifacts: %w", err)
			}
		} else if response.GetArtifactProposal() != nil {
			return ReflectionDecision{}, errors.New("no-change reflection cannot propose artifacts")
		}
	}
	return ReflectionDecision{LearningDecision: response.GetLearningDecision(), NoChangeReason: response.GetNoChangeReason()}, nil
}
