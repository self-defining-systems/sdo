package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

const driftTaskType = "drift-investigation"

type RegisteredDriftInvestigator struct {
	Executor        ReflectionTaskExecutor
	AgentID         string
	ToolLoopFactory func(*contracts.DriftInvestigationTask) *ResponderLoop
}

func (worker RegisteredDriftInvestigator) Investigate(ctx context.Context, request *contracts.DriftInvestigationTask) (*contracts.DriftInvestigationResult, error) {
	if worker.Executor == nil || worker.AgentID == "" || request == nil || request.GetBatchId() == "" || request.GetAcceptedCommit() == "" ||
		request.GetMemorySnapshot() == nil || request.GetMemorySnapshot().GetAcceptedCommit() != request.GetAcceptedCommit() {
		return nil, errors.New("drift investigator requires executor, agent, batch ID, and accepted-commit-bound memory")
	}
	packed, err := anypb.New(request)
	if err != nil {
		return nil, fmt.Errorf("pack drift investigation: %w", err)
	}
	task := &contracts.AgentTask{
		SchemaVersion: ProtocolSchemaVersion, TaskId: request.BatchId, AgentId: worker.AgentID, TaskType: driftTaskType,
		Request: packed, Workspace: &contracts.ProposalWorkspace{ProposalId: request.BatchId, CheckoutPath: request.RepositoryWorktree, BaseCommit: request.AcceptedCommit},
	}
	executor := worker.Executor
	if job, ok := executor.(worker_registry.KubernetesExecutor); ok {
		subpath, err := memory.PrepareIncidentSession(job.RepositoryMountPath, job.RepositoryPVCSubPath, request.BatchId)
		if err != nil {
			return nil, err
		}
		job.SessionPVCSubPath = subpath
		job.CapturedCorpusReadOnly = true
		if worker.ToolLoopFactory != nil {
			loop := worker.ToolLoopFactory(request)
			if loop == nil {
				return nil, errors.New("drift investigator tool loop is unavailable")
			}
			captureDir, err := memory.IncidentCorpusDir(job.RepositoryMountPath, request.BatchId)
			if err != nil {
				return nil, err
			}
			server := SessionToolServer{Directory: filepath.Join(filepath.Dir(captureDir), "tools"), Handle: driftReadOnlyToolHandle(loop)}
			if err := server.Prepare(); err != nil {
				return nil, err
			}
			toolCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() { _ = server.Serve(toolCtx) }()
			environment := job.Environment
			job.Environment = make(map[string]string, len(environment)+1)
			for name, value := range environment {
				job.Environment[name] = value
			}
			job.Environment["SDO_TOOLS_DIR"] = "/sdo/session/tools"
		}
		executor = job
	}
	response, err := executor.Execute(ctx, task)
	if err != nil {
		return nil, err
	}
	if response == nil || response.GetTaskId() != task.TaskId || response.GetStatus() != "completed" || response.GetProposalId() != request.BatchId {
		return nil, errors.New("drift investigator returned mismatched or incomplete result")
	}
	result := new(contracts.DriftInvestigationResult)
	if response.GetResponse() == nil || response.GetResponse().UnmarshalTo(result) != nil {
		return nil, errors.New("drift investigator omitted typed result")
	}
	return result, nil
}

func driftReadOnlyToolHandle(loop *ResponderLoop) func(context.Context, ToolRequest) ToolResponse {
	return func(ctx context.Context, call ToolRequest) ToolResponse {
		if call.Command != "status" && call.Command != "diagnose" {
			return ToolResponse{Command: call.Command, Error: "drift investigation permits only status and diagnose"}
		}
		return loop.Handle(ctx, call)
	}
}
