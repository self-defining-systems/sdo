package runtime

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
)

// reflectionCheckExecutor serves trusted validation only while this incident's
// worker runs. Server termination is joined before its checkout can be released.
type reflectionCheckExecutor struct {
	Executor  ReflectionTaskExecutor
	Directory string
	Handle    func(context.Context, ToolRequest) ToolResponse
}

func (e reflectionCheckExecutor) Execute(ctx context.Context, task *contracts.AgentTask) (result *contracts.AgentResult, err error) {
	server := SessionToolServer{Directory: e.Directory, Handle: e.Handle, MaxRequestBytes: 2 * 1024 * 1024}
	if err := server.Prepare(); err != nil {
		return nil, fmt.Errorf("prepare reflection check endpoint: %w", err)
	}
	serving, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { err := server.Serve(serving); stop(); done <- err }()
	defer func() {
		stop()
		serverErr := <-done
		if err == nil {
			err = serverErr
		}
	}()
	return e.Executor.Execute(serving, task)
}

func (e reflectionCheckExecutor) RecoverCompleted(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, bool, error) {
	if recoverer, ok := e.Executor.(worker_registry.ReflectionResultRecoverer); ok {
		return recoverer.RecoverCompleted(ctx, task)
	}
	return nil, false, nil
}

func reflectionCheckHandler(repository *broker.Broker, sessionRoot, incidentID string) func(context.Context, ToolRequest) ToolResponse {
	return func(ctx context.Context, request ToolRequest) ToolResponse {
		response := ToolResponse{Command: request.Command}
		if request.Command != "memory-check" {
			response.Error = "reflection endpoint only accepts memory-check"
			return response
		}
		decision := new(contracts.ReflectionDecision)
		if err := protojson.Unmarshal([]byte(request.Parameters["decision"]), decision); err != nil {
			response.Error = err.Error()
			return response
		}
		if sessionRoot != "" {
			capture, err := memory.IncidentCorpusDir(sessionRoot, incidentID)
			if err != nil {
				response.Error = err.Error()
				return response
			}
			ctx = validate.WithCaptureSource(ctx, capture, incidentID)
		}
		bounded, cancel := context.WithTimeout(ctx, 300*time.Second)
		defer cancel()
		report, err := repository.CheckReflection(bounded, incidentID, decision)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(bounded.Err(), context.DeadlineExceeded) {
				response.MemoryCheckTimedOut = true
			}
			response.Error = err.Error()
			return response
		}
		response.MemoryCheckReport, err = protojson.Marshal(report)
		if err != nil {
			response.Error = err.Error()
		}
		return response
	}
}
