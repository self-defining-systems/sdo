package runtime

import (
	"context"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func reflectionNoChangeResult(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "independent cause evidence unavailable"})
	return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
}
