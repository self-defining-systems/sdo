package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry/registrytest"
	"testing"
)

type reflectionTaskExecutorFunc func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)

func (f reflectionTaskExecutorFunc) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	return f(ctx, task)
}

// reflectionTaskType is the task type the built-in manifests register for
// reflection requests.
func reflectionTaskType(t testing.TB) string {
	t.Helper()
	registry := registrytest.BuiltIn(t)
	agent, err := registry.AgentFor(&contracts.ReflectionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return registrytest.TaskType(t, agent, &contracts.ReflectionRequest{})
}
