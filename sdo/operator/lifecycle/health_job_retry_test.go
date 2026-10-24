package lifecycle

import (
	"context"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/worker_registry"
)

func TestFailedHealthJobGetsFreshBoundedTask(t *testing.T) {
	c, b, e, _ := testCoordinator(t)
	e.judgeJobFailures = 1
	_, err := c.Run(context.Background(), Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	requests := judgeRequests(t, e)
	if len(requests) != 2 || requests[1].GetValidationAttempt() != 2 {
		t.Fatalf("failed health Job did not advance to a fresh attempt: %+v", requests)
	}
	var names []string
	for _, task := range e.tasks {
		if task.GetTaskType() == healthJudgeTask {
			names = append(names, worker_registry.JobName(task.GetTaskId()))
		}
	}
	if names[0] == names[1] {
		t.Fatal("retained failed Job was reattached instead of starting a fresh Job")
	}
	log, _ := b.Rejections("op-judge-1")
	if len(log.GetRejections()) != 1 {
		t.Fatal("failed Job retry was not recorded durably")
	}
}

func TestFailedHealthJobExhaustionSurvivesRestart(t *testing.T) {
	c, _, e, _ := testCoordinator(t)
	e.judgeJobFailures = 100
	cfg := Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1}
	if _, err := c.Run(context.Background(), cfg); err == nil {
		t.Fatal("expected bounded worker failure")
	}
	before := len(judgeRequests(t, e))
	if before != 1+broker.MaxValidationRetries {
		t.Fatalf("attempts = %d, want %d", before, 1+broker.MaxValidationRetries)
	}
	if _, err := c.Run(context.Background(), cfg); err == nil {
		t.Fatal("restart forgot exhausted worker attempts")
	}
	if got := len(judgeRequests(t, e)); got != before {
		t.Fatalf("restart dispatched more health Jobs: %d -> %d", before, got)
	}
}
