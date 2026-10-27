package lifecycle

import (
	"context"
	"testing"

	"sdo.dev/operator/broker"
)

// A Job lost to an earlier killed attempt must not fail the lifecycle: the
// stale dispatch is discarded by its owner and the same task runs again.
func TestVanishedLifecycleWorkerIsDiscardedAndRedispatched(t *testing.T) {
	c, _, e, _ := testCoordinator(t)
	e.judgeJobsVanished = 1
	_, err := c.Run(context.Background(), Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err != nil {
		t.Fatalf("vanished worker failed the lifecycle: %v", err)
	}
	requests := judgeRequests(t, e)
	if len(requests) != 2 || len(e.discarded) != 1 {
		t.Fatalf("vanished judge was not discarded and redispatched once: requests=%d discarded=%v", len(requests), e.discarded)
	}
	if e.tasks[len(e.tasks)-1].GetTaskId() != e.discarded[0] {
		t.Fatalf("redispatch used a different task than the discarded one: %v", e.discarded)
	}
}

// A worker that vanishes again after redispatch is a recorded failed attempt
// on a fresh task, bounded by the validation retries, never an unbounded loop.
func TestRepeatedlyVanishedHealthWorkerIsRecordedAndBounded(t *testing.T) {
	c, b, e, _ := testCoordinator(t)
	e.judgeJobsVanished = 1000
	_, err := c.Run(context.Background(), Config{OperationID: "op", Application: "demo", Namespace: "demo", HealthObjective: "serve", DeployerAgentID: "source-agent", JudgeAgentID: "judge-agent", MaxAttempts: 1})
	if err == nil {
		t.Fatal("expected failure when the worker keeps vanishing")
	}
	log, _ := b.Rejections("op-judge-1")
	if !log.GetTerminal() || len(log.GetRejections()) != 1+broker.MaxValidationRetries {
		t.Fatalf("want a terminal log of %d rejections, got %v", 1+broker.MaxValidationRetries, log)
	}
	for _, rejection := range log.GetRejections() {
		if rejection.GetDiagnostics()[0].GetRule() != vanishedWorkerRule {
			t.Fatalf("vanished attempt recorded as %v", rejection)
		}
	}
	// Every attempt is a fresh task, dispatched at most twice (once, then redispatched).
	ids := map[string]int{}
	for _, task := range e.tasks {
		if task.GetTaskType() == healthJudgeTask {
			ids[task.GetTaskId()]++
		}
	}
	if len(ids) != 1+broker.MaxValidationRetries {
		t.Fatalf("attempts did not use fresh tasks: %v", ids)
	}
	for id, n := range ids {
		if n != 2 {
			t.Fatalf("task %s dispatched %d times, want 2", id, n)
		}
	}
	if _, ok := ids["op-judge-1-retry1"]; !ok {
		t.Fatalf("retry task IDs missing: %v", ids)
	}
}
