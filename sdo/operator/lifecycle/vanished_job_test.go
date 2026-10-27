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
	if len(log.GetRejections()) == 0 || log.GetRejections()[0].GetDiagnostics()[0].GetRule() != "worker.job_vanished" {
		t.Fatalf("vanished attempts were not recorded as typed failures: %v", log)
	}
	if got := len(judgeRequests(t, e)); got > 2*(1+broker.MaxValidationRetries) {
		t.Fatalf("vanished worker was dispatched %d times", got)
	}
}
