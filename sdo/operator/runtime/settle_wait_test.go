package runtime

import (
	"context"
	"testing"
	"time"
)

func TestTrustedFastPathUsesExecutorSettleWait(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, true, true)
	var waited time.Duration
	loop.Executor.Sleep = func(_ context.Context, delay time.Duration) error { waited = delay; return context.Canceled }
	result := loop.TryTrustedFastPath(context.Background(), 3)
	if waited != loop.DiagnosisSettleDelay || result.SkippedReason != FastPathSkipSettleCancelled {
		t.Fatalf("wait %s, result %+v", waited, result)
	}
}

func TestInitialDiagnosisRetryUsesExecutorSettleWait(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, true, true)
	var waited time.Duration
	loop.Executor.Sleep = func(_ context.Context, delay time.Duration) error { waited = delay; return context.Canceled }
	loop.RetryInitialDiagnose(context.Background())
	if waited != loop.DiagnosisSettleDelay {
		t.Fatalf("wait %s, want %s", waited, loop.DiagnosisSettleDelay)
	}
}

func TestPlanRetryBackoffUsesInjectedJitter(t *testing.T) {
	executor := PlanExecutor{Jitter: func(bound int64) int64 { return bound - 1 }}
	if got := executor.applyBackoff(3); got != 120*time.Millisecond-time.Nanosecond {
		t.Fatalf("backoff %s", got)
	}
}
