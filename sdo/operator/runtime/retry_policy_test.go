package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/memory"
)

func TestRetryLedgerBacksOffExponentiallyAndGivesUpAtTheBound(t *testing.T) {
	ledger := newRetryLedger(boundedRetry{First: 2 * time.Second, Max: 10 * time.Second, Bound: 30 * time.Second})
	start := time.Unix(1000, 0).UTC()
	now := start
	var delays []time.Duration
	for range 20 {
		next, exhausted := ledger.failed("subject", now)
		if exhausted {
			if got := now.Sub(start); got < 30*time.Second {
				t.Fatalf("gave up at %s, before the 30s bound", got)
			}
			if want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 6 * time.Second}; fmt.Sprint(delays) != fmt.Sprint(want) {
				t.Fatalf("delays = %v, want %v (doubling, the cap, then the retry that lands on the bound)", delays, want)
			}
			if ledger.attempts("subject") != 0 {
				t.Fatal("an exhausted subject keeps its streak")
			}
			return
		}
		delays = append(delays, next.Sub(now))
		now = next
	}
	t.Fatalf("never gave up; delays %v", delays)
}

func TestRetryLedgerKeysAreIndependentAndClearable(t *testing.T) {
	ledger := newRetryLedger(boundedRetry{First: time.Second, Max: time.Minute, Bound: time.Minute})
	now := time.Unix(1000, 0).UTC()
	ledger.failed("a", now)
	ledger.failed("a", now.Add(time.Second))
	if next, _ := ledger.failed("b", now.Add(2*time.Second)); next.Sub(now.Add(2*time.Second)) != time.Second {
		t.Fatalf("subject b's first retry = %s after its failure, want 1s: streaks must not share state", next.Sub(now.Add(2*time.Second)))
	}
	ledger.cleared("a")
	if ledger.attempts("a") != 0 {
		t.Fatal("cleared subject keeps its streak")
	}
}

func TestClassifyPermanent(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"unreadable memory":     {fmt.Errorf("read: %w", memory.ErrUnreadable), true},
		"snapshot budget":       {fmt.Errorf("read: %w", memory.ErrSnapshotBudget), true},
		"snapshot incomplete":   {memory.ErrSnapshotIncomplete, true},
		"marked permanent":      {fmt.Errorf("%w: bad request", ErrDispatchPermanent), true},
		"deadline":              {fmt.Errorf("read: %w", context.DeadlineExceeded), false},
		"cancelled":             {context.Canceled, false},
		"deadline over a class": {fmt.Errorf("%w: %w", memory.ErrUnreadable, context.DeadlineExceeded), false},
		"unclassed":             {errors.New("git: exit status 128"), false},
		"nil":                   {nil, false},
	} {
		if got := classifyPermanent(tc.err); got != tc.want {
			t.Errorf("%s: classifyPermanent = %v, want %v", name, got, tc.want)
		}
	}
}
