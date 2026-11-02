package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func fastSyncPolicy() InformerSyncPolicy {
	return InformerSyncPolicy{Attempts: 3, Timeout: 100 * time.Millisecond, Backoff: time.Millisecond, Budget: 5 * time.Second}
}

func TestInformerSyncPolicyRetriesTransientFailure(t *testing.T) {
	var calls atomic.Int32
	err := fastSyncPolicy().Do(context.Background(), 0, func(context.Context) error {
		if calls.Add(1) < 3 {
			return &InformerSyncError{Kinds: []string{"v1/Pod"}, Err: context.DeadlineExceeded}
		}
		return nil
	}, nil)
	if err != nil || calls.Load() != 3 {
		t.Fatalf("want success on the third attempt, got calls=%d err=%v", calls.Load(), err)
	}
}

func TestInformerSyncPolicyReturnsTypedErrorAfterBoundedAttempts(t *testing.T) {
	var calls atomic.Int32
	var retried []int
	err := fastSyncPolicy().Do(context.Background(), 0, func(context.Context) error {
		calls.Add(1)
		return &InformerSyncError{Kinds: []string{"v1/Pod"}, Err: context.DeadlineExceeded}
	}, func(attempt int, _ error) { retried = append(retried, attempt) })
	var syncErr *InformerSyncError
	if !errors.As(err, &syncErr) || len(syncErr.Kinds) != 1 || syncErr.Kinds[0] != "v1/Pod" {
		t.Fatalf("want the typed error naming the kind, got %v", err)
	}
	if calls.Load() != 3 || len(retried) != 2 {
		t.Fatalf("want 3 attempts and 2 retries, got calls=%d retries=%v", calls.Load(), retried)
	}
}

func TestInformerSyncPolicyRejectsInvalidConfiguration(t *testing.T) {
	for _, policy := range []InformerSyncPolicy{
		{Attempts: -1}, {Timeout: -1}, {Backoff: -1}, {Budget: -1},
	} {
		if err := policy.Validate(); err == nil {
			t.Fatalf("%+v must be rejected", policy)
		}
	}
	if err := (InformerSyncPolicy{}).Validate(); err != nil {
		t.Fatalf("zero value means defaults: %v", err)
	}
	if policy := (InformerSyncPolicy{}).withDefaults(); policy.Attempts != 3 || policy.Budget > 3*time.Minute {
		t.Fatalf("defaults must stay within three attempts and three minutes: %+v", policy)
	}
}
