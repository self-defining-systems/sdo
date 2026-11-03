package memoryhost

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestCapturedHostKeepsFailureAcrossConcurrentSuccess(t *testing.T) {
	host := &capturedHost{}
	var calls sync.WaitGroup
	for range 16 {
		calls.Go(func() { host.record(context.Background(), errors.New("synthetic transport failure")) })
		calls.Go(func() { host.record(context.Background(), nil) })
	}
	calls.Wait()
	host.record(context.Background(), nil)
	if !host.transportFailed() {
		t.Fatal("successful call erased another call's transport failure")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	healthy := &capturedHost{}
	healthy.record(cancelled, context.Canceled)
	if healthy.transportFailed() {
		t.Fatal("caller cancellation retired a healthy host")
	}
}
