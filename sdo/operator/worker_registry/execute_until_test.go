package worker_registry

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// executeCeiling bounds how long a test waits for a condition. It is a failure
// ceiling, never the time the test expects Execute to need.
const executeCeiling = 30 * time.Second

// executeUntil runs Execute-like work, which polls until its context ends,
// and cancels it as soon as ready holds. A fixed short deadline instead races
// the work against the scheduler: the race detector under load can outlast it.
// It fails the test if ready never holds within executeCeiling.
func executeUntil[T any](t *testing.T, ready func() bool, run func(context.Context) (T, error)) (T, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := run(ctx)
		done <- outcome{value, err}
	}()
	for deadline := time.Now().Add(executeCeiling); !ready(); time.Sleep(time.Millisecond) {
		select {
		case result := <-done:
			// Finished by itself: the caller's assertions decide what that means.
			return result.value, result.err
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("condition not reached within %v", executeCeiling)
		}
	}
	cancel()
	result := <-done
	return result.value, result.err
}

// jobExists reports whether the named Job has been created.
func jobExists(e KubernetesExecutor, name string) func() bool {
	return func() bool {
		_, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), name, metav1.GetOptions{})
		return err == nil
	}
}
