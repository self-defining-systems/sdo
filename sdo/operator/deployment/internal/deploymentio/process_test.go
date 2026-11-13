package deploymentio

import (
	"context"
	"testing"
	"time"
)

// A descendant that outlives the process and keeps its output pipe open (the
// memory host a detector evaluation launched in its own process group) must
// not hold the call past the context bound.
func TestCallsReturnWhenDescendantHoldsOutputPipe(t *testing.T) {
	script := []string{"-c", "sleep 30 & echo started"}
	calls := map[string]func(context.Context) error{
		"Evaluate": func(ctx context.Context) error {
			_, _, err := Evaluate(ctx, t.TempDir(), "sh", script, nil)
			return err
		},
		"Run": func(ctx context.Context) error {
			_, err := Run(ctx, "sh", script, nil)
			return err
		},
		"Output": func(ctx context.Context) error {
			_, err := Output(ctx, "sh", script)
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			guard, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			done := make(chan struct{})
			go func() { _ = call(ctx); close(done) }()
			select {
			case <-done:
			case <-guard.Done():
				t.Fatalf("%s waited on a descendant holding its output pipe", name)
			}
		})
	}
}
