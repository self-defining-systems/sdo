// Package jobio owns the BuildKit Job adapters: the random Job name suffix and
// the poll timer.
package jobio

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
)

// Wait sleeps for d or until ctx ends.
func Wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// JobSuffix returns a random suffix for a BuildKit Job name.
func JobSuffix() string { return rand.String(8) }
