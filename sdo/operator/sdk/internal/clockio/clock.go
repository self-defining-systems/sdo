// Package clockio is the SDK's default wall-clock adapter.
package clockio

import (
	"context"
	"time"
)

type Clock struct{}

func (Clock) Now() time.Time                         { return time.Now() }
func (Clock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (Clock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
