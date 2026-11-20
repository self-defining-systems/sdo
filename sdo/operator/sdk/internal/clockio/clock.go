// Package clockio is the SDK's default wall-clock adapter.
package clockio

import (
	"context"
	"math/rand/v2"
	"time"
)

type Clock struct{}

func (Clock) Now() time.Time                         { return time.Now() }
func (Clock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (Clock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

// RandomBits returns 64 bits from the process random source.
func RandomBits() uint64 { return rand.Uint64() }
