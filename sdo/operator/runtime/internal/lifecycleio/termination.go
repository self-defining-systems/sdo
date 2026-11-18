package lifecycleio

import (
	"context"
	"os/signal"
	"syscall"
)

// TerminationContext ends when the process receives SIGTERM or SIGINT, so the
// controller can stop acting and release its leader Lease before it exits.
func TerminationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
}
