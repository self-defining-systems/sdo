package runtime

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	kclock "k8s.io/utils/clock"
)

// A controller outlives the API errors that clear on their own. An error from an
// external call (the Kubernetes API, the network) that a later attempt can clear
// is retried with backoff and never ends the process; only errors that retrying
// cannot fix (bad configuration, a violated contract, a refused request) leave Run.

// transientAPIError reports an error that a later attempt can clear: an API
// server that answered 5xx or 429 or timed out, or a connection that dropped.
// Cancellation is not transient: the caller is shutting down.
func transientAPIError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	// client-go's token bucket refuses a wait that cannot finish before the
	// caller's deadline with a plain error that wraps no context error. It
	// reports local throttling or a stalled process, never a server verdict.
	if strings.Contains(err.Error(), "would exceed context deadline") {
		return true
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) ||
		apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsUnexpectedServerError(err) {
		return true
	}
	var status apierrors.APIStatus
	return errors.As(err, &status) && status.Status().Code >= 500
}

const (
	transientRetryBase = time.Second
	transientRetryCap  = 15 * time.Second
)

// transientBackoff paces retries of consecutive transient failures: one second,
// doubling, up to fifteen. A success resets it.
type transientBackoff struct{ failures int }

func (b *transientBackoff) next() time.Duration {
	delay := transientRetryBase << min(b.failures, 8)
	b.failures++
	return min(delay, transientRetryCap)
}

func (b *transientBackoff) reset() { b.failures = 0 }

// retryTransient calls call until it succeeds or fails with an error that is not
// transient. report receives every transient failure. It returns early with the
// last error when ctx ends.
func retryTransient(ctx context.Context, clock kclock.WithTicker, report func(error), call func() error) error {
	var backoff transientBackoff
	for {
		err := call()
		if err == nil || ctx.Err() != nil || !transientAPIError(err) {
			return err
		}
		if report != nil {
			report(err)
		}
		select {
		case <-ctx.Done():
			return err
		case <-clock.After(backoff.next()):
		}
	}
}
