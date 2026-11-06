package rendered

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
)

// SourceErrorKind classifies a failure to read or render the accepted source
// by what a caller does next, not by where it happened.
type SourceErrorKind int

const (
	// SourceUnconfigured: no deployment source (no manifest paths) is
	// configured. Callers skip the work permanently.
	SourceUnconfigured SourceErrorKind = iota + 1
	// SourceInvalid: the source cannot be read as configured (bad selection,
	// decode failure, limit, duplicate ref). Permanent for this commit.
	SourceInvalid
	// SourceUnavailable: git I/O or context cancellation. Transient; retry with
	// backoff. Unavailable is not absent.
	SourceUnavailable
)

func (k SourceErrorKind) String() string {
	switch k {
	case SourceUnconfigured:
		return "unconfigured"
	case SourceInvalid:
		return "invalid"
	case SourceUnavailable:
		return "unavailable"
	}
	return fmt.Sprintf("SourceErrorKind(%d)", int(k))
}

// ErrSourceUnconfigured, ErrSourceInvalid and ErrSourceUnavailable match any
// *SourceError of the kind through errors.Is.
var (
	ErrSourceUnconfigured = errors.New("deployment source is not configured")
	ErrSourceInvalid      = errors.New("deployment source is invalid")
	ErrSourceUnavailable  = errors.New("deployment source is unavailable")
)

// SourceError carries a SourceErrorKind and the underlying cause. Its message
// is the cause's message.
type SourceError struct {
	Kind SourceErrorKind
	Err  error
}

func (e *SourceError) Error() string {
	if e.Err == nil {
		return e.Kind.String()
	}
	return e.Err.Error()
}

func (e *SourceError) Unwrap() error { return e.Err }

func (e *SourceError) Is(target error) bool {
	switch e.Kind {
	case SourceUnconfigured:
		return target == ErrSourceUnconfigured
	case SourceInvalid:
		return target == ErrSourceInvalid
	case SourceUnavailable:
		return target == ErrSourceUnavailable
	}
	return false
}

func sourceErrorf(kind SourceErrorKind, format string, args ...any) error {
	return &SourceError{Kind: kind, Err: fmt.Errorf(format, args...)}
}

// classifySource makes err a *SourceError. An error that already carries a
// kind keeps it; context errors and failed file reads are Unavailable (not
// found and invalid paths are verdicts on the source); anything else is Invalid.
func classifySource(err error) error {
	if err == nil {
		return nil
	}
	var existing *SourceError
	if errors.As(err, &existing) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &SourceError{Kind: SourceUnavailable, Err: err}
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, fs.ErrInvalid) {
		// A failed read of an input is I/O, not a verdict on the source.
		return &SourceError{Kind: SourceUnavailable, Err: err}
	}
	return &SourceError{Kind: SourceInvalid, Err: err}
}
