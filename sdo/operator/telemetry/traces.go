package telemetry

import (
	"context"
	"fmt"
	"time"
)

// Trace reads are a separate, read-only surface of a source that serves
// KindTraces: detectors never query traces, the responder's `sdo traces` does.
// The types are the normalized form every trace adapter produces.

// TraceSummary is one recent trace in a listing.
type TraceSummary struct {
	ID    string
	Start time.Time
	// Duration is the span of the whole trace.
	Duration time.Duration
	// RootService, RootOperation and Route describe the root span; Route is
	// the HTTP route or path when the root span carries one.
	RootService   string
	RootOperation string
	Route         string
	Services      []string
	Spans         int
	Failed        bool
	// FirstFailure names the earliest failing span ("service operation: why").
	FirstFailure string
}

// Span is one span of a trace.
type Span struct {
	ID       string
	ParentID string
	Service  string
	// Operation is the span name; Statement is a database statement or query
	// text when the span carries one.
	Operation string
	Statement string
	// Route is the HTTP route or path the span carries, when it does.
	Route    string
	Start    time.Time
	Duration time.Duration
	Failed   bool
	// Error says why a failed span failed, when the span says.
	Error string
}

// Trace is one trace with all its spans.
type Trace struct {
	ID    string
	Spans []Span
}

// TraceQuery selects recent traces.
type TraceQuery struct {
	// End is the end of the window (the caller's clock); Lookback is how far
	// back from it to list.
	End      time.Time
	Lookback time.Duration
	// Failing keeps only traces with a failed span.
	Failing bool
	// Limit bounds the traces per service; zero means the adapter's default.
	Limit int
}

// TraceReader is the optional interface of a client whose source serves
// KindTraces. Listings are newest first.
type TraceReader interface {
	// Services names the services that emitted traces.
	Services(ctx context.Context) ([]string, error)
	Traces(ctx context.Context, q TraceQuery) ([]TraceSummary, error)
	Trace(ctx context.Context, id string) (Trace, error)
}

// TraceReader resolves the source that serves traces (the named one, or the
// default) and returns its reader.
func (r *Resolver) TraceReader(sourceID string) (TraceReader, error) {
	i, err := r.index(sourceID, KindTraces)
	if err != nil {
		return nil, err
	}
	reader, ok := r.clients[i].(TraceReader)
	if !ok {
		return nil, fmt.Errorf("telemetry source %q serves traces but its client cannot list them", r.sources[i].ID)
	}
	return reader, nil
}
