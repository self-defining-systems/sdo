// Package telemetry is the seam between SDO and application-telemetry
// backends (metrics, logs, traces). Core passes query text through and never
// parses a dialect, exactly as database/sql passes SQL text through: the
// detector author knows the dialect of the source a query names.
//
// "Application telemetry" is what the application reports and the backends that
// store it. It is unrelated to the controller's own firing telemetry.
//
// This package holds the interface only. Adapters live in subpackages, the
// conformance suite in telemetrytest, and nothing in this module performs I/O.
package telemetry

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"sdo.dev/operator/sdk/metrics"
)

// Kind is a class of telemetry signal.
type Kind string

const (
	KindMetrics Kind = "metrics"
	KindLogs    Kind = "logs"
	KindTraces  Kind = "traces"
)

// Valid reports whether k is a known kind.
func (k Kind) Valid() bool {
	switch k {
	case KindMetrics, KindLogs, KindTraces:
		return true
	}
	return false
}

// Capability is an optional feature of a source beyond answering queries.
type Capability string

const (
	// CapAlerts means the client also implements AlertReader.
	CapAlerts Capability = "alerts"
	// CapRange means the source answers queries with a non-zero Range.
	CapRange Capability = "range"
)

// Valid reports whether c is a known capability.
func (c Capability) Valid() bool { return c == CapAlerts || c == CapRange }

// Origin says who introduced a source into the effective set.
type Origin string

const (
	OriginOperator        Origin = "operator"
	OriginDesignerBackend Origin = "designer-backend"
	OriginAppShipped      Origin = "app-shipped"
)

// Valid reports whether o is a known origin.
func (o Origin) Valid() bool {
	return o == OriginOperator || o == OriginDesignerBackend || o == OriginAppShipped
}

// Bounds shared by every adapter. Metrics bounds come from sdk/metrics so the
// detector-facing and adapter-facing limits cannot drift.
const (
	MaxSeries    = metrics.MaxSeries
	MaxSamples   = metrics.MaxSamples
	MaxLines     = 1000
	MaxLineBytes = 8 * 1024
	// MaxTextBytes bounds one query text.
	MaxTextBytes = metrics.MaxExprBytes
	// QueryTimeout is the per-query deadline the runtime enforces even for a
	// client that ignores its context.
	QueryTimeout = 5 * time.Second
)

// Source is one bound backend as detectors and agents see it.
type Source struct {
	ID      string
	Kinds   []Kind
	Adapter string
	// Dialect is descriptor text for prompts, for example
	// "PromQL (Prometheus 3 HTTP API)".
	Dialect      string
	Default      bool
	Capabilities []Capability
	Origin       Origin
}

// Serves reports whether the source answers kind k.
func (s Source) Serves(k Kind) bool {
	for _, have := range s.Kinds {
		if have == k {
			return true
		}
	}
	return false
}

// ServesAlerts reports whether the source answers alert reads: an alerts
// capability on a metrics source. The resolver's alert sources and the
// condition for installing the generic alerts detector are this one predicate.
func (s Source) ServesAlerts() bool { return s.Serves(KindMetrics) && s.Has(CapAlerts) }

// Has reports whether the source advertises capability c.
func (s Source) Has(c Capability) bool {
	for _, have := range s.Capabilities {
		if have == c {
			return true
		}
	}
	return false
}

// Query is the adapter-facing request. Text is opaque to core.
type Query struct {
	// SourceID names a bound source; empty means the default source for Kind.
	SourceID string
	Kind     Kind
	Text     string
	// Range is zero for an instant metrics query or the latest window of logs.
	Range time.Duration
	Step  time.Duration
	// Limit bounds lines or spans; adapters must honor it and report Truncated.
	// Zero means the package maximum.
	Limit int
}

// EffectiveLimit is Limit, or the kind's maximum when Limit is zero, and never
// more than that maximum.
func (q Query) EffectiveLimit() int {
	max := MaxLines
	if q.Limit > 0 && q.Limit < max {
		return q.Limit
	}
	return max
}

// Validate checks a query in isolation, before any source is resolved.
func (q Query) Validate() error {
	if !q.Kind.Valid() {
		return fmt.Errorf("telemetry query has unknown kind %q", q.Kind)
	}
	if strings.TrimSpace(q.Text) == "" {
		return errors.New("telemetry query has empty text")
	}
	if len(q.Text) > MaxTextBytes {
		return fmt.Errorf("telemetry query text exceeds %d bytes", MaxTextBytes)
	}
	if q.Range < 0 || q.Step < 0 || q.Limit < 0 {
		return errors.New("telemetry query range, step and limit must not be negative")
	}
	if q.Range > metrics.MaxRange {
		return fmt.Errorf("telemetry query range exceeds %s", metrics.MaxRange)
	}
	if q.Range == 0 && q.Step != 0 {
		return errors.New("telemetry query step needs a range")
	}
	return nil
}

// Line is one log line.
type Line struct {
	At     time.Time
	Text   string
	Labels map[string]string
}

// Result is normalized and bounded. Exactly the payload field matching the
// query Kind may be non-empty; "no data" is an empty Result, never an error.
type Result struct {
	Series    []metrics.Series
	Lines     []Line
	Truncated bool
}

// Empty reports whether the result carries no payload.
func (r Result) Empty() bool { return len(r.Series) == 0 && len(r.Lines) == 0 }

// Validate checks that r respects the contract for kind k: only the matching
// payload is set, bounds hold and sample values are finite.
func (r Result) Validate(k Kind) error {
	switch k {
	case KindMetrics:
		if len(r.Lines) != 0 {
			return errors.New("metrics result carries log lines")
		}
		if len(r.Series) > MaxSeries {
			return fmt.Errorf("result has %d series, over the %d bound", len(r.Series), MaxSeries)
		}
		samples := 0
		for _, s := range r.Series {
			samples += len(s.Samples)
			for _, p := range s.Samples {
				if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
					return fmt.Errorf("series %s has a non-finite sample", metrics.LabelString(s.Labels))
				}
			}
		}
		if samples > MaxSamples {
			return fmt.Errorf("result has %d samples, over the %d bound", samples, MaxSamples)
		}
	case KindLogs:
		if len(r.Series) != 0 {
			return errors.New("logs result carries metric series")
		}
		if len(r.Lines) > MaxLines {
			return fmt.Errorf("result has %d lines, over the %d bound", len(r.Lines), MaxLines)
		}
		for _, l := range r.Lines {
			if len(l.Text) > MaxLineBytes {
				return fmt.Errorf("a log line exceeds %d bytes", MaxLineBytes)
			}
		}
	default:
		return fmt.Errorf("no result contract for kind %q yet", k)
	}
	return nil
}
