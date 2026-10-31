// Package logs exposes bounded, runtime-counted container logs to deterministic
// detectors. No log I/O or pattern matching occurs in detectors.
package logs

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// Watch schedules log consumers on their interval; no informer backs it.
var Watch = sdk.WatchKind{APIVersion: "sdo.dev/v1alpha1", Kind: "Logs"}

// Limits bound declarations, collection, retained history and evidence.
// MaxPatterns bounds both each query and total declared patterns across queries.
const (
	MaxQueries      = 32
	MaxPatterns     = 32
	MaxPatternBytes = 1024
	MaxIDBytes      = 128
	LimitBytes      = 64 * 1024
	WindowCycles    = 20
	MaxExcerpts     = 3
	MaxExcerptBytes = 256
	MaxContainers   = 256
)

// Pattern names an RE2 expression evaluated by the runtime once per line.
type Pattern struct {
	ID  string
	RE2 string
}

// Query declares patterns, optional previous logs and completed-container tails.
type Query struct {
	ID       string
	Patterns []Pattern
	Previous bool
	// Terminations requests tails retained across incarnation window resets.
	Terminations bool
}

// Consumer declares a detector's log inputs.
type Consumer interface{ LogQueries() []Query }

// Validate checks a query's identifiers, expressions and bounds.
func (q Query) Validate() error {
	if !validID(q.ID) {
		return fmt.Errorf("invalid log query id %q", q.ID)
	}
	if len(q.Patterns) > MaxPatterns {
		return fmt.Errorf("log query %q exceeds %d patterns", q.ID, MaxPatterns)
	}
	seen := map[string]bool{}
	for _, p := range q.Patterns {
		if !validID(p.ID) || seen[p.ID] {
			return fmt.Errorf("invalid or duplicate pattern id %q", p.ID)
		}
		seen[p.ID] = true
		if strings.TrimSpace(p.RE2) == "" || len(p.RE2) > MaxPatternBytes {
			return fmt.Errorf("invalid pattern %q length", p.ID)
		}
		if _, err := regexp.Compile(p.RE2); err != nil {
			return fmt.Errorf("pattern %q: %w", p.ID, err)
		}
	}
	return nil
}
func validID(id string) bool { return id != "" && len(id) <= MaxIDBytes && strings.TrimSpace(id) == id }

// ValidateQueries rejects malformed, duplicate or excessive declarations.
func ValidateQueries(qs []Query) error {
	if len(qs) > MaxQueries {
		return fmt.Errorf("log queries exceed %d", MaxQueries)
	}
	seen := map[string]bool{}
	patternCount := 0
	for _, q := range qs {
		if err := q.Validate(); err != nil {
			return err
		}
		if seen[q.ID] {
			return fmt.Errorf("duplicate log query %q", q.ID)
		}
		seen[q.ID] = true
		patternCount += len(q.Patterns)
		if patternCount > MaxPatterns {
			return fmt.Errorf("log patterns exceed %d declarations", MaxPatterns)
		}
	}
	return nil
}

// ValidateDetector validates a log consumer at registration.
func ValidateDetector(d sdk.Detector) error {
	if c, ok := d.(Consumer); ok {
		return ValidateQueries(c.LogQueries())
	}
	return nil
}

// Queries merges identical query IDs and rejects conflicting definitions.
func Queries(ds []sdk.Detector) ([]Query, error) {
	byID := map[string]Query{}
	for _, d := range ds {
		if err := ValidateDetector(d); err != nil {
			return nil, err
		}
		if c, ok := d.(Consumer); ok {
			for _, q := range c.LogQueries() {
				if old, ok := byID[q.ID]; ok && !reflect.DeepEqual(old, q) {
					return nil, fmt.Errorf("conflicting log query %q", q.ID)
				}
				byID[q.ID] = q
			}
		}
	}
	qs := make([]Query, 0, len(byID))
	for _, q := range byID {
		q.Patterns = append([]Pattern(nil), q.Patterns...)
		qs = append(qs, q)
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].ID < qs[j].ID })
	return qs, ValidateQueries(qs)
}

// Match holds a lower-bound count and bounded matching excerpts.
type Match struct {
	Count    int
	Excerpts []string
}

// Cycle is one container's observation interval. Error makes it unobservable.
// Previous is separate from current counts and must not establish a healthy rate.
type Cycle struct {
	Start, End time.Time
	Running    bool
	Lines      int
	Matches    map[string]Match
	Truncated  bool
	Previous   *Cycle
	Error      string
}

// Termination is a bounded tail captured once for a completed container.
// RestartCount identifies the exited incarnation, not its successor.
type Termination struct {
	RestartCount int32
	FinishedAt   time.Time
	ExitCode     int32
	Tail         Cycle
}

// Window holds current-incarnation cycles and at most WindowCycles completed
// container tails retained across restarts. Removed pods discard their history.
type Window struct {
	Namespace, Pod, UID, Container string
	RestartCount                   int32
	Cycles                         []Cycle
	Terminations                   []Termination
}

// Result is a query's frozen windows; Error is neither health nor a finding.
type Result struct {
	Query      Query
	ObservedAt time.Time
	Windows    []Window
	Error      string
}

// Source supplies frozen query results.
type Source interface{ LogResult(string) (Result, bool) }

// ResultFrom reads a frozen result without I/O.
func ResultFrom(ctx sdk.DetectionContext, id string) (Result, bool) {
	if s, ok := ctx.(Source); ok {
		return s.LogResult(id)
	}
	return Result{}, false
}

// Require returns a result or an actionable detector error, including per-cycle
// source errors. Detectors must use this before interpreting a window.
func Require(ctx sdk.DetectionContext, id string) (Result, error) {
	r, ok := ResultFrom(ctx, id)
	if !ok {
		return r, fmt.Errorf("log query %q unavailable", id)
	}
	if r.Error != "" {
		return r, fmt.Errorf("log query %q: %s", id, r.Error)
	}
	for _, w := range r.Windows {
		for _, t := range w.Terminations {
			if t.Tail.Error != "" {
				return r, fmt.Errorf("log query %q terminated container %s/%s: %s", id, w.Pod, w.Container, t.Tail.Error)
			}
		}
		for _, c := range w.Cycles {
			if c.Error != "" {
				return r, fmt.Errorf("log query %q container %s/%s: %s", id, w.Pod, w.Container, c.Error)
			}
			if c.Previous != nil && c.Previous.Error != "" {
				return r, fmt.Errorf("log query %q previous container: %s", id, c.Previous.Error)
			}
		}
	}
	return r, nil
}

// Evidence describes the counts, interval and excerpts behind a helper decision.
type Evidence struct {
	Count, Lines, Cycles int
	Start, End           time.Time
	Truncated            bool
	Excerpts             []string
}

// Rate returns matched lines per second over the observed window; empty pattern
// selects all lines. Truncated evidence means the rate is a lower bound.
func Rate(w Window, pattern string) (float64, Evidence) {
	e := evidence(w, pattern)
	seconds := e.End.Sub(e.Start).Seconds()
	if seconds <= 0 {
		return 0, e
	}
	return float64(e.Count) / seconds, e
}
func evidence(w Window, p string) Evidence {
	e := Evidence{Cycles: len(w.Cycles)}
	for i, c := range w.Cycles {
		if i == 0 {
			e.Start = c.Start
		}
		e.End = c.End
		e.Lines += c.Lines
		e.Truncated = e.Truncated || c.Truncated
		if p == "" {
			e.Count += c.Lines
		} else {
			m := c.Matches[p]
			e.Count += m.Count
			for _, s := range m.Excerpts {
				if len(e.Excerpts) < MaxExcerpts {
					e.Excerpts = append(e.Excerpts, s[:min(len(s), MaxExcerptBytes)])
				}
			}
		}
	}
	return e
}

// Appeared reports a zero-to-positive transition in the last two cycles.
// A truncated or failed zero observation cannot establish absence.
func Appeared(w Window, p string) (bool, Evidence) {
	e := evidence(w, p)
	n := len(w.Cycles)
	if n < 2 {
		return false, e
	}
	a, b := w.Cycles[n-2], w.Cycles[n-1]
	return a.Error == "" && b.Error == "" && !a.Truncated && a.Matches[p].Count == 0 && b.Matches[p].Count > 0, e
}

// Silent reports N complete empty Running cycles after earlier log activity.
// Restarts begin new windows, so an old incarnation never establishes activity.
func Silent(w Window, n int) (bool, Evidence) {
	e := evidence(w, "")
	if n <= 0 || len(w.Cycles) <= n {
		return false, e
	}
	cut := len(w.Cycles) - n
	active := false
	for _, c := range w.Cycles[:cut] {
		active = active || (c.Error == "" && c.Lines > 0)
	}
	for _, c := range w.Cycles[cut:] {
		if !c.Running || c.Lines != 0 || c.Truncated || c.Error != "" {
			return false, e
		}
	}
	return active, e
}
