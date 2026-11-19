package runtime

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// phaseTrace attributes the wall time of one observation-lock hold or one
// responder tool call to named phases. A phase is charged only the time its own
// code took (a phase that begins inside another is subtracted from it), so the
// phases of one trace sum to at most its length and what no phase accounts for
// is reported as unattributed. It reads time only through the injected clock.
// A nil trace measures nothing. The cost is two clock reads per phase.
type phaseTrace struct {
	mu    sync.Mutex
	now   func() time.Time
	order []string
	total map[string]time.Duration
	open  []*phaseFrame
}

type phaseFrame struct {
	start  time.Time
	nested time.Duration
}

func newPhaseTrace(now func() time.Time) *phaseTrace {
	return &phaseTrace{now: now, total: map[string]time.Duration{}}
}

// begin starts the named phase and returns the function that ends it. A phase
// that begins more than once accumulates.
func (t *phaseTrace) begin(name string) func() {
	if t == nil {
		return func() {}
	}
	frame := &phaseFrame{start: t.now()}
	t.mu.Lock()
	if _, seen := t.total[name]; !seen {
		t.total[name] = 0
		t.order = append(t.order, name)
	}
	t.open = append(t.open, frame)
	t.mu.Unlock()
	return func() {
		elapsed := t.now().Sub(frame.start)
		t.mu.Lock()
		defer t.mu.Unlock()
		for i, candidate := range t.open {
			if candidate != frame {
				continue
			}
			t.open = append(t.open[:i], t.open[i+1:]...)
			if i > 0 {
				t.open[i-1].nested += elapsed
			}
			break
		}
		t.total[name] += elapsed - frame.nested
	}
}

// durations returns each phase's accumulated time.
func (t *phaseTrace) durations() map[string]time.Duration {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]time.Duration, len(t.total))
	for name, d := range t.total {
		out[name] = d
	}
	return out
}

// names lists the phases in the order they began.
func (t *phaseTrace) names() []string {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.order...)
}

// logAttrs are the slog attributes that attribute total: a phase_ms group with
// every phase in milliseconds, and unattributed_ms, the time in no phase. A
// A holder that never marks a phase logs only unattributed_ms, its whole hold.
func (t *phaseTrace) logAttrs(total time.Duration) []any {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.order) == 0 {
		return []any{"unattributed_ms", total.Milliseconds()}
	}
	group := make([]any, 0, len(t.order))
	var accounted time.Duration
	for _, name := range t.order {
		group = append(group, slog.Int64(name, t.total[name].Milliseconds()))
		accounted += t.total[name]
	}
	return []any{slog.Group("phase_ms", group...), "unattributed_ms", (total - accounted).Milliseconds()}
}

type phaseTraceKey struct{}

// withPhaseTrace carries the trace to the snapshot providers and observers
// below, which charge their own time to it.
func withPhaseTrace(ctx context.Context, t *phaseTrace) context.Context {
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, phaseTraceKey{}, t)
}

// measurePhase begins the named phase on the trace the context carries and
// returns its end. Without a trace it does nothing.
func measurePhase(ctx context.Context, name string) func() {
	t, _ := ctx.Value(phaseTraceKey{}).(*phaseTrace)
	return t.begin(name)
}
