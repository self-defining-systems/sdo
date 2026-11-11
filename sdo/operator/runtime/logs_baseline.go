package runtime

import (
	"reflect"
	"slices"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
)

// LogsBaseline holds, per log query, samples of the query's windows taken
// during settled healthy stretches (logs.Result.History), so a log detector
// judges a count against how the healthy application logs, not against zero.
//
// A stretch settles like PodReadBaseline's: once health has lasted Settle,
// the windows of the stretch become one sample, cut to cycles that started
// after the stretch began and ended a lag before its last evaluation (a fault
// still below its firing threshold is not absorbed); an unhealthy evaluation
// discards the stretch. Samples keep counts only: excerpts and previous or
// terminated tails are dropped. At most logs.MaxHistorySamples are kept per
// query, spread over time; a changed query declaration starts afresh.
//
// The holder is in-memory controller state, never written to operational
// memory: a restarted controller starts empty and its log detectors judge
// nothing until health has settled again.
type LogsBaseline struct {
	mu          sync.Mutex
	settle      time.Duration
	candidateAt time.Time
	queries     map[string]logs.Query
	history     map[string]logs.Result
}

// NewLogsBaseline returns an empty holder whose stretches settle for settle
// (DefaultStateSettle when not positive).
func NewLogsBaseline(settle time.Duration) *LogsBaseline {
	if settle <= 0 {
		settle = DefaultStateSettle
	}
	return &LogsBaseline{settle: settle, queries: map[string]logs.Query{}, history: map[string]logs.Result{}}
}

// attach records each query's declaration and sets its History to the
// samples held for the same declaration.
func (b *LogsBaseline) attach(results map[string]logs.Result) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, result := range results {
		b.queries[id] = result.Query
		if held, ok := b.history[id]; ok && reflect.DeepEqual(held.Query, result.Query) {
			result.History = slices.Clone(held.History)
			results[id] = result
		}
	}
}

// observe records one evaluation's health: a healthy one starts a stretch or,
// once the stretch has lasted the settle period, samples it and starts the
// next; an unhealthy one discards the stretch.
func (b *LogsBaseline) observe(now time.Time, healthy bool, snapshot sdk.DetectionContext) {
	if b == nil {
		return
	}
	source, ok := snapshot.(logs.Source)
	if !ok {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case !healthy:
		b.candidateAt = time.Time{}
		return
	case b.candidateAt.IsZero():
		b.candidateAt = now
		return
	case now.Sub(b.candidateAt) < b.settle:
		return
	}
	to := now.Add(-min(30*time.Second, b.settle/2))
	for id, query := range b.queries {
		result, ok := source.LogResult(id)
		if !ok || result.Error != "" || !reflect.DeepEqual(result.Query, query) {
			continue
		}
		sample, ok := cutSample(result, b.candidateAt, to)
		if !ok {
			continue
		}
		held := b.history[id]
		if !reflect.DeepEqual(held.Query, query) {
			held = logs.Result{Query: query}
		}
		held.History = spreadSamples(append(held.History, sample), logs.MaxHistorySamples)
		b.history[id] = held
	}
	b.candidateAt = now
}

// cutSample keeps each window's complete cycles inside [from, to], counts
// only. False when no window has such a cycle.
func cutSample(result logs.Result, from, to time.Time) (logs.Sample, bool) {
	sample := logs.Sample{End: to}
	for _, w := range result.Windows {
		var cycles []logs.Cycle
		for _, c := range w.Cycles {
			if c.Error != "" || c.Start.Before(from) || c.End.After(to) {
				continue
			}
			matches := make(map[string]logs.Match, len(c.Matches))
			for id, m := range c.Matches {
				matches[id] = logs.Match{Count: m.Count}
			}
			c.Matches, c.Previous = matches, nil
			cycles = append(cycles, c)
		}
		if len(cycles) == 0 {
			continue
		}
		w.Cycles, w.Terminations = cycles, nil
		sample.Windows = append(sample.Windows, w)
	}
	return sample, len(sample.Windows) > 0
}

// spreadSamples keeps at most limit samples, oldest first. Over the bound it
// drops, one at a time, the sample closest in time to the one before it
// (never the first or the newest), so the kept samples span the whole healthy
// history.
func spreadSamples(samples []logs.Sample, limit int) []logs.Sample {
	for len(samples) > limit {
		drop := 1
		for i := 2; i < len(samples)-1; i++ {
			if samples[i].End.Sub(samples[i-1].End) < samples[drop].End.Sub(samples[drop-1].End) {
				drop = i
			}
		}
		samples = slices.Delete(samples, drop, drop+1)
	}
	return samples
}
