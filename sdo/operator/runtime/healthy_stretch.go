package runtime

import (
	"slices"
	"sync"
	"time"
)

// healthyStretch is the one settle rule every healthy-history holder shares
// (pod reads, client-link vantage, platform): a stretch starts at the first healthy
// evaluation, an unhealthy one discards it, and once it has lasted settle it
// settles and the next stretch starts at that evaluation. Neither the first
// clear evaluation nor a fault still below its firing threshold is absorbed.
// It holds timing only; callers keep their own values. Not safe for
// concurrent use: callers hold their own lock.
type healthyStretch struct {
	settle time.Duration
	start  time.Time
}

func newHealthyStretch(settle time.Duration) healthyStretch {
	if settle <= 0 {
		settle = DefaultStateSettle
	}
	return healthyStretch{settle: settle}
}

// stretchEvent is what one evaluation did to the stretch.
type stretchEvent int

const (
	// stretchBroken: the evaluation was unhealthy; any stretch is discarded.
	stretchBroken stretchEvent = iota
	// stretchStarted: the evaluation started a new stretch.
	stretchStarted
	// stretchGrowing: the stretch continues, not yet settled.
	stretchGrowing
	// stretchSettled: the stretch that began at the returned start settled;
	// a new one starts at this evaluation.
	stretchSettled
)

// observe records one evaluation and returns what it did, with the start of
// the stretch it settled (stretchSettled) or began (stretchStarted).
func (s *healthyStretch) observe(now time.Time, healthy bool) (stretchEvent, time.Time) {
	switch {
	case !healthy:
		s.start = time.Time{}
		return stretchBroken, time.Time{}
	case s.start.IsZero():
		s.start = now
		return stretchStarted, now
	case now.Sub(s.start) >= s.settle:
		from := s.start
		s.start = now
		return stretchSettled, from
	}
	return stretchGrowing, s.start
}

// spreadByTime keeps at most limit items, oldest first. Over the bound it
// drops, one at a time, the item closest in time to the one before it (never
// the first or the newest), so the kept items span the whole healthy history
// as evenly as they can. Every bounded healthy history uses it.
func spreadByTime[T any](items []T, at func(T) time.Time, limit int) []T {
	items = slices.Clone(items)
	slices.SortStableFunc(items, func(a, b T) int { return at(a).Compare(at(b)) })
	for len(items) > max(limit, 2) {
		drop := 1
		for i := 2; i < len(items)-1; i++ {
			if at(items[i]).Sub(at(items[i-1])) < at(items[drop]).Sub(at(items[drop-1])) {
				drop = i
			}
		}
		items = slices.Delete(items, drop, drop+1)
	}
	return items
}

// settledBaseline promotes the value observed at the start of a healthy
// stretch once the stretch has lasted settle (healthyStretch): neither
// the first clear evaluation nor a fault still below its firing threshold is
// absorbed. It is in-memory controller state only.
type settledBaseline[T any] struct {
	mu        sync.RWMutex
	stretch   healthyStretch
	promoted  *T
	candidate *T
}

func newSettledBaseline[T any](settle time.Duration) *settledBaseline[T] {
	return &settledBaseline[T]{stretch: newHealthyStretch(settle)}
}

// Observe records one evaluation's value.
func (b *settledBaseline[T]) Observe(now time.Time, healthy bool, value T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch event, _ := b.stretch.observe(now, healthy); event {
	case stretchBroken:
		b.candidate = nil
	case stretchStarted:
		b.candidate = &value
	case stretchSettled:
		b.promoted = b.candidate
		b.candidate = &value
	}
}

// Current returns the promoted value.
func (b *settledBaseline[T]) Current() (T, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.promoted == nil {
		var zero T
		return zero, false
	}
	return *b.promoted, true
}
