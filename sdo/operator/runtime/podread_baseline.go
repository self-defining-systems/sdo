package runtime

import (
	"maps"
	"reflect"
	"slices"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// PodReadBaseline holds the complete pod readings of the controller's last
// promoted healthy baseline, per query. The snapshot carries them with each
// current result (podread.Result.Baseline), so a store-state detector diffs
// against the healthy store without a designer writing its baseline. One
// holder serves every memory-host swap; a query whose declaration changed gets
// no baseline until the next promotion.
//
// Promotion settles in every mode: the readings at the start of a healthy
// stretch become the reference only once the stretch has lasted Settle
// (DefaultStateSettle, the configuration baseline's settle), so neither the
// first clear evaluation nor a fault still below its firing threshold is
// absorbed. A store-state read with a refusal is never promoted
// (storestate.UsableAsBaseline). The complete healthy readings of every
// settled stretch also form the query's history (podread.Result.History),
// bounded per Pod and spread over time, so a reader judges how the healthy
// application varies, not only the moment the reference was taken; readings
// of a stretch that ended unhealthy never enter it.
//
// The holder is in-memory controller state, never written to operational
// memory or persisted elsewhere: a restarted controller starts empty, its
// Go-installed store readers report their stores as coverage gaps instead of
// judging them, and the reference is captured again once health has settled
// for Settle after the restart. Persisting it would add a store whose
// readings could outlive the deployment they describe; re-capturing after a
// settle needs no new state and has the same guard against absorbing a fault
// as the first capture.
type PodReadBaseline struct {
	mu      sync.RWMutex
	results map[string]podread.Result
	// stretch decides when the readings at the start of a healthy stretch
	// are promoted.
	stretch   healthyStretch
	candidate map[string]podread.Result
	// samples are the complete readings of the candidate stretch, per query.
	samples map[string][]podread.Reading
	// history holds the readings of every settled stretch, per query
	// declaration (podread.Result.History).
	history map[string]podread.Result
	// Payloads, when set, captures the restore text of each promoted
	// store-state reading (StorePayloads).
	Payloads *StorePayloads
}

// NewPodReadBaseline returns an empty holder whose promotions settle for
// settle (DefaultStateSettle when not positive).
func NewPodReadBaseline(settle time.Duration) *PodReadBaseline {
	return &PodReadBaseline{results: map[string]podread.Result{}, history: map[string]podread.Result{}, stretch: newHealthyStretch(settle)}
}

// Observe records one evaluation. A healthy one starts a candidate with its
// readings, or promotes the candidate once it has stayed healthy for the
// settle period and starts the next; an unhealthy one discards the candidate.
func (b *PodReadBaseline) Observe(now time.Time, healthy bool, results map[string]podread.Result) {
	if b == nil {
		return
	}
	b.mu.Lock()
	var settled map[string]podread.Result
	var stretch map[string][]podread.Reading
	switch event, _ := b.stretch.observe(now, healthy); event {
	case stretchBroken:
		b.candidate, b.samples = nil, nil
		b.mu.Unlock()
		return
	case stretchStarted:
		b.candidate, b.samples = maps.Clone(results), nil
	case stretchSettled:
		settled, stretch = b.candidate, b.samples
		b.candidate, b.samples = maps.Clone(results), nil
	}
	b.samples = withSamples(b.samples, results)
	b.mu.Unlock()
	if settled != nil {
		b.promote(settled, stretch)
	}
}

// Promote replaces each query's reference with its complete readings and adds
// them to the query's history. A query with no complete reading, or a
// store-state read that cannot be a healthy reference, keeps its earlier
// reference.
func (b *PodReadBaseline) Promote(results map[string]podread.Result) {
	if b == nil {
		return
	}
	b.promote(results, withSamples(nil, results))
}

// promote is Promote with the readings of the settled stretch, which extend
// each promoted query's history.
func (b *PodReadBaseline) promote(results map[string]podread.Result, stretch map[string][]podread.Reading) {
	b.mu.Lock()
	promoted := map[string]podread.Result{}
	for id, result := range results {
		if result.Error != "" || storestate.UsableAsBaseline(id, result) != nil {
			continue
		}
		complete := slices.DeleteFunc(slices.Clone(result.Readings), func(r podread.Reading) bool {
			return r.Error != "" || r.Truncated
		})
		if len(complete) == 0 {
			continue
		}
		b.results[id] = podread.Result{Query: result.Query, Readings: complete}
		promoted[id] = b.results[id]
		history := b.history[id]
		if !reflect.DeepEqual(history.Query, result.Query) {
			history = podread.Result{Query: result.Query}
		}
		for _, r := range stretch[id] {
			if storestate.UsableAsBaseline(id, podread.Result{Query: result.Query, Readings: []podread.Reading{r}}) == nil {
				history.Readings = append(history.Readings, r)
			}
		}
		history.Readings = spreadHistory(history.Readings, podread.HistoryPerPod)
		b.history[id] = history
	}
	b.mu.Unlock()
	b.Payloads.promoted(promoted)
}

// withSamples appends each result's complete readings to samples, skipping a
// reading already held (the same Pod read at the same moment, carried by
// several evaluations).
func withSamples(samples map[string][]podread.Reading, results map[string]podread.Result) map[string][]podread.Reading {
	for id, result := range results {
		if result.Error != "" {
			continue
		}
		for _, r := range result.Readings {
			if r.Error != "" || r.Truncated || slices.ContainsFunc(samples[id], func(s podread.Reading) bool {
				return s.Pod == r.Pod && s.ObservedAt.Equal(r.ObservedAt) && s.Stdout == r.Stdout
			}) {
				continue
			}
			if samples == nil {
				samples = map[string][]podread.Reading{}
			}
			samples[id] = append(samples[id], r)
		}
	}
	return samples
}

// spreadHistory keeps at most perPod readings of each Pod, spread over time
// (spreadByTime).
func spreadHistory(readings []podread.Reading, perPod int) []podread.Reading {
	byPod := map[string][]podread.Reading{}
	for _, r := range readings {
		byPod[r.Pod] = append(byPod[r.Pod], r)
	}
	var out []podread.Reading
	for _, pod := range slices.Sorted(maps.Keys(byPod)) {
		out = append(out, spreadByTime(byPod[pod], func(r podread.Reading) time.Time { return r.ObservedAt }, perPod)...)
	}
	return out
}

// attach sets each result's Baseline to the promoted readings of the same
// query declaration.
func (b *PodReadBaseline) attach(results map[string]podread.Result) {
	if b == nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, result := range results {
		promoted, ok := b.results[id]
		if !ok || !reflect.DeepEqual(promoted.Query, result.Query) {
			continue
		}
		result.Baseline = slices.Clone(promoted.Readings)
		if history, ok := b.history[id]; ok && reflect.DeepEqual(history.Query, result.Query) {
			result.History = slices.Clone(history.Readings)
		}
		results[id] = result
	}
}

// promotePodReadBaseline feeds each complete evaluation to the pod-read
// baseline, whose own settle decides promotion whether or not the
// configuration baseline runs.
func (c *Controller) promotePodReadBaseline(now time.Time, snapshot sdk.DetectionContext, healthy bool) {
	if c.PodReadBaseline == nil {
		return
	}
	var results map[string]podread.Result
	if set, ok := snapshot.(podread.ResultSet); ok {
		results = set.PodReadResults()
	}
	c.PodReadBaseline.Observe(now.UTC(), healthy, results)
}

// StoreStateChangeKind is the StateChange kind of one store Pod's items that
// differ from the promoted healthy baseline. Its name is "component/pod" and
// each field is one item ("<kind> [scope/]name").
const StoreStateChangeKind = "StoreState"

// withStoreChanges adds the store-state diff the view carries to an incident's
// state changes. It returns state itself when no store changed, and never
// mutates it.
func withStoreChanges(state *StateChanges, view sdk.DetectionContext, now time.Time) *StateChanges {
	var added []StateChange
	var baselineAt time.Time
	for _, store := range storestate.Changes(view) {
		if !store.Baselined || len(store.Changes) == 0 {
			continue
		}
		if baselineAt.IsZero() || store.BaselineAt.Before(baselineAt) {
			baselineAt = store.BaselineAt
		}
		change := StateChange{Kind: StoreStateChangeKind, Name: store.Component + "/" + store.Pod.Name, Change: StateChangeModified}
		for _, item := range store.Changes {
			if len(change.Fields) == maxStateFields {
				break
			}
			field := StateFieldChange{Field: item.Key().String()}
			if item.Before != nil {
				field.Before = bounded(item.Before.Value)
			}
			if item.After != nil {
				field.After = bounded(item.After.Value)
			}
			change.Fields = append(change.Fields, field)
		}
		added = append(added, change)
	}
	if len(added) == 0 {
		return state
	}
	out := cloneStateChanges(state)
	if out == nil {
		out = &StateChanges{BaselineAt: baselineAt, ObservedAt: now}
	}
	for _, change := range added {
		if len(out.Changes) >= maxStateChanges {
			out.Omitted++
			continue
		}
		out.Changes = append(out.Changes, change)
	}
	return out
}
