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
// (storestate.UsableAsBaseline).
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
	// settle is how long health must last before the readings at the start
	// of the stretch are promoted.
	settle      time.Duration
	candidateAt time.Time
	candidate   map[string]podread.Result
	// Payloads, when set, captures the restore text of each promoted
	// store-state reading (StorePayloads).
	Payloads *StorePayloads
}

// NewPodReadBaseline returns an empty holder whose promotions settle for
// settle (DefaultStateSettle when not positive).
func NewPodReadBaseline(settle time.Duration) *PodReadBaseline {
	if settle <= 0 {
		settle = DefaultStateSettle
	}
	return &PodReadBaseline{results: map[string]podread.Result{}, settle: settle}
}

// Observe records one evaluation. A healthy one starts a candidate with its
// readings, or promotes the candidate once it has stayed healthy for the
// settle period and starts the next; an unhealthy one discards the candidate.
func (b *PodReadBaseline) Observe(now time.Time, healthy bool, results map[string]podread.Result) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if !healthy {
		b.candidate, b.candidateAt = nil, time.Time{}
		b.mu.Unlock()
		return
	}
	var settled map[string]podread.Result
	switch {
	case b.candidate == nil:
		b.candidate, b.candidateAt = maps.Clone(results), now
	case now.Sub(b.candidateAt) >= b.settle:
		settled = b.candidate
		b.candidate, b.candidateAt = maps.Clone(results), now
	}
	b.mu.Unlock()
	if settled != nil {
		b.Promote(settled)
	}
}

// Promote replaces each query's reference with its complete readings. A query
// with no complete reading, or a store-state read that cannot be a healthy
// reference, keeps its earlier reference.
func (b *PodReadBaseline) Promote(results map[string]podread.Result) {
	if b == nil {
		return
	}
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
	}
	b.mu.Unlock()
	b.Payloads.promoted(promoted)
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
