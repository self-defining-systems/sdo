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
// against the healthy store without a designer writing its baseline. It is
// controller state: never written to operational memory, and the healthy
// replay capture records it with the readings of the promoted moment. One
// holder serves every memory-host swap; a query whose declaration changed gets
// no baseline until the next promotion.
type PodReadBaseline struct {
	mu      sync.RWMutex
	results map[string]podread.Result
}

// NewPodReadBaseline returns an empty holder.
func NewPodReadBaseline() *PodReadBaseline {
	return &PodReadBaseline{results: map[string]podread.Result{}}
}

// Promote replaces each query's reference with its complete readings. A query
// with no complete reading keeps its earlier reference.
func (b *PodReadBaseline) Promote(results map[string]podread.Result) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, result := range results {
		if result.Error != "" {
			continue
		}
		complete := slices.DeleteFunc(slices.Clone(result.Readings), func(r podread.Reading) bool {
			return r.Error != "" || r.Truncated
		})
		if len(complete) == 0 {
			continue
		}
		b.results[id] = podread.Result{Query: result.Query, Readings: complete}
	}
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

// promotePodReadBaseline follows the configuration baseline's boundaries, as
// the healthy replay capture does: the pod readings of a clear candidate moment
// are recorded at the start of its settle and promoted only with that
// candidate, so a fault that is still below its firing threshold is never
// absorbed. Without boundaries every clear evaluation is promoted.
func (c *Controller) promotePodReadBaseline(now time.Time, snapshot sdk.DetectionContext, healthy bool) {
	if c.PodReadBaseline == nil || !healthy {
		return
	}
	set, ok := snapshot.(podread.ResultSet)
	if !ok {
		return
	}
	now = now.UTC()
	boundaries, bounded := c.Baseline.(healthyBaselineBoundaries)
	if !bounded {
		c.PodReadBaseline.Promote(set.PodReadResults())
		return
	}
	baselineAt, candidateAt := boundaries.BaselineAt(), boundaries.BaselineCandidateAt()
	if baselineAt.IsZero() {
		return
	}
	if c.podReadCandidates == nil {
		c.podReadCandidates = map[time.Time]map[string]podread.Result{}
	}
	if baselineAt.Equal(now) || candidateAt.Equal(now) {
		c.podReadCandidates[now] = set.PodReadResults()
	}
	if results, ok := c.podReadCandidates[baselineAt]; ok && !c.podReadPromotedAt.Equal(baselineAt) {
		c.PodReadBaseline.Promote(results)
		c.podReadPromotedAt = baselineAt
	}
	maps.DeleteFunc(c.podReadCandidates, func(at time.Time, _ map[string]podread.Result) bool {
		return !at.Equal(baselineAt) && !at.Equal(candidateAt)
	})
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
