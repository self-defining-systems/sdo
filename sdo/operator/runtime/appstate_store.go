package runtime

import (
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"sdo.dev/operator/sdk/appstate/carried"
)

// AppStateStore is the controller's in-memory state for app-state readers: per
// reader a ring of the last carried.RingSize finished snapshots, the
// last-healthy snapshot and the snapshots of settled healthy stretches (for
// noisy-key derivation). It is never persisted: a restart starts empty, which
// carried.State reports as a gap (no baseline), never as "no change".
// Last-healthy follows the shared settle rule (healthyStretch, as
// PodReadBaseline): the snapshot at the start of a healthy stretch is promoted
// once the stretch has lasted the settle window. One store serves every memory
// host swap.
type AppStateStore struct {
	mu      sync.Mutex
	stretch healthyStretch
	readers map[string]*appStateReaderState
	// candidate is each reader's newest usable snapshot at the start of the
	// current healthy stretch; samples are the usable snapshots since.
	candidate map[string]carried.Snapshot
	samples   map[string][]carried.Snapshot
	active    bool
}

type appStateReaderState struct {
	declared []string
	ring     []carried.Snapshot
	latest   *carried.Snapshot // newest usable snapshot
	healthy  *carried.Snapshot
	history  []carried.Snapshot
}

// NewAppStateStore returns an empty store whose promotions settle for settle
// (DefaultStateSettle when not positive).
func NewAppStateStore(settle time.Duration) *AppStateStore {
	return &AppStateStore{stretch: newHealthyStretch(settle), readers: map[string]*appStateReaderState{}}
}

// appStateHistoryLimit bounds the healthy snapshots kept per reader.
const appStateHistoryLimit = carried.RingSize

// Retain keeps exactly the given readers (id to declared noisy keys), at most
// carried.MaxReaders in sorted-id order; state of any other reader is dropped.
func (s *AppStateStore) Retain(readers map[string][]string) {
	if s == nil {
		return
	}
	ids := make([]string, 0, len(readers))
	for id := range readers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > carried.MaxReaders {
		ids = ids[:carried.MaxReaders]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := map[string]bool{}
	for _, id := range ids {
		keep[id] = true
		state := s.readers[id]
		if state == nil {
			state = &appStateReaderState{}
			s.readers[id] = state
		}
		state.declared = slices.Clone(readers[id])
	}
	for id := range s.readers {
		if !keep[id] {
			delete(s.readers, id)
			delete(s.candidate, id)
			delete(s.samples, id)
		}
	}
}

// Record adds one finished read of a retained reader.
func (s *AppStateStore) Record(id string, snap carried.Snapshot) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.readers[id]
	if state == nil {
		return
	}
	state.ring = append(state.ring, snap)
	if len(state.ring) > carried.RingSize {
		state.ring = slices.Clone(state.ring[len(state.ring)-carried.RingSize:])
	}
	if snap.Error != "" {
		return
	}
	state.latest = &snap
	if s.active {
		s.samples[id] = append(s.samples[id], snap)
	}
}

// Observe feeds one controller evaluation's health to the settle rule.
func (s *AppStateStore) Observe(now time.Time, healthy bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	latest := func() map[string]carried.Snapshot {
		out := map[string]carried.Snapshot{}
		for id, state := range s.readers {
			if state.latest != nil {
				out[id] = *state.latest
			}
		}
		return out
	}
	switch event, _ := s.stretch.observe(now, healthy); event {
	case stretchBroken:
		s.candidate, s.samples, s.active = nil, nil, false
	case stretchStarted:
		s.candidate, s.samples, s.active = latest(), map[string][]carried.Snapshot{}, true
	case stretchSettled:
		for id, snap := range s.candidate {
			state := s.readers[id]
			if state == nil {
				continue
			}
			promoted := snap
			state.healthy = &promoted
			state.history = spreadByTime(append(state.history, s.samples[id]...), func(x carried.Snapshot) time.Time { return x.At }, appStateHistoryLimit)
		}
		s.candidate, s.samples, s.active = latest(), map[string][]carried.Snapshot{}, true
	}
}

func (s *AppStateStore) result(id string, state *appStateReaderState) carried.Result {
	r := carried.Result{Reader: id, Declared: slices.Clone(state.declared), Recent: slices.Clone(state.ring), HealthyHistory: slices.Clone(state.history)}
	if state.healthy != nil {
		healthy := *state.healthy
		r.Healthy = &healthy
	}
	return r
}

// Result is one reader's carried result.
func (s *AppStateStore) Result(id string) (carried.Result, bool) {
	if s == nil {
		return carried.Result{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.readers[id]
	if state == nil || len(state.ring) == 0 {
		return carried.Result{}, false
	}
	return s.result(id, state), true
}

// Results is every retained reader's carried result that has a snapshot.
func (s *AppStateStore) Results() map[string]carried.Result {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]carried.Result{}
	for id, state := range s.readers {
		if len(state.ring) > 0 {
			out[id] = s.result(id, state)
		}
	}
	return out
}

// AppStateChange is one reader's key move, for the page-time change list.
type AppStateChange struct {
	Reader string
	Change carried.Change
}

// Changes lists every reader's key moves in (now-d, now], oldest first.
func (s *AppStateStore) Changes(now time.Time, d time.Duration) []AppStateChange {
	var out []AppStateChange
	results := s.Results()
	for _, id := range slices.Sorted(maps.Keys(results)) {
		changes, _ := carried.State{Result: results[id]}.ChangedWithin("*", now, d)
		for _, c := range changes {
			out = append(out, AppStateChange{Reader: id, Change: c})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Change.At.Before(out[j].Change.At) })
	return out
}

// observeAppState feeds one evaluation's health to the app-state settle rule.
func (c *Controller) observeAppState(now time.Time, healthy bool) {
	c.AppState.Observe(now.UTC(), healthy)
}
