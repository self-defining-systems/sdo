package runtime

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/podread"
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
	// samples are each reader's usable snapshots since the current healthy
	// stretch began; the first becomes last-healthy when the stretch settles.
	samples map[string][]carried.Snapshot
	active  bool
}

type appStateReaderState struct {
	declared []string
	// targets are the Pods the reader's own pod reads select: the only link
	// from a reader to a component of the application map.
	targets []podread.Target
	ring    []carried.Snapshot
	healthy *carried.Snapshot
	history []carried.Snapshot
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
			delete(s.samples, id)
		}
	}
}

// SetTargets records the pod-read targets of a retained reader.
func (s *AppStateStore) SetTargets(id string, targets []podread.Target) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.readers[id]; state != nil {
		state.targets = slices.Clone(targets)
	}
}

// AppStateReaderStatus is what one retained reader has observed: the targets
// its pod reads select, whether it has read at all, why its newest read gave
// no answer and whether a last-healthy baseline exists to diff against.
type AppStateReaderStatus struct {
	Reader    string
	Targets   []podread.Target
	Read      bool
	Err       string
	Baselined bool
}

// AppStateReaders lists every retained reader's status, by reader ID.
func (s *AppStateStore) AppStateReaders() []AppStateReaderStatus {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []AppStateReaderStatus
	for _, id := range slices.Sorted(maps.Keys(s.readers)) {
		state := s.readers[id]
		status := AppStateReaderStatus{Reader: id, Targets: slices.Clone(state.targets), Read: len(state.ring) > 0, Baselined: state.healthy != nil}
		if n := len(state.ring); n > 0 {
			status.Err = state.ring[n-1].Error
		}
		out = append(out, status)
	}
	return out
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
	switch event, _ := s.stretch.observe(now, healthy); event {
	case stretchBroken:
		s.samples, s.active = nil, false
	case stretchStarted:
		s.samples, s.active = map[string][]carried.Snapshot{}, true
	case stretchSettled:
		// The reference is the first snapshot read inside the stretch: a
		// snapshot from before it may have been read while unhealthy.
		for id, state := range s.readers {
			samples := s.samples[id]
			if len(samples) == 0 {
				continue
			}
			promoted := samples[0]
			state.healthy = &promoted
			state.history = spreadByTime(append(state.history, samples...), func(x carried.Snapshot) time.Time { return x.At }, appStateHistoryLimit)
		}
		s.samples, s.active = map[string][]carried.Snapshot{}, true
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

// AppStateAbsent is the before or after of an app-state row whose key does not
// exist on that side, so a missing key is never read as an empty value.
const AppStateAbsent = "(absent)"

// AppStateReadKey is the key of the row that reports a reader whose latest
// read failed; its After is the error.
const AppStateReadKey = "(read)"

// AppStateChanges adapts the store to the app-state seam of `sdo changes`
// (AppStateChangeSource): each reader's key moves in (since, now]; a key that
// differs between the reader's last-healthy snapshot and its newest one (when
// the ring holds no move of it; a key on one side only shows (absent)); and one row for each reader whose
// latest read failed, since such a reader derives no change at all.
func (s *AppStateStore) AppStateChanges(since, now time.Time) []IncidentChange {
	var out []IncidentChange
	moved := map[string]map[string]bool{}
	for _, c := range s.Changes(now, now.Sub(since)) {
		before, after := c.Change.Before, c.Change.After
		switch c.Change.Kind {
		case carried.Added:
			before = AppStateAbsent
		case carried.Removed:
			after = AppStateAbsent
		}
		if moved[c.Reader] == nil {
			moved[c.Reader] = map[string]bool{}
		}
		moved[c.Reader][c.Change.Key] = true
		out = append(out, IncidentChange{At: c.Change.At, Source: "app-state", ReaderOrObject: c.Reader, Key: c.Change.Key, Before: before, After: after, Noisy: c.Change.Noisy})
	}
	results := s.Results()
	for _, id := range slices.Sorted(maps.Keys(results)) {
		result := results[id]
		latest, ok := result.Latest()
		if !ok || latest.At.After(now) || !latest.At.After(since) {
			continue
		}
		if latest.Error != "" {
			out = append(out, IncidentChange{At: latest.At, Source: "app-state", ReaderOrObject: id, Key: AppStateReadKey, After: latest.Error})
			continue
		}
		healthy, _ := carried.State{Result: result}.DiffersFromHealthy("*")
		for _, c := range healthy {
			if moved[id][c.Key] {
				continue
			}
			before, after := c.Before, c.After
			switch c.Kind {
			case carried.Added:
				before = AppStateAbsent
			case carried.Removed:
				after = AppStateAbsent
			}
			// A difference from the last-healthy snapshot has no change time: the
			// row carries when the reader read it, never an onset-relative time.
			out = append(out, IncidentChange{ReadAt: c.At, Source: "app-state", ReaderOrObject: id, Key: c.Key, Before: before, After: after, Noisy: c.Noisy, FirstSeen: c.Kind == carried.Added})
		}
	}
	return collapseFirstSeen(out)
}

// firstSeenKeep is how many first-seen keys of one reader stay as rows.
const firstSeenKeep = 2

// collapseFirstSeen replaces a reader's many `(absent) -> value` rows from the
// last-healthy diff (keys the baseline read never held, as when a reader's
// first read was partial) by one summary row, so they neither crowd out the
// reader's real changes nor read as change at a time.
func collapseFirstSeen(rows []IncidentChange) []IncidentChange {
	byReader := map[string][]int{}
	for index, row := range rows {
		if row.FirstSeen {
			byReader[row.ReaderOrObject] = append(byReader[row.ReaderOrObject], index)
		}
	}
	drop := map[int]bool{}
	var summaries []IncidentChange
	for _, reader := range slices.Sorted(maps.Keys(byReader)) {
		indexes := byReader[reader]
		if len(indexes) <= firstSeenKeep {
			continue
		}
		var keys []string
		for _, index := range indexes {
			drop[index] = true
			keys = append(keys, rows[index].Key)
		}
		first := rows[indexes[0]]
		after := fmt.Sprintf("%d keys, e.g. %s", len(keys), strings.Join(keys[:min(3, len(keys))], ", "))
		summaries = append(summaries, IncidentChange{ReadAt: first.ReadAt, Source: "app-state", ReaderOrObject: reader, Key: "(keys absent at the last healthy read)", Before: AppStateAbsent, After: after, FirstSeen: true})
	}
	if len(drop) == 0 {
		return rows
	}
	kept := make([]IncidentChange, 0, len(rows))
	for index, row := range rows {
		if !drop[index] {
			kept = append(kept, row)
		}
	}
	return append(kept, summaries...)
}
