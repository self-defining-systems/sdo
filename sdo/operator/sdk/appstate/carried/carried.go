// Package carried holds the application-state values a detector reads from the
// frozen observation: what a reader returned, the snapshots the controller
// keeps of it, and the pure derivations over them (changes, noisy keys and the
// source predicates). It is a leaf package so the input carrier can embed its
// Source; detectors and readers use it through package appstate.
package carried

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// Bounds of one reader's state and of the history the controller keeps.
const (
	MaxKeys       = 256
	MaxKeyBytes   = 256
	MaxValueBytes = 256
	MaxReaders    = 16
	// RingSize is how many snapshots of a reader the controller keeps.
	RingSize = 30
	// MaxIDBytes bounds a reader identifier.
	MaxIDBytes = 128
)

// Values is one reader's state: key to value. The zero value is empty and
// ready. Prototype: no secret filtering; readers must not select secret
// columns. A key or value past the bounds is refused and reported by Err,
// which fails the ReadState call.
type Values struct {
	items   map[string]string
	refused []string
}

// Set records one key.
func (v *Values) Set(key, value string) {
	switch {
	case key == "":
		v.refuse("empty key")
	case len(key) > MaxKeyBytes:
		v.refuse(fmt.Sprintf("key %q exceeds %d bytes", key[:32]+"...", MaxKeyBytes))
	case len(value) > MaxValueBytes:
		v.refuse(fmt.Sprintf("value of key %q exceeds %d bytes", key, MaxValueBytes))
	default:
		if _, exists := v.items[key]; !exists && len(v.items) >= MaxKeys {
			v.refuse(fmt.Sprintf("more than %d keys", MaxKeys))
			return
		}
		if v.items == nil {
			v.items = map[string]string{}
		}
		v.items[key] = value
	}
}

func (v *Values) refuse(reason string) {
	if len(v.refused) < 8 {
		v.refused = append(v.refused, reason)
	}
}

// Get returns a value and whether the key is present.
func (v Values) Get(key string) (string, bool) { s, ok := v.items[key]; return s, ok }

// Len is the number of keys.
func (v Values) Len() int { return len(v.items) }

// Keys lists the keys in sorted order.
func (v Values) Keys() []string {
	keys := make([]string, 0, len(v.items))
	for k := range v.items {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Err reports every refused Set, or nil.
func (v Values) Err() error {
	if len(v.refused) == 0 {
		return nil
	}
	return errors.New("app-state values refused: " + strings.Join(v.refused, "; "))
}

// MarshalJSON writes the values as a plain key-to-value object.
func (v Values) MarshalJSON() ([]byte, error) {
	if v.items == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(v.items)
}

// UnmarshalJSON reads a plain object, applying the same bounds as Set. A key
// the current rules refuse is dropped.
func (v *Values) UnmarshalJSON(data []byte) error {
	var items map[string]string
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}
	*v = Values{}
	for _, k := range sortedKeys(items) {
		v.Set(k, items[k])
	}
	v.refused = nil
	return nil
}

func sortedKeys(items map[string]string) []string {
	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Clone returns an independent copy, including any refusals.
func (v Values) Clone() Values {
	out := Values{refused: append([]string(nil), v.refused...)}
	if v.items != nil {
		out.items = make(map[string]string, len(v.items))
		for k, val := range v.items {
			out.items[k] = val
		}
	}
	return out
}

// Snapshot is one finished ReadState call. Error is the read error of a call
// that failed; its Values are then empty.
type Snapshot struct {
	At     time.Time `json:"at"`
	Values Values    `json:"values"`
	Error  string    `json:"error,omitempty"`
}

// Result is what the controller carries for one reader: the ring of recent
// snapshots (oldest first, newest last), the snapshot at the last healthy
// moment and the snapshots taken while healthy (for noise). Healthy is nil
// until the application has been healthy once since the controller started:
// there is then no baseline, which is a gap and never "no change".
type Result struct {
	Reader         string     `json:"reader"`
	Declared       []string   `json:"declared_noisy,omitempty"`
	Recent         []Snapshot `json:"recent,omitempty"`
	Healthy        *Snapshot  `json:"healthy,omitempty"`
	HealthyHistory []Snapshot `json:"healthy_history,omitempty"`
}

// Latest is the newest snapshot.
func (r Result) Latest() (Snapshot, bool) {
	if len(r.Recent) == 0 {
		return Snapshot{}, false
	}
	return r.Recent[len(r.Recent)-1], true
}

// Source supplies frozen reader results.
type Source interface {
	AppStateResult(string) (Result, bool)
}

// ResultSet lists every carried result, for snapshot capture.
type ResultSet interface {
	AppStateResults() map[string]Result
}

// Consumer declares the readers a detector's judgement depends on.
type Consumer interface{ AppStateReaders() []string }

// idPattern is the detector ID grammar: a reader is identified by its detector.
var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)

// ValidateID checks a reader identifier.
func ValidateID(id string) error {
	if len(id) > MaxIDBytes || !idPattern.MatchString(id) {
		return fmt.Errorf("invalid app-state reader id %q", id)
	}
	return nil
}

// ResultFrom reads a frozen result without I/O.
func ResultFrom(ctx sdk.DetectionContext, id string) (Result, bool) {
	if s, ok := ctx.(Source); ok {
		return s.AppStateResult(id)
	}
	return Result{}, false
}

// State is a reader's frozen result with the source predicates.
type State struct{ Result }

// Require returns a reader's state when its newest call succeeded, or a
// detector error: a missing, failed or empty reader cannot establish a clear.
func Require(ctx sdk.DetectionContext, id string) (State, error) {
	r, ok := ResultFrom(ctx, id)
	if !ok {
		return State{}, fmt.Errorf("app-state reader %q unavailable", id)
	}
	latest, ok := r.Latest()
	if !ok {
		return State{Result: r}, fmt.Errorf("app-state reader %q has no snapshot yet", id)
	}
	if latest.Error != "" {
		return State{Result: r}, fmt.Errorf("app-state reader %q: %s", id, latest.Error)
	}
	return State{Result: r}, nil
}

// ChangeKind says how a key moved.
type ChangeKind string

const (
	Changed ChangeKind = "changed"
	Added   ChangeKind = "added"
	Removed ChangeKind = "removed"
)

// Change is one key's move between two snapshots. Noisy changes are flagged,
// never dropped.
type Change struct {
	Key    string
	Kind   ChangeKind
	Before string
	After  string
	// At is the time of the later snapshot.
	At    time.Time
	Noisy bool
}

// Diff compares two value sets, sorted by key. At is left zero.
func Diff(before, after Values, noisy map[string]bool) []Change {
	var changes []Change
	for _, key := range before.Keys() {
		b, _ := before.Get(key)
		a, ok := after.Get(key)
		switch {
		case !ok:
			changes = append(changes, Change{Key: key, Kind: Removed, Before: b, Noisy: noisy[key]})
		case a != b:
			changes = append(changes, Change{Key: key, Kind: Changed, Before: b, After: a, Noisy: noisy[key]})
		}
	}
	for _, key := range after.Keys() {
		if _, ok := before.Get(key); !ok {
			a, _ := after.Get(key)
			changes = append(changes, Change{Key: key, Kind: Added, After: a, Noisy: noisy[key]})
		}
	}
	sort.SliceStable(changes, func(i, j int) bool { return changes[i].Key < changes[j].Key })
	return changes
}

// NoisyKeys is every key that is declared noisy (exact key or a glob) or has
// two or more distinct values across the healthy snapshots.
func NoisyKeys(healthy []Snapshot, declared []string) map[string]bool {
	noisy := map[string]bool{}
	first := map[string]string{}
	for _, snap := range healthy {
		if snap.Error != "" {
			continue
		}
		for _, key := range snap.Values.Keys() {
			value, _ := snap.Values.Get(key)
			if prior, seen := first[key]; !seen {
				first[key] = value
			} else if prior != value {
				noisy[key] = true
			}
			for _, glob := range declared {
				if Match(glob, key) {
					noisy[key] = true
				}
			}
		}
	}
	return noisy
}

// Match reports whether key matches a glob where * matches any run of
// characters and ? matches one. A malformed glob matches nothing.
func Match(glob, key string) bool {
	var pattern strings.Builder
	pattern.WriteString("^")
	for _, r := range glob {
		switch r {
		case '*':
			pattern.WriteString(".*")
		case '?':
			pattern.WriteString(".")
		default:
			pattern.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	pattern.WriteString("$")
	ok, err := regexp.MatchString(pattern.String(), key)
	return err == nil && ok
}

func (r Result) noisy() map[string]bool {
	history := r.HealthyHistory
	if r.Healthy != nil {
		history = append([]Snapshot{*r.Healthy}, history...)
	}
	return NoisyKeys(history, r.Declared)
}

// flag marks a change noisy when the key is noisy in healthy history or
// matches a declared glob, whether or not healthy history ever held the key.
func (r Result) flag(changes []Change, noisy map[string]bool) {
	for i := range changes {
		if noisy[changes[i].Key] {
			changes[i].Noisy = true
			continue
		}
		for _, glob := range r.Declared {
			if Match(glob, changes[i].Key) {
				changes[i].Noisy = true
			}
		}
	}
}

func filter(changes []Change, glob string) []Change {
	var out []Change
	for _, c := range changes {
		if Match(glob, c.Key) {
			out = append(out, c)
		}
	}
	return out
}

// DiffersFromHealthy compares the newest snapshot with the last healthy one
// for keys matching glob. ok is false when there is no baseline (or no usable
// newest snapshot): the caller must treat that as unknown, never as no change.
func (s State) DiffersFromHealthy(glob string) (changes []Change, ok bool) {
	latest, has := s.Latest()
	if !has || latest.Error != "" || s.Healthy == nil || s.Healthy.Error != "" {
		return nil, false
	}
	diff := Diff(s.Healthy.Values, latest.Values, s.noisy())
	s.flag(diff, nil)
	for i := range diff {
		diff[i].At = latest.At
	}
	return filter(diff, glob), true
}

// usable lists the ring's snapshots that hold values, oldest first.
func (s State) usable() []Snapshot {
	var out []Snapshot
	for _, snap := range s.Recent {
		if snap.Error == "" {
			out = append(out, snap)
		}
	}
	return out
}

// ChangedWithin lists changes between consecutive usable ring snapshots (a
// failed read in between does not hide a change) whose later snapshot lies in
// (now-d, now], for keys matching glob. ok is false when fewer than two
// snapshots are usable or the ring does not reach back to now-d: the changes
// it did see are returned, but the caller must not read an empty list as "no
// change". now comes from the observation clock.
func (s State) ChangedWithin(glob string, now time.Time, d time.Duration) (changes []Change, ok bool) {
	snaps := s.usable()
	if len(snaps) < 2 {
		return nil, false
	}
	noisy := s.noisy()
	for i := 1; i < len(snaps); i++ {
		prev, next := snaps[i-1], snaps[i]
		if next.At.After(now) || !next.At.After(now.Add(-d)) {
			continue
		}
		diff := Diff(prev.Values, next.Values, noisy)
		s.flag(diff, nil)
		for j := range diff {
			diff[j].At = next.At
		}
		changes = append(changes, filter(diff, glob)...)
	}
	return changes, !snaps[0].At.After(now.Add(-d))
}

// HeldAbove reports whether the numeric value of key exceeded n for the whole
// window [now-d, now]: the value in force at now-d (the last snapshot at or
// before it) and every later snapshot. ok is false when no snapshot is at or
// before now-d (the ring does not reach back far enough), or a snapshot in
// the window failed, lacks the key or holds a non-numeric value.
func (s State) HeldAbove(key string, n float64, now time.Time, d time.Duration) (held, ok bool) {
	start := -1
	for i, snap := range s.Recent {
		if !snap.At.After(now.Add(-d)) {
			start = i
		}
	}
	if start < 0 {
		return false, false
	}
	held = true
	for _, snap := range s.Recent[start:] {
		if snap.At.After(now) {
			break
		}
		raw, present := snap.Values.Get(key)
		if snap.Error != "" || !present {
			return false, false
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return false, false
		}
		if !(value > n) {
			held = false
		}
	}
	return held, true
}
