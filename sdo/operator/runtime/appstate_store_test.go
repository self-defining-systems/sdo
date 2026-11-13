package runtime

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/appstate/carried"
)

var storeT0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func stateSnap(at time.Duration, kv ...string) carried.Snapshot {
	var v carried.Values
	for i := 0; i+1 < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	return carried.Snapshot{At: storeT0.Add(at), Values: v}
}

func TestAppStateStoreKeepsTheLast30SnapshotsPerReader(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil})
	for i := 0; i < carried.RingSize+7; i++ {
		store.Record("r", stateSnap(time.Duration(i)*time.Minute, "n", fmt.Sprint(i)))
	}
	result, ok := store.Result("r")
	if !ok || len(result.Recent) != carried.RingSize {
		t.Fatalf("ring = %d, want %d", len(result.Recent), carried.RingSize)
	}
	if v, _ := result.Recent[0].Values.Get("n"); v != "7" {
		t.Fatalf("oldest kept = %s, want 7", v)
	}
	if v, _ := result.Recent[len(result.Recent)-1].Values.Get("n"); v != fmt.Sprint(carried.RingSize+6) {
		t.Fatalf("newest = %s", v)
	}
}

func TestAppStateStoreBoundsReadersAndForgetsRemovedOnes(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	readers := map[string][]string{}
	for i := 0; i < carried.MaxReaders+3; i++ {
		readers[fmt.Sprintf("r%02d", i)] = nil
	}
	store.Retain(readers)
	kept := 0
	for id := range readers {
		store.Record(id, stateSnap(0, "a", "1"))
		if _, ok := store.Result(id); ok {
			kept++
		}
	}
	if kept != carried.MaxReaders {
		t.Fatalf("kept %d readers, want %d", kept, carried.MaxReaders)
	}
	store.Retain(map[string][]string{"r00": nil})
	if _, ok := store.Result("r01"); ok {
		t.Fatal("a reader the bundle no longer registers kept its state")
	}
}

// Last-healthy is the snapshot at the start of a healthy stretch, promoted
// only after the stretch lasts the settle window (the PodReadBaseline rule).
func TestAppStateStoreLastHealthyUsesTheSettleRule(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil})
	at := func(d time.Duration) time.Time { return storeT0.Add(d) }

	// A snapshot read before the stretch began (possibly while unhealthy) is no reference.
	store.Record("r", stateSnap(-30*time.Second, "limit", "stale"))
	store.Observe(at(0), true)
	store.Record("r", stateSnap(10*time.Second, "limit", "10"))
	if r, _ := store.Result("r"); r.Healthy != nil {
		t.Fatal("the first clear evaluation must not be a baseline")
	}
	store.Record("r", stateSnap(time.Minute, "limit", "10"))
	store.Observe(at(time.Minute), true)
	store.Record("r", stateSnap(DefaultStateSettle, "limit", "10"))
	store.Observe(at(DefaultStateSettle), true)
	r, _ := store.Result("r")
	if r.Healthy == nil || !r.Healthy.At.Equal(at(10*time.Second)) {
		t.Fatalf("healthy = %+v, want the first snapshot read inside the settled stretch", r.Healthy)
	}

	// A fault that changes the value never becomes the baseline.
	store.Record("r", stateSnap(DefaultStateSettle+time.Minute, "limit", "1"))
	store.Observe(at(DefaultStateSettle+time.Minute), false)
	store.Observe(at(DefaultStateSettle+2*time.Minute), true)
	store.Record("r", stateSnap(DefaultStateSettle+2*time.Minute+time.Second, "limit", "1"))
	store.Observe(at(DefaultStateSettle+2*time.Minute+time.Minute), true)
	r, _ = store.Result("r")
	if v, _ := r.Healthy.Values.Get("limit"); v != "10" {
		t.Fatalf("an unhealthy stretch changed last-healthy to %s", v)
	}
}

func TestAppStateStoreHealthyHistoryMakesAKeyNoisy(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": {"declared*"}})
	step := DefaultStateSettle / 2
	for i := 0; i < 8; i++ {
		d := time.Duration(i) * step
		store.Observe(storeT0.Add(d), true)
		store.Record("r", stateSnap(d, "stable", "1", "flaps", fmt.Sprint(i%2), "declared_x", "1"))
	}
	result, _ := store.Result("r")
	state := carried.State{Result: result}
	bad := stateSnap(10*time.Minute, "stable", "2", "flaps", "9", "declared_x", "2")
	state.Recent = append(state.Recent, bad)
	changes, ok := state.DiffersFromHealthy("*")
	if !ok || len(changes) != 3 {
		t.Fatalf("changes = %v ok=%v", changes, ok)
	}
	noisy := map[string]bool{}
	for _, c := range changes {
		noisy[c.Key] = c.Noisy
	}
	if noisy["stable"] || !noisy["flaps"] || !noisy["declared_x"] {
		t.Fatalf("noisy flags = %v", noisy)
	}
}

func TestAppStateStoreRestartIsAGapNotNoChange(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil})
	store.Record("r", stateSnap(0, "a", "1"))
	result, _ := store.Result("r")
	if _, ok := (carried.State{Result: result}).DiffersFromHealthy("*"); ok {
		t.Fatal("a fresh store has no baseline: ok must be false")
	}
}

func TestAppStateStoreChangesListsRecentMovesAcrossReaders(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"a": nil, "b": nil})
	store.Record("a", stateSnap(-20*time.Minute, "k", "1"))
	store.Record("a", stateSnap(-10*time.Minute, "k", "2"))
	store.Record("b", stateSnap(-20*time.Minute, "z", "1"))
	store.Record("b", stateSnap(-1*time.Minute, "z", "1"))
	changes := store.Changes(storeT0, 30*time.Minute)
	if len(changes) != 1 || changes[0].Reader != "a" || changes[0].Change.Key != "k" || changes[0].Change.After != "2" {
		t.Fatalf("changes = %+v", changes)
	}
}

func TestAppStateStoreIsTheChangesCSVSeam(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"a": nil})
	store.Record("a", stateSnap(-20*time.Minute, "k", "1"))
	store.Record("a", stateSnap(-10*time.Minute, "k", "2"))
	var source AppStateChangeSource = store
	rows := source.AppStateChanges(storeT0.Add(-30*time.Minute), storeT0)
	if len(rows) != 1 || rows[0].Source != "app-state" || rows[0].ReaderOrObject != "a" || rows[0].Before != "1" || rows[0].After != "2" {
		t.Fatalf("rows = %+v", rows)
	}
}

// settleHealthyBaseline records base as the last-healthy snapshot of reader r at
// storeT0-50min (the first snapshot of a stretch that then settles).
func settleHealthyBaseline(store *AppStateStore, r string, kv ...string) {
	start := -50 * time.Minute
	store.Observe(storeT0.Add(start), true)
	store.Record(r, stateSnap(start+time.Second, kv...))
	store.Observe(storeT0.Add(start+DefaultStateSettle+time.Second), true)
}

// A key that exists on one side of last-healthy only is a change even when no
// two ring snapshots disagree (the move happened before the ring began).
func TestAppStateChangesListKeysAppearingAndDisappearingSinceLastHealthy(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil})
	settleHealthyBaseline(store, "r", "gone", "7", "same", "1")
	store.Record("r", stateSnap(-10*time.Minute, "same", "1", "new", "16"))
	store.Record("r", stateSnap(-5*time.Minute, "same", "1", "new", "16"))
	rows := store.AppStateChanges(storeT0.Add(-30*time.Minute), storeT0)
	got := map[string]IncidentChange{}
	for _, row := range rows {
		got[row.Key] = row
	}
	if row := got["new"]; row.Before != AppStateAbsent || row.After != "16" || row.ReaderOrObject != "r" {
		t.Fatalf("new key row = %+v (rows %+v)", row, rows)
	}
	if row := got["gone"]; row.Before != "7" || row.After != AppStateAbsent {
		t.Fatalf("vanished key row = %+v (rows %+v)", row, rows)
	}
	if _, listed := got["same"]; listed {
		t.Fatalf("an unchanged key was listed: %+v", rows)
	}
}

func TestAppStateChangesListAKeyThatAppearedBetweenSnapshotsOnce(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil})
	settleHealthyBaseline(store, "r", "plan_limits.rows", "0")
	store.Record("r", stateSnap(-10*time.Minute, "plan_limits.rows", "0"))
	store.Record("r", stateSnap(-5*time.Minute, "plan_limits.rows", "1", "plan.x.limit", "16"))
	var keys []string
	for _, row := range store.AppStateChanges(storeT0.Add(-30*time.Minute), storeT0) {
		keys = append(keys, row.Key)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("keys = %v, want each moved key exactly once", keys)
	}
}

// A reader whose reads fail leaves no change to derive; that must be a row,
// not silence.
func TestAppStateChangesSurfaceAFailingReader(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil, "ok": nil})
	store.Record("r", carried.Snapshot{At: storeT0.Add(-3 * time.Minute), Error: "store read refused: boom"})
	store.Record("r", carried.Snapshot{At: storeT0.Add(-2 * time.Minute), Error: "store read refused: boom"})
	store.Record("ok", stateSnap(-3*time.Minute, "k", "1"))
	rows := store.AppStateChanges(storeT0.Add(-30*time.Minute), storeT0)
	if len(rows) != 1 || rows[0].ReaderOrObject != "r" || rows[0].Key != AppStateReadKey || !strings.Contains(rows[0].After, "boom") {
		t.Fatalf("rows = %+v", rows)
	}
}
