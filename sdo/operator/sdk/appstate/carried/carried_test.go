package carried_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func values(kv ...string) carried.Values {
	var v carried.Values
	for i := 0; i+1 < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	return v
}

func snap(at time.Duration, kv ...string) carried.Snapshot {
	return carried.Snapshot{At: t0.Add(at), Values: values(kv...)}
}

func TestValuesRefuseCredentialNamedKeysAndKeepNoValue(t *testing.T) {
	var v carried.Values
	v.Set("rate_limit", "10")
	v.Set("db_password", "hunter2")
	v.Set("api_token", "x")
	if _, ok := v.Get("db_password"); ok {
		t.Fatal("credential-named key was stored")
	}
	if v.Len() != 1 {
		t.Fatalf("len = %d, want 1", v.Len())
	}
	err := v.Err()
	if err == nil || !strings.Contains(err.Error(), "db_password") {
		t.Fatalf("Err = %v, want refusal naming the key", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("refusal leaked the value: %v", err)
	}
	data, _ := json.Marshal(v)
	if strings.Contains(string(data), "hunter2") {
		t.Fatalf("serialised values leaked: %s", data)
	}
}

func TestValuesBounds(t *testing.T) {
	var v carried.Values
	for i := 0; i < carried.MaxKeys+5; i++ {
		v.Set(fmt.Sprintf("k%03d", i), "v")
	}
	if v.Len() != carried.MaxKeys || v.Err() == nil {
		t.Fatalf("len=%d err=%v, want %d keys and an error", v.Len(), v.Err(), carried.MaxKeys)
	}
	var long carried.Values
	long.Set(strings.Repeat("k", carried.MaxKeyBytes+1), "v")
	long.Set("k", strings.Repeat("v", carried.MaxValueBytes+1))
	if long.Len() != 0 || long.Err() == nil {
		t.Fatalf("oversize accepted: len=%d err=%v", long.Len(), long.Err())
	}
}

func TestValuesJSONRoundTripIsAPlainMap(t *testing.T) {
	data, err := json.Marshal(values("b", "2", "a", "1"))
	if err != nil || string(data) != `{"a":"1","b":"2"}` {
		t.Fatalf("json = %s, %v", data, err)
	}
	var back carried.Values
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if v, _ := back.Get("b"); v != "2" {
		t.Fatalf("round trip lost b: %v", back)
	}
}

func TestDiffReportsChangedAddedRemovedSortedWithNoiseFlag(t *testing.T) {
	before := values("a", "1", "b", "2", "gone", "x")
	after := values("a", "1", "b", "3", "new", "y")
	changes := carried.Diff(before, after, map[string]bool{"b": true})
	got := fmt.Sprint(changes)
	want := fmt.Sprint([]carried.Change{
		{Key: "b", Kind: carried.Changed, Before: "2", After: "3", Noisy: true},
		{Key: "gone", Kind: carried.Removed, Before: "x"},
		{Key: "new", Kind: carried.Added, After: "y"},
	})
	if got != want {
		t.Fatalf("diff = %s\nwant   %s", got, want)
	}
}

func TestNoisyKeysAreDeclaredOrMultiValuedInHealthyHistory(t *testing.T) {
	history := []carried.Snapshot{snap(-4*time.Minute, "a", "1", "b", "1", "c", "1"), snap(-3*time.Minute, "a", "1", "b", "2", "c", "1"), snap(-2*time.Minute, "a", "1", "b", "1", "c", "1")}
	noisy := carried.NoisyKeys(history, []string{"c*"})
	if noisy["a"] || !noisy["b"] || !noisy["c"] {
		t.Fatalf("noisy = %v, want b (two values) and c (declared)", noisy)
	}
}

func TestDiffersFromHealthyNeedsABaseline(t *testing.T) {
	state := carried.State{Result: carried.Result{Reader: "r", Recent: []carried.Snapshot{snap(0, "a", "1")}}}
	if changes, ok := state.DiffersFromHealthy("*"); ok || changes != nil {
		t.Fatalf("no baseline must be ok=false, got %v %v", changes, ok)
	}
}

func TestDiffersFromHealthyComparesLatestAndFlagsNoise(t *testing.T) {
	healthy := snap(-10*time.Minute, "limit", "10", "mode", "a", "jitter", "1")
	state := carried.State{Result: carried.Result{
		Reader:         "r",
		Healthy:        &healthy,
		HealthyHistory: []carried.Snapshot{healthy, snap(-9*time.Minute, "limit", "10", "mode", "a", "jitter", "2")},
		Recent:         []carried.Snapshot{snap(0, "limit", "1", "mode", "a", "jitter", "3")},
	}}
	changes, ok := state.DiffersFromHealthy("*")
	if !ok || len(changes) != 2 {
		t.Fatalf("changes = %v ok=%v", changes, ok)
	}
	if changes[0].Key != "jitter" || !changes[0].Noisy || changes[1].Key != "limit" || changes[1].Noisy {
		t.Fatalf("changes = %v", changes)
	}
	only, _ := state.DiffersFromHealthy("lim*")
	if len(only) != 1 || only[0].Key != "limit" {
		t.Fatalf("glob filter = %v", only)
	}
}

func TestChangedWithinUsesTheRingAndTheGivenClock(t *testing.T) {
	state := carried.State{Result: carried.Result{Reader: "r", Recent: []carried.Snapshot{
		snap(-10*time.Minute, "a", "1"), snap(-9*time.Minute, "a", "2"), snap(-1*time.Minute, "a", "2"), snap(0, "a", "3"),
	}}}
	changes, ok := state.ChangedWithin("a", t0, 2*time.Minute)
	if !ok || len(changes) != 1 || changes[0].After != "3" || !changes[0].At.Equal(t0) {
		t.Fatalf("changes = %v ok=%v", changes, ok)
	}
	wide, _ := state.ChangedWithin("a", t0, 20*time.Minute)
	if len(wide) != 2 {
		t.Fatalf("wide = %v", wide)
	}
	if _, ok := (carried.State{}).ChangedWithin("a", t0, time.Minute); ok {
		t.Fatal("an empty ring must be ok=false")
	}
}

func TestHeldAboveRequiresEverySnapshotInTheWindow(t *testing.T) {
	state := carried.State{Result: carried.Result{Reader: "r", Recent: []carried.Snapshot{
		snap(-5*time.Minute, "q", "1"), snap(-3*time.Minute, "q", "9"), snap(-2*time.Minute, "q", "8"), snap(0, "q", "7"),
	}}}
	if held, ok := state.HeldAbove("q", 5, t0, 4*time.Minute); !ok || !held {
		t.Fatalf("held=%v ok=%v, want true", held, ok)
	}
	if held, ok := state.HeldAbove("q", 5, t0, 6*time.Minute); !ok || held {
		t.Fatalf("a low value inside the window must break the hold: held=%v ok=%v", held, ok)
	}
	if _, ok := state.HeldAbove("missing", 5, t0, time.Minute); ok {
		t.Fatal("a key absent from the window is ok=false")
	}
	if _, ok := state.HeldAbove("q", 5, t0.Add(time.Hour), time.Minute); ok {
		t.Fatal("a window with no snapshot is ok=false")
	}
	text := carried.State{Result: carried.Result{Recent: []carried.Snapshot{snap(0, "q", "high")}}}
	if _, ok := text.HeldAbove("q", 5, t0, time.Minute); ok {
		t.Fatal("a non-numeric value is ok=false")
	}
}

type ctxOnly struct {
	sdk.DetectionContext
	results map[string]carried.Result
}

func (c ctxOnly) AppStateResult(id string) (carried.Result, bool) {
	r, ok := c.results[id]
	return r, ok
}

func TestRequireErrorsWhenUnavailableOrFailed(t *testing.T) {
	good := carried.Result{Reader: "ok", Recent: []carried.Snapshot{snap(0, "a", "1")}}
	failed := carried.Result{Reader: "bad", Recent: []carried.Snapshot{{At: t0, Error: "read refused"}}}
	src := ctxOnly{results: map[string]carried.Result{"ok": good, "bad": failed, "empty": {Reader: "empty"}}}
	if _, err := carried.Require(src, "ok"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"bad", "empty", "missing"} {
		if _, err := carried.Require(src, id); err == nil {
			t.Errorf("Require(%q) = nil error", id)
		}
	}
	if _, err := carried.Require(struct{ sdk.DetectionContext }{}, "ok"); err == nil {
		t.Error("a context with no app-state source must be an error")
	}
}
