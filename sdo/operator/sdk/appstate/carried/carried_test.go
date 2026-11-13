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

func TestValuesWithholdCredentialNamedStringsAndKeepNoValue(t *testing.T) {
	var v carried.Values
	v.Set("rate_limit", "10")
	v.Set("db_password", "hunter2")
	v.Set("api_token", "x")
	if _, ok := v.Get("db_password"); ok {
		t.Fatal("credential-named key was stored")
	}
	if v.Len() != 1 || v.Withheld() != 2 {
		t.Fatalf("len=%d withheld=%d, want 1 and 2", v.Len(), v.Withheld())
	}
	data, _ := json.Marshal(v)
	if strings.Contains(string(data), "hunter2") {
		t.Fatalf("serialised values leaked: %s", data)
	}
}

func TestValuesKeepBooleansAndIntegersUnderCredentialNames(t *testing.T) {
	var v carried.Values
	for k, val := range map[string]string{
		"password_authentication_enabled_for_web": "true",
		"disable_personal_access_tokens":          "f",
		"minimum_password_length":                 "8",
		"otp_required_for_login":                  "False",
		"enable_hashjoin":                         "t",
		"token_ttl":                               "-30",
	} {
		v.Set(k, val)
		if got, ok := v.Get(k); !ok || got != val {
			t.Fatalf("%s = %q, %v; want kept as %q", k, got, ok, val)
		}
	}
	if v.Withheld() != 0 {
		t.Fatalf("withheld = %d", v.Withheld())
	}
}

func TestValuesWithholdSecretLookingValuesUnderCredentialNames(t *testing.T) {
	var v carried.Values
	for _, val := range []string{"hunter2", "12ab", "1.5", "--1", "+", "", "yes", "1234567890123456789012", "tt"} {
		v.Set("api_token", val)
	}
	if v.Len() != 0 || v.Withheld() != 9 {
		t.Fatalf("len=%d withheld=%d, want 0 and 9", v.Len(), v.Withheld())
	}
}

func TestReportWithheldCountsUnderKeysWithheldPastTheCap(t *testing.T) {
	var v carried.Values
	for i := 0; i < carried.MaxKeys+2; i++ {
		v.Set(fmt.Sprintf("k%03d", i), "v")
	}
	v.ReportWithheld()
	if got, _ := v.Get(carried.WithheldKey); got != "2" {
		t.Fatalf("keys.withheld = %q, want 2", got)
	}
}

func TestValuesBounds(t *testing.T) {
	var v carried.Values
	for i := 0; i < carried.MaxKeys+5; i++ {
		v.Set(fmt.Sprintf("k%03d", i), "v")
	}
	if v.Len() != carried.MaxKeys || v.Withheld() != 5 {
		t.Fatalf("len=%d withheld=%d, want %d keys and 5 withheld", v.Len(), v.Withheld(), carried.MaxKeys)
	}
	var long carried.Values
	long.Set(strings.Repeat("k", carried.MaxKeyBytes+1), "v")
	long.Set("k", strings.Repeat("v", carried.MaxValueBytes+1))
	if long.Len() != 0 || long.Withheld() != 2 {
		t.Fatalf("oversize accepted: len=%d withheld=%d", long.Len(), long.Withheld())
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
	wide, ok := state.ChangedWithin("a", t0, 10*time.Minute)
	if !ok || len(wide) != 2 {
		t.Fatalf("wide = %v ok=%v", wide, ok)
	}
	if _, ok := (carried.State{}).ChangedWithin("a", t0, time.Minute); ok {
		t.Fatal("an empty ring must be ok=false")
	}
}

func TestChangedWithinIsAGapWhenTheRingDoesNotCoverTheWindow(t *testing.T) {
	state := carried.State{Result: carried.Result{Recent: []carried.Snapshot{snap(-2*time.Minute, "a", "1"), snap(0, "a", "2")}}}
	changes, ok := state.ChangedWithin("a", t0, 20*time.Minute)
	if ok || len(changes) != 1 {
		t.Fatalf("a window longer than the ring must report the changes it saw with ok=false: %v ok=%v", changes, ok)
	}
	single := carried.State{Result: carried.Result{Recent: []carried.Snapshot{snap(0, "a", "1")}}}
	if _, ok := single.ChangedWithin("a", t0, 0); ok {
		t.Fatal("one snapshot cannot show a change: ok=false")
	}
}

func TestChangedWithinSeesAChangeAcrossAFailedRead(t *testing.T) {
	state := carried.State{Result: carried.Result{Recent: []carried.Snapshot{
		snap(-4*time.Minute, "a", "1"), {At: t0.Add(-2 * time.Minute), Error: "store down"}, snap(0, "a", "2"),
	}}}
	changes, ok := state.ChangedWithin("a", t0, 4*time.Minute)
	if !ok || len(changes) != 1 || changes[0].Before != "1" || changes[0].After != "2" {
		t.Fatalf("changes = %v ok=%v", changes, ok)
	}
}

func TestDeclaredNoisyKeysAreFlaggedEvenWhenFirstSeenAfterTheBaseline(t *testing.T) {
	healthy := snap(-10*time.Minute, "limit", "10")
	state := carried.State{Result: carried.Result{
		Healthy: &healthy, Declared: []string{"jitter*"},
		Recent: []carried.Snapshot{snap(0, "limit", "10", "jitter_ms", "5")},
	}}
	changes, ok := state.DiffersFromHealthy("*")
	if !ok || len(changes) != 1 || !changes[0].Noisy {
		t.Fatalf("changes = %v ok=%v", changes, ok)
	}
}

func TestHeldAboveCoversTheWholeWindow(t *testing.T) {
	state := carried.State{Result: carried.Result{Reader: "r", Recent: []carried.Snapshot{
		snap(-5*time.Minute, "q", "1"), snap(-3*time.Minute, "q", "9"), snap(-2*time.Minute, "q", "8"), snap(0, "q", "7"),
	}}}
	// The value in force at now-d counts: 9 since -3m.
	if held, ok := state.HeldAbove("q", 5, t0, 3*time.Minute); !ok || !held {
		t.Fatalf("held=%v ok=%v, want true", held, ok)
	}
	// At -4m the value in force was 1.
	if held, ok := state.HeldAbove("q", 5, t0, 4*time.Minute); !ok || held {
		t.Fatalf("a low value in force inside the window must break the hold: held=%v ok=%v", held, ok)
	}
	// Nothing at or before now-6m: the ring does not reach back far enough.
	if _, ok := state.HeldAbove("q", 5, t0, 6*time.Minute); ok {
		t.Fatal("a window older than the ring is ok=false, not held")
	}
	if _, ok := state.HeldAbove("missing", 5, t0, time.Minute); ok {
		t.Fatal("a key absent from the window is ok=false")
	}
	text := carried.State{Result: carried.Result{Recent: []carried.Snapshot{snap(0, "q", "high")}}}
	if _, ok := text.HeldAbove("q", 5, t0, 0); ok {
		t.Fatal("a non-numeric value is ok=false")
	}
	fresh := carried.State{Result: carried.Result{Recent: []carried.Snapshot{snap(0, "q", "9")}}}
	if _, ok := fresh.HeldAbove("q", 5, t0, time.Minute); ok {
		t.Fatal("a reader that just started cannot claim a hold for a minute")
	}
}

func TestValuesLoadTolerantlyWhenAKeyIsNowRefused(t *testing.T) {
	var v carried.Values
	if err := json.Unmarshal([]byte(`{"ok":"1","old_password":"x"}`), &v); err != nil {
		t.Fatalf("a stored snapshot with a now-refused key must still load: %v", err)
	}
	if _, ok := v.Get("old_password"); ok || v.Len() != 1 {
		t.Fatalf("refused key kept: %v", v.Keys())
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
