package storestate_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

func withBaseline(s sdktest.Snapshot, baseline ...podread.Reading) sdktest.Snapshot {
	for id, result := range s.PodReads {
		result.Baseline = baseline
		s.PodReads[id] = result
	}
	return s
}

// A detector Go installs carries no baseline of its own: the controller's
// promoted healthy readings travel with the pod read, and the detector diffs
// against them.
func TestDetectorDiffsAgainstThePromotedHealthyReadings(t *testing.T) {
	d := detector(t, config(t))
	changed := strings.Replace(healthyRedis, "role:master", "role:slave", 1)
	findings, err := d.Detect(context.Background(), withBaseline(snapshot(t, d, reading(changed)), reading(healthyRedis)))
	if err != nil {
		t.Fatal(err)
	}
	var role bool
	for _, f := range findings {
		role = role || f.RuleID == storestate.RuleRoleChanged
	}
	if !role {
		t.Fatalf("no role change against the promoted baseline: %+v", findings)
	}
	findings, err = d.Detect(context.Background(), withBaseline(snapshot(t, d, reading(healthyRedis)), reading(healthyRedis)))
	if err != nil || len(findings) != 0 {
		t.Fatalf("unchanged against the promoted baseline: %+v, %v", findings, err)
	}
}

// A promoted reading that does not parse into a healthy baseline is ignored:
// the detector falls back to absolute facts and never errors on it.
func TestDetectorIgnoresAnUnusablePromotedReading(t *testing.T) {
	d := detector(t, config(t))
	for name, baseline := range map[string]podread.Reading{
		"unparsable": reading("sh: redis-cli: not found\n"),
		"blocked":    reading(strings.Replace(healthyRedis, "min-replicas-to-write\n0", "min-replicas-to-write\n1", 1)),
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := d.Detect(context.Background(), withBaseline(snapshot(t, d, reading(healthyRedis)), baseline))
			if err != nil || len(findings) != 0 {
				t.Fatalf("%+v, %v", findings, err)
			}
		})
	}
}

// The pod read names the engine and the component, so a reader without the
// detector (a Diagnose read view, the incident request) can parse it.
func TestQueryIDNamesEngineAndComponent(t *testing.T) {
	d := detector(t, config(t))
	id := d.(podread.Consumer).PodReads()[0].ID
	if id != storestate.QueryID(storestate.EngineRedis, "cache") {
		t.Fatalf("query ID %q", id)
	}
	engine, component, ok := storestate.ParseQueryID(id)
	if !ok || engine != storestate.EngineRedis || component != "cache" {
		t.Fatalf("ParseQueryID(%q) = %q, %q, %v", id, engine, component, ok)
	}
	if _, _, ok := storestate.ParseQueryID("store-state-cache"); ok {
		t.Fatal("an ID without an engine parsed")
	}
	if _, _, ok := storestate.ParseQueryID("policy"); ok {
		t.Fatal("a foreign pod read parsed")
	}
}

// Changes is the read-only view of every store the view carries a read of:
// each Pod's diff against the promoted baseline, and Baselined false where
// there is no baseline to compare with.
func TestChangesListsEachStoreDiffAgainstItsBaseline(t *testing.T) {
	d := detector(t, config(t))
	changed := strings.Replace(healthyRedis, "min-replicas-to-write\n0", "min-replicas-to-write\n1", 1)
	view := withBaseline(snapshot(t, d, reading(changed)), reading(healthyRedis))
	view.PodReads["policy"] = podread.Result{Query: podread.Query{ID: "policy"}, Readings: []podread.Reading{{Pod: "other", Stdout: "x"}}}
	stores := storestate.Changes(view)
	if len(stores) != 1 {
		t.Fatalf("stores = %+v", stores)
	}
	got := stores[0]
	if got.Component != "cache" || got.Engine != storestate.EngineRedis || got.Pod.Name != "cache-0" || !got.Baselined || got.Error != "" {
		t.Fatalf("store = %+v", got)
	}
	var setting bool
	for _, c := range got.Changes {
		setting = setting || (c.Key() == storestate.Key{Kind: storestate.KindSetting, Name: "min-replicas-to-write"})
	}
	if !setting {
		t.Fatalf("changes do not name the setting: %+v", got.Changes)
	}
	if got.BaselineAt != time.Unix(100, 0).UTC() {
		t.Fatalf("baseline at %v", got.BaselineAt)
	}

	unbaselined := storestate.Changes(snapshot(t, d, reading(changed)))
	if len(unbaselined) != 1 || unbaselined[0].Baselined || len(unbaselined[0].Changes) != 0 {
		t.Fatalf("without a baseline: %+v", unbaselined)
	}
	failed := storestate.Changes(snapshot(t, d, podread.Reading{Namespace: "shop", Pod: "cache-0", Error: "exec timed out"}))
	if len(failed) != 1 || failed[0].Error == "" || failed[0].Pod != (sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "cache-0"}) {
		t.Fatalf("failed read: %+v", failed)
	}
}
