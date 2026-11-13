package appstate_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/sdk/appstate"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCapturedAppStateReplaysThroughTheCarrierAndJSON(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var healthy, bad appstate.Values
	healthy.Set("limit", "10")
	bad.Set("limit", "1")
	baseline := appstate.Snapshot{At: now.Add(-5 * time.Minute), Values: healthy}
	source := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &now, AppState: map[string]appstate.Result{
		"settings": {Reader: "settings", Healthy: &baseline, Recent: []appstate.Snapshot{baseline, {At: now, Values: bad}}},
	}}
	captured, err := sdktest.CaptureSnapshot(inputs.New(source), "app")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var back sdktest.Snapshot
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.AppState, source.AppState) {
		t.Fatalf("replay lost app state:\n got %+v\nwant %+v", back.AppState, source.AppState)
	}
	state, err := appstate.Require(back, "settings")
	if err != nil {
		t.Fatal(err)
	}
	changes, ok := state.DiffersFromHealthy("limit")
	if !ok || len(changes) != 1 || changes[0].Before != "10" || changes[0].After != "1" {
		t.Fatalf("changes = %v ok=%v", changes, ok)
	}
	if recent, _ := state.ChangedWithin("limit", back.ObservedAt(), time.Minute); len(recent) != 1 {
		t.Fatalf("ChangedWithin on the replay clock = %v", recent)
	}
}
