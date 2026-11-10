package inputs_test

import (
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

// embeddingView is the shape of every runtime read-view wrapper: it embeds
// program.ReadView and so exposes exactly the methods ReadView declares.
type embeddingView struct{ program.ReadView }

// Every observation window CaptureSnapshot keeps must reach it through any
// wrapper that embeds program.ReadView. An input the capture reads by type
// assertion but Sources (and so ReadView) does not declare is silently dropped
// on the live controller path while a bare Snapshot keeps it.
func TestCaptureThroughAnEmbeddingReadViewKeepsEveryObservation(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	source := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &now,
		Traffic:      map[string]traffic.Window{"health": {Workload: traffic.Workload{Name: "health"}, ObservedAt: now}},
		Metrics:      map[string]metrics.Result{"p99": {Error: "synthetic"}},
		Alerts:       &metrics.AlertsResult{Error: "synthetic"},
		Reachability: &servicehealth.ReachabilityWindow{ObservedAt: now, Failures: 3},
		Vantage:      map[string]vantage.Result{"limits": {Error: "synthetic"}},
		Platform:     &sdk.PlatformObservation{ObservedAt: now, Errors: map[string]string{sdk.PlatformRoles: "synthetic"}}}
	want, err := sdktest.CaptureSnapshot(source, "app")
	if err != nil {
		t.Fatal(err)
	}
	got, err := sdktest.CaptureSnapshot(embeddingView{&sdktest.ReplayReadView{Snapshot: source}}, "app")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"Traffic", "Metrics", "Alerts", "Reachability", "ObservedAtTime", "Vantage", "Platform"} {
		if g, w := reflect.ValueOf(got).FieldByName(field).Interface(), reflect.ValueOf(want).FieldByName(field).Interface(); !reflect.DeepEqual(g, w) {
			t.Errorf("%s through an embedding read view = %v, want %v", field, g, w)
		}
	}
}
