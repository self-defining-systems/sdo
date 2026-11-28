package storestate_test

import (
	"slices"
	"testing"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// Store object names come from every reading a snapshot carries: the current
// reading and the healthy baseline, so a collection a fault renamed away still
// counts.
func TestObjectNamesListDatabaseScopedNamespaces(t *testing.T) {
	id := storestate.QueryID(storestate.EngineMongoDB, "store-a")
	renamed := healthyMongo[:len(healthyMongo)-len("coll\texampledb\torders\t3\tmoderate/warn\nindex\texampledb\torders\tplaced_1\tplaced:-1 ttl=3600\n")]
	view := sdktest.Snapshot{PodReads: map[string]podread.Result{id: {
		Readings: []podread.Reading{{Namespace: "app", Pod: "store-a-0", Stdout: renamed}},
		Baseline: []podread.Reading{{Namespace: "app", Pod: "store-a-0", Stdout: healthyMongo}},
	}}}
	got := storestate.ObjectNames(view)
	want := []string{"exampledb.items", "exampledb.orders", "exampledb/items", "exampledb/orders"}
	if !slices.Equal(got, want) {
		t.Fatalf("ObjectNames = %v, want %v", got, want)
	}
	if got := storestate.ObjectNames(sdktest.Snapshot{}); len(got) != 0 {
		t.Fatalf("a view without store reads named %v", got)
	}
}
