package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
)

// With --state-baseline=false the controller has no tracker. Programs that ask
// for the healthy baseline get an error, not a nil-pointer panic.
func TestBaselineOfNilTrackerIsAbsent(t *testing.T) {
	if source := baselineOf(nil); source != nil {
		t.Fatalf("baselineOf(nil) = %#v, want a nil interface", source)
	}
	view := LiveReadView{BaselineSource: baselineOf(nil)}
	if _, err := view.DiffAgainstBaseline(sdk.ObjectRef{Kind: "ConfigMap", Name: "x", Namespace: "app"}); err == nil {
		t.Fatal("diff against an absent baseline succeeded")
	}
}
