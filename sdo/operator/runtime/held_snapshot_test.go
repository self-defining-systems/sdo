package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type fixedProvider struct{ ctx sdk.DetectionContext }

func (p fixedProvider) Snapshot(context.Context) (sdk.DetectionContext, error) { return p.ctx, nil }

// The open incident's alert keys ride in every snapshot, and a closed
// incident's do not.
func TestSnapshotsCarryTheOpenIncidentsAlertKeys(t *testing.T) {
	c := &Controller{}
	c.provider = heldSnapshotProvider{Base: fixedProvider{sdktest.Snapshot{NamespaceName: "app"}}, held: &c.heldKeys}
	snapshot := func() sdk.DetectionContext {
		t.Helper()
		got, err := c.provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if held := sdk.HeldFingerprints(snapshot(), "request-metrics"); len(held) != 0 {
		t.Fatalf("held with no incident: %v", held)
	}
	c.incidentOpen = true
	c.currentIncidentRequest = &IncidentRequest{OpeningKeys: []string{FindingStateKey("request-metrics", "requestmetrics/errors/op")}, JoinedKeys: []string{FindingStateKey("traffic-probe-call", "scenario")}}
	c.publishHeldLocked()
	held := sdk.HeldFingerprints(snapshot(), "request-metrics")
	if !held["requestmetrics/errors/op"] || len(held) != 1 {
		t.Fatalf("held = %v", held)
	}
	c.incidentOpen = false
	c.currentIncidentRequest = nil
	c.publishHeldLocked()
	if held := sdk.HeldFingerprints(snapshot(), "request-metrics"); len(held) != 0 {
		t.Fatalf("held after close: %v", held)
	}
}
