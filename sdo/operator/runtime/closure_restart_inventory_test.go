package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A verified-healthy closure survives a controller restart. After the restart
// the accepted-source inventory is not loaded yet (or cannot be rendered); that
// is "not yet", never a contradiction that retires the closure.
func TestVerifiedClosureSurvivesControllerRestartBeforeInventoryLoads(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "app"
	config.VerificationTimeout = time.Minute
	declared, live := hotelInventory()
	store := &inventoryStore{objects: append(live, latentInventoryObjects()...)}
	source := &inventorySource{objects: declared}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	c, err := NewController(config, []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: store, Source: source, BatchWindow: time.Nanosecond})
	closed := []IncidentClosure{}
	c.OnIncidentClosed = func(closure IncidentClosure) { closed = append(closed, closure) }
	step := func(c *Controller, n int) {
		t.Helper()
		if err := c.Step(context.Background(), start.Add(time.Duration(n)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	step(c, 0)
	step(c, 1)
	executePendingEffect(t, c)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	step(c, 2)
	store.objects = live
	step(c, 3)
	step(c, 3+int(c.config.ClosureStabilityWindow/time.Second))
	if len(closed) != 1 {
		t.Fatalf("closure did not verify before restart: %+v", closed)
	}
	for _, mode := range []string{"inventory not loaded", "source cannot render"} {
		t.Run(mode, func(t *testing.T) {
			restoredSource := &inventorySource{objects: declared}
			if mode == "source cannot render" {
				restoredSource.err = errors.New("stale lock")
			}
			restored, err := NewController(config, []sdk.Detector{inventoryHealth()}, c.provider, dispatcher, start)
			if err != nil {
				t.Fatal(err)
			}
			restored.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: store, Source: restoredSource, BatchWindow: time.Nanosecond})
			if err := restored.RestoreState(exportState(t, c)); err != nil {
				t.Fatal(err)
			}
			recheck := restored.revalidateClosure(context.Background(), sdktest.Snapshot{NamespaceName: "app"}, closed[0])
			if recheck.Verdict != closureRecheckIncomplete || recheck.Bound <= 0 {
				t.Fatalf("restart before the inventory loaded: %+v, want a bounded wait", recheck)
			}
			if mode == "inventory not loaded" {
				step(restored, 4)
				if recheck := restored.revalidateClosure(context.Background(), sdktest.Snapshot{NamespaceName: "app"}, closed[0]); recheck.Verdict != closureRecheckConfirmed {
					t.Fatalf("closure not confirmed once the inventory loaded: %+v", recheck)
				}
			}
		})
	}
}
