package runtime

import (
	"context"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

// The real scheduler/tracker/controller runs against a synthetic snapshot while
// the scripted responder remains blocked. No wall-clock sleeping is involved.
func TestControllerEarlyClosureStableAndFlapping(t *testing.T) {
	for _, flap := range []bool{false, true} {
		name := "stable"
		if flap {
			name = "flap"
		}
		t.Run(name, func(t *testing.T) {
			samples := []sdk.Finding{stateFinding("health"), stateFinding("health"), {}, {}, {}}
			if flap {
				samples = append(samples, stateFinding("health"), sdk.Finding{})
			}
			health := controllerDetector("health", time.Second, samples...)
			health.spec.Class, health.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
			health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 1}
			health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
			health.spec.OriginatingCommit = "health-objective"
			d := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
			defer close(d.release)
			config := testControllerConfig()
			c, err := NewController(config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}}, d, time.Unix(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			closed := make(chan IncidentClosure, 1)
			c.OnIncidentClosed = func(v IncidentClosure) { closed <- v }
			for i := 0; i < 2; i++ {
				if err := c.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
					t.Fatal(err)
				}
			}
			executePendingEffect(t, c)
			awaitRequest(t, d.requests)
			want := 17
			if flap {
				want = 21
			}
			for i := 2; i <= want; i++ {
				if err := c.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
					t.Fatal(err)
				}
				select {
				case closure := <-closed:
					if i != want {
						t.Fatalf("closed at %d, want %d", i, want)
					}
					if !closure.VerifiedAt.Equal(time.Unix(int64(want), 0)) {
						t.Fatalf("verification: %+v", closure)
					}
					if c.responderDone {
						t.Fatal("closure waited for the responder")
					}
					return
				default:
				}
			}
			t.Fatal("stable independent health did not close the running incident")
		})
	}
}

func TestControllerEarlyClosurePendingWriteAndWriteReset(t *testing.T) {
	for _, pending := range []bool{true, false} {
		name := "completed-write"
		if pending {
			name = "pending-write"
		}
		t.Run(name, func(t *testing.T) {
			c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			loop := NewResponderLoop(*c.currentIncidentRequest, nil, c.provider, nil, PlanExecutor{}, 3)
			loop.MutationGate = gate
			c.responderLoop = loop
			for i := 2; i <= 7; i++ {
				step(i)
			}
			lease, err := gate.Begin()
			if err != nil {
				t.Fatal(err)
			}
			end := 8
			if pending {
				for i := 8; i <= 30; i++ {
					step(i)
					if !c.IncidentOpen() {
						t.Fatal("closed during a pending mutation")
					}
				}
				end = 31
			}
			if err := lease.Finish(false); err != nil {
				t.Fatal(err)
			}
			for i := end; i < end+15; i++ {
				step(i)
				if !c.IncidentOpen() {
					t.Fatalf("write did not reset stability at %d", i)
				}
			}
			step(end + 15)
			if c.IncidentOpen() {
				t.Fatal("settled write never verified")
			}
		})
	}
}

func TestControllerEarlyClosureLatentLeftoverBlocks(t *testing.T) {
	declared, live := hotelInventory()
	store := &inventoryStore{objects: append(live, latentInventoryObjects()...)}
	d := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	defer close(d.release)
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	c, err := NewController(cfg, []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, d, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: store, Source: &inventorySource{objects: declared}, BatchWindow: time.Nanosecond})
	step := func(i int) {
		t.Helper()
		if err := c.Step(context.Background(), time.Unix(int64(100+i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	step(0)
	step(1)
	executePendingEffect(t, c)
	awaitRequest(t, d.requests)
	for i := 2; i <= 30; i++ {
		step(i)
		if !c.IncidentOpen() {
			t.Fatal("latent leftover was hidden by healthy detectors")
		}
	}
	store.objects = live
	for i := 31; i <= 46; i++ {
		step(i)
	}
	if c.IncidentOpen() {
		t.Fatal("removed latent objects never verified")
	}
}

func TestControllerEarlyClosureReportGraceAndRestart(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.broker = &recordingIncidentBroker{}
	events := &memoryLifecycleSink{}
	c.SetLifecycleSink(events)
	for i := 2; i <= 17; i++ {
		step(i)
	}
	if _, ok := c.PendingClosureEffect(); ok {
		t.Fatal("reflection started during the responder's report grace")
	}
	closed := events.byPhase(PhaseClosed)
	if len(closed) != 1 || !closed[0].VerifiedWhileResponderRunning || closed[0].ClosureStabilityWindowSeconds != 15 {
		t.Fatalf("telemetry: %v", closed)
	}
	state := exportState(t, c)
	restored, err := NewController(testControllerConfig(), c.detectorList(), c.provider, c.dispatcher, time.Unix(17, 0))
	if err != nil {
		t.Fatal(err)
	}
	restored.broker = &recordingIncidentBroker{}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("restart bypassed report grace")
	}
	if err := restored.Step(context.Background(), time.Unix(46, 0), nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("grace ended early")
	}
	if err := restored.Step(context.Background(), time.Unix(47, 0), nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.PendingClosureEffect(); !ok {
		t.Fatal("report grace never released the frozen closure")
	}
	closure, ok := restored.PendingIncidentClosure()
	if !ok || !closure.VerifiedAt.Equal(time.Unix(17, 0)) {
		t.Fatalf("restart changed mitigation time: %+v", closure)
	}
}
