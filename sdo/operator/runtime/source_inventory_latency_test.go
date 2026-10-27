package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Both arms execute the same closure checker over the same clean hotel-shaped
// configuration. No added sleep, probe or extra responder turn is permitted.
func BenchmarkSourceInventoryClosure(b *testing.B) {
	declared, live := hotelInventory()
	for _, enabled := range []bool{false, true} {
		name := "before"
		if enabled {
			name = "after"
		}
		b.Run(name, func(b *testing.B) {
			c := &Controller{config: ControllerConfig{Namespace: "app", SourceCommit: "accepted"}, currentIncidentRequest: &IncidentRequest{}, sourceInventory: NewSourceInventory("app", "accepted", declared), drift: &DriftControllerConfig{Inventory: enabled, Events: &inventoryStore{objects: live}}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clear, findings := c.verifyClosureDrift(context.Background(), time.Unix(100, 0), nil)
				if !clear || len(findings) != 0 {
					b.Fatal("clean closure blocked")
				}
			}
		})
	}
}
func BenchmarkSourceInventoryInformerHotel(b *testing.B) {
	declared, live := hotelInventory()
	c, err := newCacheForTest(b, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		b.Fatal(err)
	}
	for _, o := range live {
		key := o.GetAPIVersion() + "/" + o.GetKind()
		if informer := c.informers[key]; informer != nil {
			if err := informer.GetStore().Add(o); err != nil {
				b.Fatal(err)
			}
		}
	}
	index := NewSourceInventory("app", "accepted", declared)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if findings := index.Findings(c.SourceDriftObjects(), time.Unix(100, 0)); len(findings) != 0 {
			b.Fatal(findings)
		}
	}
}

func TestSourceInventorySymptomaticClosureTimingParity(t *testing.T) {
	type result struct {
		verified   time.Time
		dispatches int
	}
	run := func(enabled bool) result {
		start := time.Unix(100, 0).UTC()
		config := testControllerConfig()
		config.Namespace = "app"
		health := controllerDetector("health", time.Second, stateFinding("health"), stateFinding("health"), sdk.Finding{}, sdk.Finding{}, sdk.Finding{})
		health.spec = inventoryHealth().Spec()
		dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
		c, err := NewController(config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, dispatcher, start)
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			declared, live := hotelInventory()
			c.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: &inventoryStore{objects: live}, Source: &inventorySource{objects: declared}})
		}
		var verified time.Time
		c.OnIncidentClosed = func(closure IncidentClosure) { verified = closure.VerifiedAt }
		for i := 0; i < 2; i++ {
			if err := c.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
				t.Fatal(err)
			}
		}
		executePendingEffect(t, c)
		awaitRequest(t, dispatcher.requests)
		awaitDispatchCompletionQueued(t, c)
		for i := 2; i < 20; i++ {
			if err := c.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
				t.Fatal(err)
			}
		}
		return result{verified: verified, dispatches: dispatcher.calls}
	}
	before, after := run(false), run(true)
	if before.verified.IsZero() || !before.verified.Equal(after.verified) || before.dispatches != after.dispatches {
		t.Fatalf("added mitigation latency or turns: before=%+v after=%+v", before, after)
	}
	t.Logf("same scripted closure=%s; dispatches before=%d after=%d; additional re-prompts=0", before.verified, before.dispatches, after.dispatches)
}
