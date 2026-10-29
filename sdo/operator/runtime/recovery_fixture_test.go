package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sort"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Bind integration fixtures to the same published-proof consumer as native
// workers. A standalone loop's detector report is not controller recovery.
func bindRecoveryFixture(c *Controller, loop *ResponderLoop, at time.Time) {
	c.responderLoop = loop
	c.healthDetectorIDs = nil
	for id, detector := range c.detectors {
		if detector.Spec().Class == sdk.DetectorClassHealth {
			c.healthDetectorIDs = append(c.healthDetectorIDs, id)
		}
	}
	sort.Strings(c.healthDetectorIDs)
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.incidentDispatchedAt = at
	loop.VerifyClosure = func(ctx context.Context) ToolResponse {
		return c.cachedResponderVerification(ctx, loop.Request.IncidentID, 0)
	}
}

// Drive actual detector observation and closure publication, never seed a clear
// history or cached proof. Both sides of the stability boundary are mandatory.
func assertRecoveryFixtureWindow(t *testing.T, c *Controller, ctx context.Context, at time.Time) {
	t.Helper()
	if c.currentIncidentRequest == nil {
		t.Fatal("recovery fixture is already terminal before its first observation; check the request deadline")
	}
	id := c.currentIncidentRequest.IncidentID
	var published *IncidentClosure
	previous := c.OnIncidentClosed
	c.OnIncidentClosed = func(closure IncidentClosure) {
		published = &closure
		if previous != nil {
			previous(closure)
		}
	}
	defer func() { c.OnIncidentClosed = previous }()
	for _, elapsed := range []time.Duration{0, c.config.ClosureStabilityWindow - time.Second, c.config.ClosureStabilityWindow} {
		if err := c.Step(ctx, at.Add(elapsed), nil); err != nil {
			t.Fatal(err)
		}
		got := c.cachedResponderVerification(ctx, id, 0)
		want := elapsed == c.config.ClosureStabilityWindow
		if got.Error != "" || got.Healthy != want {
			t.Fatalf("recovery proof at %s: healthy=%v want=%v error=%s summary=%s", elapsed, got.Healthy, want, got.Error, got.Summary+" findings="+fmt.Sprint(got.Findings))
		}
		if want && (published == nil || len(published.FinalDetectorStates) == 0) {
			t.Fatalf("healthy verdict lacks controller-published detector proof at %s", elapsed)
		}
		if !want && published != nil {
			t.Fatalf("controller published closure before stability at %s", elapsed)
		}
		if !want && got.IncidentVerified != nil {
			t.Fatalf("unpublished recovery proof at %s: %+v", elapsed, got.IncidentVerified)
		}
	}
}
