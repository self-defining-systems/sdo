//go:build integration

package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

func TestResponderVerifyWaitStablePassAndReactivation(t *testing.T) {
	for _, reactivate := range []bool{false, true} {
		name := "stable"
		if reactivate {
			name = "reactivation"
		}
		t.Run(name, func(t *testing.T) {
			samples := []sdk.Finding{stateFinding("health"), stateFinding("health"), {}, {}, {}}
			if reactivate {
				samples = append(samples, stateFinding("health"), stateFinding("health"))
			}
			c, _, step := earlyWaitController(t, samples...)
			step(2)
			id := c.currentIncidentRequest.IncidentID
			entered := &waitEntryContext{Context: context.Background(), entered: make(chan struct{})}
			result := make(chan ToolResponse, 1)
			go func() { result <- c.cachedResponderVerification(entered, id, time.Second) }()
			select {
			case <-entered.entered:
			case <-time.After(time.Second):
				t.Fatal("wait did not start")
			}
			end := 17
			if reactivate {
				end = 6
			}
			for i := 3; i <= end; i++ {
				step(i)
			}
			select {
			case response := <-result:
				if reactivate {
					if response.Healthy || len(response.Findings) != 1 || response.IncidentVerified != nil {
						t.Fatalf("reactivation: %+v", response)
					}
				} else if !response.Healthy || response.IncidentVerified == nil {
					t.Fatalf("stable: %+v", response)
				}
			case <-time.After(time.Second):
				t.Fatal("cached-state waiter did not wake")
			}
		})
	}
}

func TestResponderVerifyWaitTimeoutAndInstantCachedRead(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"))
	step(2)
	id := c.currentIncidentRequest.IncidentID
	// A cached read must work even if a new snapshot would be unavailable.
	c.provider = nil
	before := time.Now()
	plain := c.cachedResponderVerification(context.Background(), id, 0)
	if time.Since(before) > 100*time.Millisecond || plain.Healthy {
		t.Fatalf("plain cached read: %+v", plain)
	}
	before = time.Now()
	response := c.cachedResponderVerification(context.Background(), id, 20*time.Millisecond)
	elapsed := time.Since(before)
	if elapsed < 20*time.Millisecond || elapsed > time.Second || response.Healthy || response.IncidentVerified != nil {
		t.Fatalf("timeout %v: %+v", elapsed, response)
	}
}

