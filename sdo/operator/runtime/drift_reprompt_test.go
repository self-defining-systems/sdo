package runtime

import (
	"context"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
)

func TestDriftOnlyIncidentGetsOneExtraHealthyTurn(t *testing.T) {
	c, d, step := driftOnlyController(t)
	// Supply the owned-field evidence admitted with a drift-only opening finding.
	// The view performs the same real closure-source comparison as mixed incidents.
	event := c.currentIncidentRequest.DriftEvents[0]
	event.Fields = []sdk.DriftField{{Path: "spec.ports[0].targetPort", Old: "8087", New: "9999", Source: "8087"}}
	c.currentIncidentRequest.DriftEvents[0] = event
	for i := range c.currentIncidentRequest.Findings {
		if strings.HasPrefix(c.currentIncidentRequest.Findings[i].DetectorID, "drift.") {
			c.currentIncidentRequest.Findings[i].Metadata = map[string]any{"drift_event": event}
		}
	}
	c.drift.View = func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}}
	}
	for i := 2; i < 10; i++ {
		step(i)
	}
	if !c.IncidentOpen() {
		t.Fatal("drift-only closure skipped initial turn")
	}
	close(d.release)
	awaitDispatchCompletionQueued(t, c)
	step(10)
	// A real durable continuation is seeded with the returned response, as the
	// Kubernetes dispatcher does before publishing completion to the controller.
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	client := fake.NewSimpleClientset()
	var healthy atomic.Bool
	healthy.Store(true)
	loop := continuationLoop(request, &healthy, 1)
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	if err := dispatcher.restoreToolState(context.Background(), request, loop); err != nil {
		t.Fatal(err)
	}
	result, err := incidentResultContract(*c.currentIncidentResult)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Done: true, Result: result}); err != nil {
		t.Fatal(err)
	}
	c.dispatcher = dispatcher
	c.responderLoop = loop
	closed := 0
	c.OnIncidentClosed = func(IncidentClosure) { closed++ }
	step(30)
	if c.responderDone || closed != 0 || !loop.driftRepromptPending() {
		t.Fatal("drift-only incident skipped extra turn")
	}
	step(40)
	if closed != 0 {
		t.Fatal("drift-only extra turn closed early")
	}
	// Complete that same turn, then restart the closure decision with drift left.
	state := loop.continuation
	state.Done = true
	if err := loop.saveContinuation(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	c.responderDone = true
	c.responderCompletedAt = time.Unix(40, 0)
	step(60)
	if closed != 1 {
		t.Fatalf("remaining drift gated drift-only closure: %d", closed)
	}
}

