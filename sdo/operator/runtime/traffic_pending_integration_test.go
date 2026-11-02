//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// A process replacement loses steady qualification while retaining the incident.
// Recovery must be observable again, through the same verify transport and
// controller persistence used in production, without carrying old qualification.
func TestTrafficRestartMidIncidentCanVerifyRecovery(t *testing.T) {
	ctx := context.Background()
	frontend := &fakeFrontend{}
	firstAPI := runProber(t, frontend)
	observer := NewTrafficObserver(firstAPI, []string{"health"}, nil, 0)
	detector := trafficDetector("traffic-health", "health")
	waitWindow := func(ready func(traffic.Window) bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			observer.Poll(ctx)
			if ready(observer.Windows()["health"]) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("probe window did not become ready: %+v", observer.Windows())
	}
	waitWindow(func(w traffic.Window) bool {
		return detectorConfirmed(detector, sdktest.Snapshot{Traffic: map[string]traffic.Window{"health": w}})
	})
	start := time.Unix(100, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "hotel-reservation"
	config.VerificationTimeout = time.Minute
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	provider := TrafficSnapshotProvider{Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: config.Namespace}}, Observer: observer}
	c, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	frontend.broken.Store(true)
	waitWindow(func(w traffic.Window) bool {
		verdicts := traffic.Judge(w)
		if len(verdicts) != 2 {
			return false
		}
		for _, v := range verdicts {
			if !v.Evaluated || v.Healthy {
				return false
			}
		}
		return true
	})
	step := func(controller *Controller, i int) {
		t.Helper()
		if err := controller.StepEvents(ctx, start.Add(time.Duration(i)*time.Second), []sdk.WatchKind{traffic.Watch}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i <= 10; i++ {
		step(c, i)
	}
	executePendingEffect(t, c)
	request := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	step(c, 11)
	if !c.IncidentOpen() {
		t.Fatal("failing traffic closed before restart")
	}
	state := exportState(t, c)
	// A new process is constructed while the same application is still broken.
	restartedAPI := runProber(t, frontend)
	observer = NewTrafficObserver(restartedAPI, []string{"health"}, nil, 0)
	waitWindow(func(w traffic.Window) bool {
		if len(w.Scenarios) != 2 {
			return false
		}
		for _, s := range w.Scenarios {
			if s.Qualified || len(s.Samples) < 3 {
				return false
			}
		}
		return true
	})
	provider.Observer = observer
	restored, err := NewController(config, []sdk.Detector{trafficDetector("traffic-health", "health")}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start.Add(12*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	step(restored, 12)
	if got := restored.history[len(restored.history)-1]; got.Status != DetectorEvaluationPending || !strings.Contains(got.Reason, "no synthetic scenario has succeeded") {
		t.Fatalf("restart fabricated health: %+v", got)
	}
	loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{}, 3)
	loop.VerifyClosure = func(ctx context.Context) ToolResponse {
		return restored.cachedResponderVerification(ctx, request.IncidentID, 0)
	}
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if response := exchangeSelfCheck(t, &server, `{"command":"verify"}`); response.Healthy {
		t.Fatal("verify accepted failed unqualified traffic")
	}
	frontend.broken.Store(false)
	burst, err := restartedAPI.Burst(ctx, prober.BurstRequest{Workload: "verify"})
	if err != nil || !burst.Healthy || len(burst.Verdicts) != 2 {
		t.Fatalf("fresh recovered verify burst: %+v %v", burst, err)
	}
	for _, verdict := range burst.Verdicts {
		if !verdict.Evaluated || !verdict.Healthy {
			t.Fatalf("burst did not judge recovered scenario: %+v", verdict)
		}
	}
	waitWindow(func(w traffic.Window) bool {
		if !detectorConfirmed(detector, sdktest.Snapshot{Traffic: map[string]traffic.Window{"health": w}}) {
			return false
		}
		for _, v := range traffic.Judge(w) {
			if !v.Evaluated || !v.Healthy {
				return false
			}
		}
		return true
	})
	step(restored, 13)
	if response := exchangeSelfCheck(t, &server, `{"command":"verify"}`); response.Healthy {
		t.Fatal("verify skipped ordinary clearing persistence")
	}
	if _, healthy := restored.finalVerificationStates(); healthy {
		t.Fatal("first recovered sample cleared active traffic findings")
	}
	var closure IncidentClosure
	restored.OnIncidentClosed = func(closed IncidentClosure) { closure = closed }
	step(restored, 14)
	if !restored.IncidentOpen() {
		t.Fatal("recovered incident committed success before the stability window held")
	}
	step(restored, 14+int(restored.config.ClosureStabilityWindow/time.Second))
	verified := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
	if !verified.Healthy || restored.IncidentOpen() || closure.Request.IncidentID != request.IncidentID || len(closure.FinalDetectorStates) != 1 || closure.FinalDetectorStates[0].Status != DetectorEvaluationClear {
		t.Fatalf("restart could not verify/close recovered incident: verified=%+v closure=%+v", verified, closure)
	}
}
