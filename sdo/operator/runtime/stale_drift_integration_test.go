package runtime

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Reproduce L4's two drift-only arrivals on opposite sides of the first
// finding's individual deadline. One repair fixes both Services, even though
// the responder's frozen request contains only the first finding.
func TestPendingDriftJoinsIncidentBeforeVerifiedClosure(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "without_restart"
		if restart {
			name = "restore_before_verification"
		}
		t.Run(name, func(t *testing.T) {
			c, client, cache, _, start := newDriftScenario(t)
			c.drift.BatchWindow = 20 * time.Second
			dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 2)}
			c.dispatcher = dispatcher
			injectScenarioPort(t, client, cache, "api")
			stepScenario(t, c, cache, start.Add(time.Second))
			injectScenarioPort(t, client, cache, "peer")
			stepScenario(t, c, cache, start.Add(time.Second+111*time.Millisecond))
			stepScenario(t, c, cache, start.Add(21*time.Second))
			dispatchScenario(t, c)
			first := awaitRequest(t, dispatcher.requests)
			if len(first.Findings) != 1 || first.Findings[0].PrimaryResource.Name != "api" {
				t.Fatalf("fixture did not split the individual deadlines: %+v", first.Findings)
			}
			awaitDispatchCompletionQueued(t, c)
			for _, name := range []string{"api", "peer"} {
				service, err := client.CoreV1().Services("app").Get(context.Background(), name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
				if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if restart {
				c.processDispatchCompletions(start.Add(21 * time.Second))
				if err := c.RestoreState(exportState(t, c)); err != nil {
					t.Fatal(err)
				}
			}
			// The peer deadline has not expired when verification runs.
			c.scheduler.ExpediteAll(start.Add(21*time.Second + 50*time.Millisecond))
			stepScenario(t, c, cache, start.Add(21*time.Second+50*time.Millisecond))
			settled := start.Add(21*time.Second + 50*time.Millisecond + c.config.ClosureStabilityWindow)
			c.scheduler.ExpediteAll(settled)
			stepScenario(t, c, cache, settled)
			closure, ok := c.PendingIncidentClosure()
			if !ok || len(closure.FinalDetectorStates) == 0 {
				t.Fatal("repair did not independently verify")
			}
			if len(closure.Request.Findings) != 2 || len(closure.Request.DriftEvents) != 2 {
				t.Fatalf("the incident outcome lost the queued peer drift: %+v", closure.Request)
			}
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "repair", "scripted", "fixture")
			if err != nil || len(derived.Record.GetDriftEvents()) != 2 || derived.Record.GetTimestamps().GetVerifiedAt() == nil {
				t.Fatalf("resolved drift missing from outcome: %v %v", derived, err)
			}
			acknowledgeScenario(t, c)
			for _, at := range []time.Duration{22 * time.Second, 60 * time.Second} {
				stepScenario(t, c, cache, start.Add(at))
				if c.IncidentOpen() || len(exportState(t, c).PendingBatch.Findings) != 0 {
					t.Fatal("verified repair queued or opened a stale incident")
				}
			}
			if c.tracker.HasActiveDetector("drift.unexplained") || dispatcher.callCount() != 1 {
				t.Fatal("stale drift binding or extra responder survived acknowledgement")
			}
			// The same binding must still activate for a genuinely new fault.
			injectScenarioPort(t, client, cache, "peer")
			stepScenario(t, c, cache, start.Add(61*time.Second))
			stepScenario(t, c, cache, start.Add(81*time.Second))
			dispatchScenario(t, c)
			second := awaitRequest(t, dispatcher.requests)
			if second.IncidentID == first.IncidentID || len(second.Findings) != 1 || second.Findings[0].DetectorID != "drift.unexplained" || second.Findings[0].PrimaryResource.Name != "peer" {
				t.Fatalf("fresh drift was suppressed or incorrectly bound: %+v", second)
			}
			awaitDispatchCompletionQueued(t, c)
			for _, finding := range first.Findings {
				if finding.Status != sdk.FindingActive {
					t.Fatal("frozen responder request was mutated")
				}
			}
		})
	}
}

func TestPendingDriftPreventsVerificationWhenPeerIsUnresolved(t *testing.T) {
	for _, sourceError := range []bool{false, true} {
		name := "peer_still_drifted"
		if sourceError {
			name = "source_unavailable"
		}
		t.Run(name, func(t *testing.T) {
			c, client, cache, requests, start := newDriftScenario(t)
			c.drift.BatchWindow = 20 * time.Second
			injectScenarioPort(t, client, cache, "api")
			stepScenario(t, c, cache, start.Add(time.Second))
			injectScenarioPort(t, client, cache, "peer")
			stepScenario(t, c, cache, start.Add(time.Second+111*time.Millisecond))
			stepScenario(t, c, cache, start.Add(21*time.Second))
			dispatchScenario(t, c) // The scripted responder repairs only its api finding.
			awaitRequest(t, requests)
			awaitDispatchCompletionQueued(t, c)
			if sourceError {
				c.drift.View = func(sdk.DetectionContext) program.ReadView { return failedSourceDriftView{} }
			}
			verifyAt := start.Add(21*time.Second + 50*time.Millisecond)
			c.scheduler.ExpediteAll(verifyAt)
			stepScenario(t, c, cache, verifyAt)
			if _, ok := c.PendingIncidentClosure(); ok || !c.IncidentOpen() {
				t.Fatal("unresolved queued drift was verified as repaired")
			}
			state := exportState(t, c)
			peerRetained := false
			for _, finding := range state.IncidentRequest.Findings {
				peerRetained = peerRetained || finding.PrimaryResource.Name == "peer"
			}
			if !peerRetained || len(state.PendingBatch.Findings) != 0 || !c.tracker.HasActiveDetector("drift.unexplained") {
				t.Fatal("unresolved drift did not join the active verification")
			}
		})
	}
}
