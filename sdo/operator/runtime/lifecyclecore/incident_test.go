package lifecyclecore

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestIncidentCoreTwoIncidentPaths(t *testing.T) {
	at := time.Unix(1000, 0)
	for _, terminal := range []bool{false, true} {
		for _, verified := range []bool{false, true} {
			t.Run(fmt.Sprintf("terminal=%t/verified=%t", terminal, verified), func(t *testing.T) {
				state := &contracts.ControllerState{}
				for incident := 1; incident <= 2; incident++ {
					request := &contracts.IncidentRequest{IncidentId: fmt.Sprint(incident), RepositoryBaseCommit: "accepted"}
					before := proto.Clone(state)
					next, _ := StepIncident(state, IncidentEvent{Kind: IncidentBegin, At: at, Request: request})
					if !proto.Equal(state, before) || !next.IncidentOpen {
						t.Fatal("begin mutated input or failed admission")
					}
					state = next
					_, first := StepIncident(state, IncidentEvent{Kind: IncidentDrive})
					restarted, _ := StepIncident(state, IncidentEvent{Kind: IncidentRestart})
					_, retry := StepIncident(restarted, IncidentEvent{Kind: IncidentDrive, At: at.Add(time.Hour)})
					if len(first) != 1 || len(retry) != 1 || first[0].ID != retry[0].ID || !proto.Equal(first[0].Request, retry[0].Request) {
						t.Fatal("retry identity or payload changed")
					}
					state, _ = StepIncident(state, IncidentEvent{Kind: IncidentEffectStarted, At: at, EffectID: first[0].ID})
					prepared := proto.Clone(request).(*contracts.IncidentRequest)
					prepared.RepositoryWorktree = "/isolated/one"
					state, _ = StepIncident(state, IncidentEvent{Kind: IncidentWorkspacePrepared, Request: prepared})
					if state.DispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
						t.Fatal("workspace did not enable dispatch")
					}
					// A transient watcher failure retains the same request for rejoining.
					state, _ = StepIncident(state, IncidentEvent{Kind: IncidentResponseCompleted, At: at, Request: request, Error: "API unavailable"})
					if state.ResponderDone || state.DispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
						t.Fatal("transport failure was terminal")
					}
					state.IncidentDispatchedAt = timestamppb.New(at)
					event := IncidentEvent{Kind: IncidentResponseCompleted, At: at, Request: request, Result: &contracts.IncidentResult{IncidentId: request.IncidentId}}
					if terminal {
						event.Terminal = true
						event.Error = "responder Job failed"
					}
					state, _ = StepIncident(state, event)
					stale := IncidentEvent{Kind: IncidentResponseCompleted, At: at, Request: &contracts.IncidentRequest{IncidentId: "stale"}, Error: "late", Terminal: true}
					unchanged, _ := StepIncident(state, stale)
					if !proto.Equal(state, unchanged) {
						t.Fatal("stale completion changed current incident")
					}
					verify := IncidentEvent{Kind: IncidentVerify, At: at.Add(time.Minute - time.Nanosecond), VerificationTimeout: time.Minute, Verified: verified, FinalDetectorStates: []*contracts.DetectorEvaluation{{DetectorId: "health"}}}
					waiting, effects := StepIncident(state, verify)
					if !verified && (!waiting.IncidentOpen || len(effects) != 0) {
						t.Fatal("unverified incident closed before deadline")
					}
					verify.At = at.Add(time.Minute)
					state, effects = StepIncident(state, verify)
					if state.IncidentOpen || len(effects) != 1 || state.PendingClosure == nil {
						t.Fatal("incident did not freeze closure")
					}
					if got := len(effects[0].Closure.FinalDetectorStates) > 0; got != verified {
						t.Fatal("unverified repair acquired verified evidence")
					}
					// Spend the closure retry budget and require the second incident to open.
					id := Identity(request.IncidentId, request.RepositoryBaseCommit, ProcessClosure)
					state, _ = Step(state, Event{Kind: Rejected, At: verify.At, EffectID: id, Error: "drain expired", Policy: RetryPolicy{MaxAttempts: 1}})
					if BlocksNext(state.ClosureState, true) || !state.ClosureFailure.Permanent {
						t.Fatal("failed closure blocks next incident")
					}
					at = at.Add(2 * time.Minute)
				}
			})
		}
	}
}

func TestIncidentCoreOwnsOutputPayloads(t *testing.T) {
	request := &contracts.IncidentRequest{IncidentId: "one"}
	state, _ := StepIncident(nil, IncidentEvent{Kind: IncidentBegin, Request: request})
	request.IncidentId = "changed"
	_, effects := StepIncident(state, IncidentEvent{Kind: IncidentDrive})
	effects[0].Request.IncidentId = "mutated"
	if state.IncidentRequest.IncidentId != "one" {
		t.Fatal("input and effect payloads alias core state")
	}
}

func TestIncidentCoreFastPathThreshold(t *testing.T) {
	for threshold := 1; threshold <= 4; threshold++ {
		for successes := 0; successes <= 5; successes++ {
			for failures := 0; failures <= 1; failures++ {
				for _, trusted := range []bool{false, true} {
					for _, revision := range []string{"", "old", "accepted"} {
						credit := FastPathCredit{VerifiedSuccesses: successes, Failures: failures, Trusted: trusted, Revision: revision}
						got := EligibleFastPath(credit, "accepted", threshold)
						want := trusted && revision == "accepted" && failures == 0 && successes >= threshold
						if got != want {
							t.Fatalf("credit=%+v threshold=%d got=%t", credit, threshold, got)
						}
					}
				}
			}
		}
	}
	if EligibleFastPath(FastPathCredit{Trusted: true, VerifiedSuccesses: 3, Revision: "accepted"}, "accepted", 0) {
		t.Fatal("invalid threshold authorized repair")
	}
}
