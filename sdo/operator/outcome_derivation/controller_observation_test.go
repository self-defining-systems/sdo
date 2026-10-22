package outcome_derivation

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"slices"
	"testing"
	"time"
)

func observationJSON(t testing.TB, request *c.ControllerObservationRequest) []byte {
	t.Helper()
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func observationRequest() *c.ControllerObservationRequest {
	return &c.ControllerObservationRequest{State: &c.ControllerState{IncidentOpen: true, IncidentRequest: &c.IncidentRequest{IncidentId: "two"}, PendingClosure: &c.IncidentClosure{Request: &c.IncidentRequest{IncidentId: "two"}, VerifiedAt: timestamppb.New(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC))}, LastAcknowledgedIncidentId: "one"}, KnownIncidentIds: []string{"one"}, IncidentId: "two", StopOnReview: true}
}

func TestControllerObservationUsesGeneratedClosureState(t *testing.T) {
	req := observationRequest()
	req.State.ClosureState = c.ClosureState_CLOSURE_STATE_FAILED
	req.State.ClosureFailure = &c.ClosureFailure{IncidentId: "two", Permanent: true, Attempts: 8, LastError: "invalid detector", Action: "inspect broker ledger"}
	got := ObserveController(observationJSON(t, req))
	if got.GetFailure().GetCode() != c.ControllerObservationFailure_CODE_CLOSURE_FAILED || got.GetFailure().GetIncidentId() != "two" {
		t.Fatal(got)
	}
	req.IncidentId = "other"
	if got := ObserveController(observationJSON(t, req)); got.GetFailure() != nil {
		t.Fatal("unrelated failure poisoned requested incident", got)
	}
	req.IncidentId = "two"
	req.State.ClosureState = c.ClosureState_CLOSURE_STATE_PENDING
	req.State.ClosureFailure.Permanent = false
	if got := ObserveController(observationJSON(t, req)); got.GetFailure() != nil {
		t.Fatal("retryable failure became permanent", got)
	}
}

func TestControllerObservationCorrelatesVerifiedAndAcknowledgedIncidents(t *testing.T) {
	req := observationRequest()
	before := proto.Clone(req)
	got := ObserveController(observationJSON(t, req))
	if got.GetVerifiedIncidentId() != "two" || got.GetVerifiedClosure() == nil || !got.GetNewIncidentOpened() || got.GetAcknowledged() || !slices.Equal(got.GetKnownIncidentIds(), []string{"one", "two"}) {
		t.Fatal(got)
	}
	if !proto.Equal(before, req) {
		t.Fatal("observation mutated accepted state")
	}
	req.State.LastAcknowledgedIncidentId = "two"
	req.State.PendingClosure = nil
	got = ObserveController(observationJSON(t, req))
	if !got.GetAcknowledged() || got.GetVerifiedIncidentId() != "two" || got.GetVerifiedClosure() != nil {
		t.Fatal(got)
	}
	req.KnownIncidentIds = []string{"two"}
	got = ObserveController(observationJSON(t, req))
	if got.GetVerifiedIncidentId() != "" {
		t.Fatal("known incident was rediscovered", got)
	}
}

func TestControllerObservationOrdersReviewAfterVerifiedHealth(t *testing.T) {
	req := observationRequest()
	req.State.DetectorReviewRequired = true
	req.State.DetectorReviewReason = "health detector is stale"
	if got := ObserveController(observationJSON(t, req)); got.GetFailure() != nil {
		t.Fatal("verified closure lost to later review", got)
	}
	req.State.PendingClosure = nil
	got := ObserveController(observationJSON(t, req))
	if got.GetFailure().GetCode() != c.ControllerObservationFailure_CODE_DETECTOR_REVIEW_REQUIRED {
		t.Fatal(got)
	}
	req.StopOnReview = false
	if got := ObserveController(observationJSON(t, req)); got.GetFailure() != nil {
		t.Fatal(got)
	}
}

func FuzzControllerObservationBoundary(f *testing.F) {
	f.Add(observationJSON(f, observationRequest()))
	f.Add([]byte(`{"state":{"closure_state":"CLOSURE_STATE_FAILED","closure_failure":{"incident_id":"two","permanent":true}},"incident_id":"two"}`))
	f.Add([]byte(`{"state":{"closure_state":12345}}`))
	f.Add([]byte(`{"state":{"closure_state":"failed"}}`))
	f.Add([]byte{0xff, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		got := ObserveController(data)
		if got.GetFailure() != nil && got.GetFailure().GetCode() == c.ControllerObservationFailure_CODE_UNSPECIFIED {
			t.Fatal("untyped failure")
		}
		if got.GetAcknowledged() || got.GetVerifiedIncidentId() != "" {
			req := &c.ControllerObservationRequest{}
			if err := protojson.Unmarshal(data, req); err != nil {
				t.Fatal(err)
			}
			if got.GetAcknowledged() && (req.GetIncidentId() == "" || req.GetState().GetLastAcknowledgedIncidentId() != req.GetIncidentId()) {
				t.Fatal("unrelated acknowledgment", got)
			}
			if got.GetVerifiedIncidentId() != "" && slices.Contains(req.GetKnownIncidentIds(), got.GetVerifiedIncidentId()) {
				t.Fatal("known incident rediscovered", got)
			}
		}
		if next := ObserveController(observationJSON(t, observationRequest())); next.GetVerifiedIncidentId() != "two" {
			t.Fatal("failure poisoned next snapshot", next)
		}
	})
}
