package submission

import (
	"google.golang.org/protobuf/encoding/protojson"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func encode(t testing.TB, request *c.SubmissionGuardRequest) []byte {
	t.Helper()
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSubmissionGuardNeverPostsAnotherPhase(t *testing.T) {
	for _, phase := range []string{"diagnosis", "mitigation"} {
		for _, stage := range []string{"setup", "diagnosis", "grading", "mitigation", "done", "completed", "finished", "awaiting_cleanup", "unknown"} {
			for _, sent := range []bool{false, true} {
				request := &c.SubmissionGuardRequest{Phase: phase, ConductorEvent: &c.SubmissionGuardRequest_Status{Status: &c.ConductorStatus{Stage: stage}}, SubmissionSent: sent}
				result := Decide(encode(t, request))
				wantPost := !sent && phase == stage
				if (result.GetAction() == c.SubmissionGuardDecision_ACTION_POST) != wantPost {
					t.Fatalf("phase=%s stage=%s sent=%v: %v", phase, stage, sent, result)
				}
				if result.GetFailure() != nil {
					t.Fatal(result)
				}
			}
		}
	}
}

func TestLateDiagnosisReturnsTargetedFeedback(t *testing.T) {
	got := Decide(encode(t, &c.SubmissionGuardRequest{Phase: "diagnosis", ConductorEvent: &c.SubmissionGuardRequest_Status{Status: &c.ConductorStatus{Stage: "mitigation"}}}))
	if got.GetAction() != c.SubmissionGuardDecision_ACTION_RETURN || got.GetResponse().GetDiagnosis().GetStatus() != "already_submitted" || got.GetResponse().GetDiagnosis().GetStage() != "mitigation" {
		t.Fatal(got)
	}
}

func TestSubmissionAcknowledgementsArePhaseSpecific(t *testing.T) {
	for _, phase := range []string{"diagnosis", "mitigation"} {
		for _, status := range []string{"200", "ok", "acknowledged", "done", "error", ""} {
			got := Decide(encode(t, &c.SubmissionGuardRequest{Phase: phase, ConductorEvent: &c.SubmissionGuardRequest_Acknowledgement{Acknowledgement: &c.ConductorAcknowledgement{Status: status}}}))
			switch status {
			case "error", "":
				if got.GetFailure().GetCode() != c.SubmissionGuardFailure_CODE_NOT_ACKNOWLEDGED {
					t.Fatal(got)
				}
			case "done":
				if got.GetAction() != c.SubmissionGuardDecision_ACTION_RETURN {
					t.Fatal(got)
				}
			default:
				want := c.SubmissionGuardDecision_ACTION_RETURN
				if phase == "mitigation" {
					want = c.SubmissionGuardDecision_ACTION_WAIT
				}
				if got.GetAction() != want {
					t.Fatal(got)
				}
			}
		}
	}
}

func FuzzSubmissionPhaseBoundary(f *testing.F) {
	for _, phase := range []string{"diagnosis", "mitigation"} {
		for _, stage := range []string{"diagnosis", "mitigation", "done", "awaiting_cleanup"} {
			f.Add(encode(f, &c.SubmissionGuardRequest{Phase: phase, ConductorEvent: &c.SubmissionGuardRequest_Status{Status: &c.ConductorStatus{Stage: stage}}}))
		}
	}
	f.Add([]byte(`{"phase":"diagnosis","status":{"stage":42}}`))
	f.Add([]byte{0xff, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		got := Decide(data)
		if got.GetAction() == c.SubmissionGuardDecision_ACTION_POST {
			request := &c.SubmissionGuardRequest{}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, request); err != nil {
				t.Fatal(err)
			}
			if request.GetSubmissionSent() || request.GetPhase() != request.GetStatus().GetStage() || request.GetAcknowledgement() != nil {
				t.Fatalf("wrong-phase POST authorized: %v -> %v", request, got)
			}
		}
		if got.GetFailure() != nil && got.GetFailure().GetCode() == c.SubmissionGuardFailure_CODE_UNSPECIFIED {
			t.Fatal("untyped rejection")
		}
	})
}
