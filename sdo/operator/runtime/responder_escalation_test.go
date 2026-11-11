package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
)

// A responder that confirmed an out-of-reach cause from the opening evidence
// stops the response: health cannot return from another turn.
func TestResponderEscalatedOnConfirmedOutOfReachCause(t *testing.T) {
	store := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "db"}
	request := IncidentRequest{Findings: []sdk.Finding{{DetectorID: "service-reachability", RuleID: "service-port-unreachable", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "Service db is unreachable although its pod is Ready", Evidence: "5 dials timed out", PrimaryResource: store, Fingerprint: "service-reachability/db"}}}
	cause := ConfirmedRootCause{Summary: "the network path to db drops every packet", Resources: []sdk.ObjectRef{store}, CauseClass: "network-loss", Reach: "out-of-reach",
		Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: "service-reachability", Observation: "5 dials timed out while the pod stayed Ready"}}}
	if !responderEscalated(request, IncidentResult{ConfirmedRootCauses: []ConfirmedRootCause{cause}}) {
		t.Fatal("a confirmed out-of-reach cause backed by an opening finding must stop the response")
	}
	inScope := cause
	inScope.Reach = "in-scope"
	if responderEscalated(request, IncidentResult{ConfirmedRootCauses: []ConfirmedRootCause{inScope}}) {
		t.Fatal("an in-scope cause keeps the response going")
	}
	unbacked := cause
	unbacked.Evidence = []RootCauseEvidence{{Kind: "live-read", Source: "kubectl", Observation: "dial timed out"}}
	if responderEscalated(request, IncidentResult{ConfirmedRootCauses: []ConfirmedRootCause{unbacked}}) {
		t.Fatal("an out-of-reach claim without opening controller evidence keeps the response going")
	}
	if responderEscalated(request, IncidentResult{}) {
		t.Fatal("no cause, no escalation")
	}
}
