package contractsv1alpha1_test

import (
	"testing"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func menuSelection() *contracts.MitigationSelection {
	return &contracts.MitigationSelection{Version: 1, IncidentId: "synthetic-b3", SessionId: "provider-session", DispatchEpoch: 7, RequestId: "retry-stable-request", MenuId: "menu-revision", OptionId: "routing-then-restart", ProgramRevision: "program-digest", DependencyRevision: "dependencies-digest", SourceCommit: "accepted-source", ScopeDigest: "named-scope", PolicyDigest: "menu-policy", RoundLimit: 2}
}

func TestMitigationSelectionValidationAndRoundTrip(t *testing.T) {
	message := menuSelection()
	if err := protovalidate.Validate(message); err != nil {
		t.Fatal(err)
	}
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(contracts.MitigationSelection)
	if err := protojson.Unmarshal(raw, decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(message, decoded) {
		t.Fatalf("selection changed across ProtoJSON: %s", raw)
	}
	for _, limit := range []uint32{0, 101} {
		message.RoundLimit = limit
		if err := protovalidate.Validate(message); err == nil {
			t.Errorf("accepted round limit %d", limit)
		}
	}
}

func TestMitigationSelectionRejectsAmbiguousBindingsAndAgentActions(t *testing.T) {
	message := menuSelection()
	message.Bindings = []*contracts.Binding{{Name: "service", Object: programRef()}, {Name: "service", Object: programRef()}}
	if err := protovalidate.Validate(message); err == nil {
		t.Fatal("duplicate binding names accepted")
	}
	if err := protojson.Unmarshal([]byte(`{"actions":[{"delete":"Secret/app/token"}]}`), message); err == nil {
		t.Fatal("agent-supplied action accepted")
	}
}

func TestMitigationTerminalStatusesAndUnknownActionState(t *testing.T) {
	for _, status := range []contracts.MitigationReport_Status{contracts.MitigationReport_STATUS_SUCCESS, contracts.MitigationReport_STATUS_REFUSED, contracts.MitigationReport_STATUS_EXECUTION_ERROR, contracts.MitigationReport_STATUS_UNRESOLVED, contracts.MitigationReport_STATUS_DIFFERENT_CAUSE, contracts.MitigationReport_STATUS_SUPERSEDED, contracts.MitigationReport_STATUS_INTERRUPTED, contracts.MitigationReport_STATUS_TIMEOUT} {
		report := &contracts.MitigationReport{Version: 1, IncidentId: "synthetic-b3", SessionId: "provider-session", RequestId: "retry-stable-request", AttemptId: "attempt-unique", AcceptanceEpoch: 7, Status: status}
		if err := protovalidate.Validate(report); err != nil {
			t.Fatalf("%v: %v", status, err)
		}
		raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		decoded := new(contracts.MitigationReport)
		if err := protojson.Unmarshal(raw, decoded); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(report, decoded) {
			t.Fatalf("report changed: %s", raw)
		}
	}
	action := &contracts.MitigationActionReceipt{ActionId: "attempt/1/forward/1", Resource: programRef(), Effect: contracts.MitigationOption_EFFECT_SCOPED_ROUTING_SET_FIELD, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}
	if err := protovalidate.Validate(action); err != nil {
		t.Fatal(err)
	}
	action.State = contracts.MitigationActionReceipt_State(99)
	if err := protovalidate.Validate(action); err == nil {
		t.Fatal("unknown wire enum accepted")
	}
}
