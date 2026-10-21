package contractsv1alpha1_test

import (
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func programRef() *contracts.ObjectRef {
	return &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "frontend"}
}

func programVerdict() *contracts.Verdict {
	return &contracts.Verdict{
		Result:   contracts.Verdict_RESULT_MATCH,
		Evidence: []*contracts.Evidence{{Resource: programRef(), Fact: "target port", Observed: "9999", Expected: "8080"}},
	}
}

func TestProgramContractsProtoJSONRoundTrip(t *testing.T) {
	value, err := structpb.NewValue(int64(8080))
	if err != nil {
		t.Fatal(err)
	}
	plan := &contracts.Plan{Actions: []*contracts.Action{
		{Kind: &contracts.Action_ReconcileFromSource{ReconcileFromSource: &contracts.ReconcileFromSource{Ref: programRef()}}},
		{Kind: &contracts.Action_Rollback{Rollback: &contracts.Rollback{Ref: programRef(), ToRevision: "abc123"}}},
		{Kind: &contracts.Action_SetField{SetField: &contracts.SetField{Ref: programRef(), Path: "spec.ports[0].targetPort", Value: value, Reason: "restore source port"}}},
		{Kind: &contracts.Action_RestartRollout{RestartRollout: &contracts.RestartRollout{Ref: programRef(), Reason: "reload configuration"}}},
	}, Rationale: "restore service"}
	checks := []proto.Message{
		&contracts.ProgramSpec{Id: "service-targetport-mismatch", Params: []*contracts.ParamSpec{{Name: "service", Kind: "Service", Required: true}}},
		&contracts.Binding{Name: "service", Object: programRef()},
		programVerdict(),
		&contracts.Verdict{Result: contracts.Verdict_RESULT_NEEDS_JUDGEMENT, Question: "Which port is intended?"},
		plan,
		&contracts.ProgramRun{ProgramId: "service-targetport-mismatch", Bindings: []*contracts.Binding{{Name: "service", Object: programRef()}}, Verdict: programVerdict(), Duration: durationpb.New(time.Second)},
		&contracts.PlanApplication{Plan: plan, Applied: true, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}},
	}
	for _, message := range checks {
		raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
		if err != nil {
			t.Fatalf("marshal %T: %v", message, err)
		}
		decoded := proto.Clone(message)
		proto.Reset(decoded)
		if err := protojson.Unmarshal(raw, decoded); err != nil {
			t.Fatalf("unmarshal %T: %v", message, err)
		}
		if !proto.Equal(message, decoded) {
			t.Fatalf("round trip changed %T: %s", message, raw)
		}
	}
}

func TestProgramContractValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	valid := []proto.Message{
		&contracts.ProgramSpec{Id: "service-targetport-mismatch", Params: []*contracts.ParamSpec{{Name: "service", Kind: "Service", Required: true}}},
		&contracts.Binding{Name: "service", Object: programRef()},
		programVerdict(),
		&contracts.Verdict{Result: contracts.Verdict_RESULT_NEEDS_JUDGEMENT, Question: "Which port?"},
		&contracts.Plan{}, // An empty plan is valid after a repair.
		&contracts.ProgramRun{ProgramId: "p", Verdict: programVerdict(), Duration: durationpb.New(0)},
		&contracts.PlanApplication{Plan: &contracts.Plan{}, DryRun: true},
	}
	for _, message := range valid {
		if err := validator.Validate(message); err != nil {
			t.Errorf("rejected valid %T: %v", message, err)
		}
	}
	invalid := []proto.Message{
		&contracts.ProgramSpec{},
		&contracts.ParamSpec{Name: "service"},
		&contracts.Binding{Name: "service"},
		&contracts.Verdict{},
		&contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH, Question: "Unexpected question"},
		&contracts.Verdict{Result: contracts.Verdict_RESULT_NEEDS_JUDGEMENT},
		&contracts.Evidence{Fact: "target port"},
		&contracts.Action{},
		&contracts.Action{Kind: &contracts.Action_Rollback{Rollback: &contracts.Rollback{Ref: programRef()}}},
		&contracts.ProgramRun{Verdict: programVerdict(), Duration: durationpb.New(0)},
		&contracts.PlanApplication{},
	}
	for _, message := range invalid {
		if err := validator.Validate(message); err == nil {
			t.Errorf("accepted invalid %T: %v", message, message)
		}
	}
}
