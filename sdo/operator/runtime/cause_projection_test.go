package runtime

import (
	"context"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// populate sets every field of message (recursively) to a non-zero value that
// satisfies its string `in` rule, so a field added to the contract but not to a
// runtime projection fails the round trip below instead of vanishing silently.
func populate(message protoreflect.Message) {
	fields := message.Descriptor().Fields()
	for index := range fields.Len() {
		field := fields.Get(index)
		switch {
		case field.Kind() == protoreflect.MessageKind && field.IsList():
			populate(message.Mutable(field).List().AppendMutable().Message())
		case field.Kind() == protoreflect.MessageKind:
			populate(message.Mutable(field).Message())
		case field.Kind() == protoreflect.StringKind:
			value := protoreflect.ValueOfString(stringFixture(field))
			if field.IsList() {
				message.Mutable(field).List().Append(value)
			} else {
				message.Set(field, value)
			}
		default:
			panic("populate: unsupported field " + string(field.FullName()))
		}
	}
}

func stringFixture(field protoreflect.FieldDescriptor) string {
	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	strings := rules.GetString()
	if strings == nil {
		strings = rules.GetRepeated().GetItems().GetString()
	}
	if allowed := strings.GetIn(); len(allowed) > 0 {
		return allowed[len(allowed)-1]
	}
	return "fixture-value"
}

// Every field a responder reports on a confirmed root cause must survive the
// controller's runtime state and reach the derived outcome record unchanged.
func TestResponderCauseFieldsReachTheOutcome(t *testing.T) {
	reported := &contractsv1alpha1.ConfirmedRootCause{}
	populate(reported.ProtoReflect())
	if reported.GetCauseClass() == "" || reported.GetReach() == "" {
		t.Fatalf("fixture leaves typed cause fields empty: %+v", reported)
	}
	result := incidentResultState(&contractsv1alpha1.IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentId: "inc-cause", Status: string(IncidentCompleted),
		ConfirmedRootCauses: []*contractsv1alpha1.ConfirmedRootCause{reported},
	})
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	closure := IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: "inc-cause",
			SourceCommit: "base", DeployedCommit: "base", RepositoryWorktree: "/proposal", RepositoryBaseCommit: "base",
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
			Findings: []sdk.Finding{{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
				Summary: "fault", Evidence: "evidence", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "health"}, Fingerprint: "health"}},
		},
		Result:     &result,
		DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second),
		Frozen: true,
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	causes := derived.Record.GetConfirmedRootCauses()
	if len(causes) != 1 {
		t.Fatalf("outcome causes = %+v, want the reported cause", causes)
	}
	if !proto.Equal(reported, causes[0]) {
		t.Fatalf("reported cause changed on its way to the outcome:\nreported: %v\nstored:   %v", reported, causes[0])
	}
}
