package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func readContractFixture(t *testing.T, name string, message proto.Message) {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := wire.Unmarshal(content, message); err != nil {
		t.Fatalf("fixture %s is not canonical ProtoJSON: %v", name, err)
	}
}

func goldenRequest(t *testing.T) IncidentRequest {
	t.Helper()
	message := &contracts.IncidentRequest{}
	readContractFixture(t, "incident_request.json", message)
	return *incidentRequestState(message)
}

func TestIncidentRequestGoldenFixtureRoundTrip(t *testing.T) {
	message := &contracts.IncidentRequest{}
	readContractFixture(t, "incident_request.json", message)
	request := incidentRequestState(message)

	if request.SchemaVersion != ProtocolSchemaVersion {
		t.Fatalf("unexpected schema version %q", request.SchemaVersion)
	}
	if got := request.Findings[0].ParameterBindings["missing_config_map"].Name; got != "geo-config" {
		t.Fatalf("unexpected missing ConfigMap binding %q", got)
	}
	if request.CancellationToken != "cancel-inc-20260709-0001" {
		t.Fatalf("unexpected cancellation token %q", request.CancellationToken)
	}
	if request.RepairPolicy != "commit" {
		t.Fatalf("unexpected repair policy %q", request.RepairPolicy)
	}
	again, err := incidentRequestContract(*request)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(message, again) {
		t.Fatalf("projection changed the request:\n%v\n%v", message, again)
	}
}

func TestIncidentRequestDoesNotExposeOperationalMemoryPaths(t *testing.T) {
	fields := (&contracts.IncidentRequest{}).ProtoReflect().Descriptor().Fields()
	for _, name := range []protoreflect.Name{"architecture_summary_path", "health_objective_path"} {
		if fields.ByName(name) != nil {
			t.Fatalf("incident request exposes physical operational memory field %q", name)
		}
	}
}

func TestIncidentRequestStateChangesFixtureRoundTrip(t *testing.T) {
	message := &contracts.IncidentRequest{}
	readContractFixture(t, "incident_request_state_changes.json", message)
	request := incidentRequestState(message)

	if request.StateChanges == nil || len(request.StateChanges.Changes) != 3 || request.StateChanges.Omitted != 2 ||
		request.StateChanges.Changes[0].Fields[0].Field != "selector" || request.StateChanges.UnobservedKinds[0] != "Secret" {
		t.Fatalf("state changes must match the proto contract: %+v", request.StateChanges)
	}
	if err := validateIncidentRequest(*request); err != nil {
		t.Fatalf("validate: %v", err)
	}
	again, err := incidentRequestContract(*request)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(message, again) {
		t.Fatalf("projection changed the request:\n%v\n%v", message, again)
	}
}

func TestIncidentResultGoldenFixtureRoundTrip(t *testing.T) {
	message := &contracts.IncidentResult{}
	readContractFixture(t, "incident_result.json", message)
	result := incidentResultState(message)

	if result.Status != IncidentCompleted {
		t.Fatalf("unexpected incident status %q", result.Status)
	}
	if len(result.VerificationEvidence) != 1 || !result.VerificationEvidence[0].Passed {
		t.Fatalf("unexpected verification evidence %#v", result.VerificationEvidence)
	}
	if len(result.RepairActions) != 1 || !result.RepairActions[0].Success {
		t.Fatalf("unexpected repair actions %#v", result.RepairActions)
	}
	again, err := incidentResultContract(result)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(message, again) {
		t.Fatalf("projection changed the result:\n%v\n%v", message, again)
	}
}
