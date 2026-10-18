package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestIncidentRequestGoldenFixtureRoundTrip(t *testing.T) {
	var request IncidentRequest
	payload := roundTripGoldenFixture(t, "incident_request.json", &request)

	if request.SchemaVersion != ProtocolSchemaVersion {
		t.Fatalf("unexpected schema version %q", request.SchemaVersion)
	}
	if got := request.Findings[0].ParameterBindings["missing_config_map"].Name; got != "geo-config" {
		t.Fatalf("unexpected missing ConfigMap binding %q", got)
	}
	if got := payload["cancellation_token"]; got != "cancel-inc-20260709-0001" {
		t.Fatalf("unexpected cancellation token %#v", got)
	}
	if request.RepairPolicy != "commit" {
		t.Fatalf("unexpected repair policy %q", request.RepairPolicy)
	}
}

func TestIncidentRequestDoesNotExposeOperationalMemoryPaths(t *testing.T) {
	encoded, err := json.Marshal(IncidentRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"architecture_summary_path", "health_objective_path"} {
		if _, found := fields[key]; found {
			t.Fatalf("incident request exposes physical operational memory field %q", key)
		}
	}
}

func TestRuntimePlaybookAndArtifactReferencesUseLogicalIDs(t *testing.T) {
	for name, value := range map[string]any{
		"surfaced": SurfacedPlaybook{},
		"applied": AppliedPlaybook{},
		"result": IncidentResult{},
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if name != "result" && !strings.Contains(string(encoded), `"playbook_id"`) {
			t.Fatalf("%s has no logical playbook ID: %s", name, encoded)
		}
		if name == "applied" && !strings.Contains(string(encoded), `"script_names"`) {
			t.Fatalf("applied playbook has no logical script names: %s", encoded)
		}
		if name == "result" && !strings.Contains(string(encoded), `"proposed_artifact_ids"`) {
			t.Fatalf("result has no logical artifact IDs: %s", encoded)
		}
	}
}

func TestIncidentRequestStateChangesFixtureRoundTrip(t *testing.T) {
	var request IncidentRequest
	roundTripGoldenFixture(t, "incident_request_state_changes.json", &request)

	if request.StateChanges == nil || len(request.StateChanges.Changes) != 3 || request.StateChanges.Omitted != 2 ||
		request.StateChanges.Changes[0].Fields[0].Field != "selector" || request.StateChanges.UnobservedKinds[0] != "Secret" {
		t.Fatalf("state changes must match the Python contract: %+v", request.StateChanges)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestIncidentResultGoldenFixtureRoundTrip(t *testing.T) {
	var result IncidentResult
	roundTripGoldenFixture(t, "incident_result.json", &result)

	if result.Status != IncidentCompleted {
		t.Fatalf("unexpected incident status %q", result.Status)
	}
	if len(result.VerificationEvidence) != 1 || !result.VerificationEvidence[0].Passed {
		t.Fatalf("unexpected verification evidence %#v", result.VerificationEvidence)
	}
	if len(result.RepairActions) != 1 || !result.RepairActions[0].Success {
		t.Fatalf("unexpected repair actions %#v", result.RepairActions)
	}
}

func TestSurfacedPlaybookOmitsAbsentBindingsForPythonProtocolDefault(t *testing.T) {
	payload, err := json.Marshal(SurfacedPlaybook{PlaybookID: "health-objective"})
	if err != nil {
		t.Fatalf("marshal playbook: %v", err)
	}
	if string(payload) != `{"playbook_id":"health-objective"}` {
		t.Fatalf("nil bindings must be omitted, got %s", payload)
	}
}

func roundTripGoldenFixture(t *testing.T, name string, target any) map[string]any {
	t.Helper()
	path := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	var original map[string]any
	if err := json.Unmarshal(content, &original); err != nil {
		t.Fatalf("decode original fixture: %v", err)
	}
	if err := json.Unmarshal(content, target); err != nil {
		t.Fatalf("decode typed fixture: %v", err)
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("encode typed fixture: %v", err)
	}
	var roundTripped map[string]any
	if err := json.Unmarshal(encoded, &roundTripped); err != nil {
		t.Fatalf("decode round-tripped fixture: %v", err)
	}
	if !reflect.DeepEqual(roundTripped, original) {
		t.Fatalf("protocol round trip changed payload:\noriginal: %#v\nround trip: %#v", original, roundTripped)
	}
	return roundTripped
}
