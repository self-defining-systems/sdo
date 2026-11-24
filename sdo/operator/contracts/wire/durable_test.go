package wire

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestDurablePlanEvidenceDropsOnlyRetiredJournals(t *testing.T) {
	data := []byte(`{"plan":{"actions":[{"set_field":{"ref":{"api_version":"v1","kind":"Service","namespace":"app","name":"api"},"path":"spec.selector","value":{"source":{"ref":{"api_version":"v1","kind":"ConfigMap","namespace":"app","name":"settings"},"path":"data[pre_images.patches]"}},"reason":"restore selector"}}]},"applied":true,"pre_images":[{"object":{"spec":{"old":true}}}],"patches":[{"patch_json":"retired write"}]}`)
	if err := Unmarshal(data, new(contracts.PlanApplication)); err == nil {
		t.Fatal("live wire boundary accepted retired fields")
	}
	var application contracts.PlanApplication
	if err := UnmarshalDurable(data, &application); err != nil {
		t.Fatal(err)
	}
	if !application.GetApplied() {
		t.Fatal("lost applied outcome")
	}
	value := application.GetPlan().GetActions()[0].GetSetField().GetValue().GetSource()
	if value.GetPath() != "data[pre_images.patches]" {
		t.Fatal("migration edited arbitrary action data")
	}
	camel := strings.Replace(string(data), `"pre_images":[`, `"preImages":[`, 1)
	if err := UnmarshalDurable([]byte(camel), new(contracts.PlanApplication)); err != nil {
		t.Fatal(err)
	}
}

func TestDurablePlanEvidenceStillRejectsUnknownFields(t *testing.T) {
	for _, data := range []string{
		`{"plan":{},"unexpected":true}`,
		`{"plan":{},"pre_images":[],"unexpected":true}`,
		`{"plan":{},"patches":[],"actions_applied":[{"unexpected":true}]}`,
	} {
		if err := UnmarshalDurable([]byte(data), new(contracts.PlanApplication)); err == nil {
			t.Fatalf("accepted unknown fields: %s", data)
		}
	}
}

func TestDurableControllerDropsRetiredSweepCursorOnly(t *testing.T) {
	for _, key := range []string{"program_sweep_cursors", "programSweepCursors"} {
		data := []byte(`{"version":"sdo.controller/v1","` + key + `":{"consul":"18,18,18,18,18,18,18"}}`)
		if err := Unmarshal(data, new(contracts.ControllerState)); err == nil {
			t.Fatal("live contract accepted retired cursor")
		}
		state := new(contracts.ControllerState)
		if err := UnmarshalDurable(data, state); err != nil {
			t.Fatalf("cannot restore prior controller state: %v", err)
		}
		if state.Version != "sdo.controller/v1" {
			t.Fatal("migration lost state identity")
		}
		withUnknown := strings.TrimSuffix(string(data), "}") + `,"unknown":true}`
		if err := UnmarshalDurable([]byte(withUnknown), new(contracts.ControllerState)); err == nil {
			t.Fatal("cursor migration accepted unrelated unknown field")
		}
	}
}

// The setup quarantine was retired: a stored document that still carries a
// disabled_setup_detectors list is migrated with the list dropped, so it decodes
// under the current schema, while a strict decode of the raw bytes refuses it.
func TestRetiredSetupQuarantineIsDroppedFromStoredDocuments(t *testing.T) {
	list := `[{"detector_id":"learned","accepted_revision":"abc","fingerprints":["f"],"reason":"fired on the confirmed-healthy app at setup"}]`
	for name, c := range map[string]struct {
		data    string
		message func() proto.Message
	}{
		"controller state":     {`{"version":"sdo.controller/v1","disabled_setup_detectors":` + list + `}`, func() proto.Message { return new(contracts.ControllerState) }},
		"namespace onboarding": {`{"namespace":"app","disabled_setup_detectors":` + list + `}`, func() proto.Message { return new(contracts.NamespaceOnboarding) }},
		"incident request":     {`{"application":"app","disabled_setup_detectors":` + list + `}`, func() proto.Message { return new(contracts.IncidentRequest) }},
		"outcome record":       {`{"incident_id":"i","disabled_setup_detectors":` + list + `}`, func() proto.Message { return new(contracts.OutcomeRecord) }},
	} {
		t.Run(name, func(t *testing.T) {
			if err := protojson.Unmarshal([]byte(c.data), c.message()); err == nil {
				t.Fatal("the current schema accepted the retired field")
			}
			message := c.message()
			migrated, err := migrateRetired([]byte(c.data), message.ProtoReflect().Descriptor())
			if err != nil {
				t.Fatal(err)
			}
			if err := protojson.Unmarshal(migrated, message); err != nil {
				t.Fatalf("a stored document with the retired field no longer decodes: %v", err)
			}
		})
	}
}

func TestDurableScriptGateReasonsMigrate(t *testing.T) {
	data := []byte(`{"playbook_id":"synthetic","result":"skipped","skip_reason":"script_unprovenanced"}`)
	var stage contracts.StageMitigation
	if err := Unmarshal(data, &stage); err == nil {
		t.Fatal("live contract accepted retired script gate reason")
	}
	if err := UnmarshalDurable(data, &stage); err != nil || stage.GetSkipReason() != "plan_not_executable" {
		t.Fatalf("durable stage did not preserve the historical skip: %v", err)
	}

	data = []byte(`{"action_id":"synthetic","kind":"run-script","target":"script","summary":"refused","details":"historical refusal","started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T00:00:00Z","failure_reason":"script_unprovenanced"}`)
	var action contracts.RepairActionReceipt
	if err := UnmarshalDurable(data, &action); err != nil || action.GetFailureReason() != "authorization_denied" {
		t.Fatalf("durable receipt did not preserve the historical refusal: %v", err)
	}
}
