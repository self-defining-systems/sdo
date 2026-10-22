package operatorcli

import (
	"bytes"
	"google.golang.org/protobuf/encoding/protojson"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
)

func TestControllerObservationCLITransportsFailureAsData(t *testing.T) {
	for _, raw := range []string{`{`, `{"state":{"closure_state":"failed"}}`, strings.Repeat("x", 2<<20+1)} {
		var out bytes.Buffer
		if err := RunControllerObservation(strings.NewReader(raw), &out); err != nil {
			t.Fatal(err)
		}
		got := &c.ControllerObservation{}
		if err := protojson.Unmarshal(out.Bytes(), got); err != nil {
			t.Fatal(err)
		}
		if got.GetFailure().GetCode() != c.ControllerObservationFailure_CODE_INVALID_SNAPSHOT {
			t.Fatal(got)
		}
	}
}

func TestControllerObservationCLIProjectsAcknowledgement(t *testing.T) {
	var out bytes.Buffer
	raw := `{"state":{"last_acknowledged_incident_id":"two"},"incident_id":"two"}`
	if err := RunControllerObservation(strings.NewReader(raw), &out); err != nil {
		t.Fatal(err)
	}
	got := &c.ControllerObservation{}
	if err := protojson.Unmarshal(out.Bytes(), got); err != nil {
		t.Fatal(err)
	}
	if !got.GetAcknowledged() || got.GetVerifiedIncidentId() != "two" || got.GetFailure() != nil {
		t.Fatal(got)
	}
}
