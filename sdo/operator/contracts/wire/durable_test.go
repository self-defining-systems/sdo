package wire

import (
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestDurablePlanEvidenceDropsOnlyRetiredJournals(t *testing.T) {
	data := []byte(`{"plan":{"actions":[{"set_field":{"ref":{"api_version":"v1","kind":"Service","namespace":"app","name":"api"},"path":"spec.selector","value":{"pre_images":{"preserve":true},"patches":[1,2]},"reason":"restore selector"}}]},"applied":true,"pre_images":[{"object":{"spec":{"old":true}}}],"patches":[{"patch_json":"retired write"}]}`)
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
	value := application.GetPlan().GetActions()[0].GetSetField().GetValue().GetStructValue()
	if value.GetFields()["pre_images"] == nil || len(value.GetFields()["patches"].GetListValue().GetValues()) != 2 {
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
