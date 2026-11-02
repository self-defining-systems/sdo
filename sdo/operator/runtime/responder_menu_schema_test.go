package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
)

func TestMenuParameterErrorsNameNestedFieldAndExpectedShape(t *testing.T) {
	action := rawMenuActions()["EnsureDatabaseUser"].action
	for _, test := range []struct {
		parameters  map[string]string
		path, shape string
	}{
		{map[string]string{"target": `{"engine":"mongodb","pod":[],"container":"mongo","database":"app"}`}, "target.pod", "object"},
		{map[string]string{"target": `{"engine":"mongodb","pod":{"api_version":"v1","kind":"Pod","namespace":"app","name":"db"},"container":"mongo","database":"app","auth":{"database":"admin","username":[],"password":{"env":"PASSWORD"}}}`}, "target.auth.username", "object"},
		{map[string]string{"roles": `[{"database":123,"name":"PRIVATE_CANARY"}]`}, "roles[0].database", "string"},
	} {
		_, err := menuRawPlan(action, test.parameters, &program.ValueInputs{})
		if err == nil || !strings.Contains(err.Error(), test.path) || !strings.Contains(err.Error(), test.shape) || strings.Contains(err.Error(), "PRIVATE_CANARY") {
			t.Fatalf("unsafe/unhelpful parameter error: %v", err)
		}
	}
}

func FuzzMenuToolInputRoundTrip(f *testing.F) {
	for _, seed := range []string{
		`{"command":"menu","parameters":{"target":{"engine":"mongodb"},"roles":[],"port":1,"flag":false}}`,
		`{"command":"mitigate","selection_set":{"version":1,"options":[{"option_id":"SetField","parameters":{"value":8080,"ref":{"kind":"Service","namespace":"app","name":"backend"}}}]}}`,
		`{"selection_set":{"options":[{"parameters":null}]}}`,
		`{"command":"mitigate","selection":{"bindings":[{"name":"count","value":2}]}}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		var request ToolRequest
		if json.Unmarshal(raw, &request) != nil {
			return
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var restored ToolRequest
		if err := json.Unmarshal(encoded, &restored); err != nil {
			t.Fatalf("normalized request cannot roundtrip: %v", err)
		}
		again, err := json.Marshal(restored)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != string(again) {
			t.Fatal("normalized request changed across roundtrip")
		}
	})
}

func TestMenuStructuredNullParametersAreRefused(t *testing.T) {
	for _, raw := range []string{
		`{"command":"mitigate","parameters":{"target":null}}`,
		`{"command":"mitigate","selection_set":{"version":1,"options":[{"option_id":"EnsureDatabaseUser","parameters":{"target":null}}]}}`,
	} {
		var request ToolRequest
		err := json.Unmarshal([]byte(raw), &request)
		if err == nil || !strings.Contains(err.Error(), "parameters.target") || !strings.Contains(err.Error(), "expected") {
			t.Fatalf("null parameter accepted or lost shape: %v", err)
		}
	}
}

