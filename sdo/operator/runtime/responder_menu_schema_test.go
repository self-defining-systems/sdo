package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/reflect/protoreflect"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
)

func TestMenuHelpExportsNestedShapes(t *testing.T) {
	loop, _ := menuB3Loop(t)
	help := menuCall(t, loop, map[string]any{"command": "menu"})
	encoded, _ := json.Marshal(help)
	var document map[string]any
	_ = json.Unmarshal(encoded, &document)
	if _, ok := document["selection_set_schema"].(map[string]any); !ok {
		t.Fatal("selection set input schema absent")
	}
	options := document["menu"].(map[string]any)["options"].([]any)
	for _, raw := range options {
		option := raw.(map[string]any)
		for _, p := range option["parameters"].([]any) {
			param := p.(map[string]any)
			schema, ok := param["schema"].(map[string]any)
			if !ok {
				t.Fatalf("%s.%s lacks recursive schema", option["option_id"], param["name"])
			}
			if option["option_id"] == "EnsureDatabaseUser" && param["name"] == "target" {
				auth := schema["properties"].(map[string]any)["auth"].(map[string]any)
				username := auth["properties"].(map[string]any)["username"].(map[string]any)
				cm := username["properties"].(map[string]any)["config_map"].(map[string]any)
				if cm["properties"].(map[string]any)["assignment"] == nil {
					t.Fatal("credential assignment shape missing")
				}
			}
		}
	}
}

func TestMenuStructuredParametersThroughTransport(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := multiCall(t, loop, `[{"option_id":"SetField","parameters":{"ref":{"api_version":"v1","kind":"Service","namespace":"app","name":"backend"},"path":"spec.ports[0].targetPort","value":8082,"reason":"restore route"}}]`)
	if response.Report == nil || !response.TargetedHealthy {
		t.Fatalf("structured selection failed: %+v", response)
	}
	value, _ := testField(cluster.objects[menuService], "spec.ports.0.targetPort")
	if value != float64(8082) {
		t.Fatalf("write missing: %v", value)
	}
	var direct ToolRequest
	if err := json.Unmarshal([]byte(`{"command":"menu","parameters":{"replicas":2,"ref":{"kind":"Deployment","namespace":"app","name":"frontend"}}}`), &direct); err != nil {
		t.Fatal(err)
	}
	if direct.Parameters["replicas"] != "2" {
		t.Fatal("structured scalar lost")
	}
}

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

// The fixture reads only the exported schemas to construct every published
// action parameter, then crosses the JSON tool boundary and the real plan codec.
// It performs no destructive effects or Kubernetes reads.
func TestMenuHelpAloneInvokesEveryMessageParameter(t *testing.T) {
	loop, _ := menuB3Loop(t)
	help := menuCall(t, loop, map[string]any{"command": "menu"})
	encoded, _ := json.Marshal(help)
	var document struct {
		Menu struct {
			Options []struct {
				OptionID   string `json:"option_id"`
				ProgramID  string `json:"program_id"`
				Parameters []struct {
					Name    string      `json:"name"`
					Schema  *MenuSchema `json:"schema"`
					Example any         `json:"example"`
				} `json:"parameters"`
			} `json:"options"`
		} `json:"menu"`
	}
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, option := range document.Menu.Options {
		if !strings.HasPrefix(option.ProgramID, "action:") {
			continue
		}
		t.Run(option.OptionID, func(t *testing.T) {
			parameters := map[string]any{}
			for _, parameter := range option.Parameters {
				if parameter.Example != nil {
					parameters[parameter.Name] = parameter.Example
				} else if parameter.Schema != nil {
					continue
				}
			}
			raw, err := json.Marshal(map[string]any{"command": "mitigate", "selection_set": map[string]any{"version": 1, "options": []any{map[string]any{"option_id": option.OptionID, "parameters": parameters}}}})
			if err != nil {
				t.Fatal(err)
			}
			var request ToolRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Fatalf("exported help cannot construct request: %v", err)
			}
			action := rawMenuActions()[option.OptionID].action
			plan, err := menuRawPlan(action, request.SelectionSet.Options[0].Parameters, &program.ValueInputs{})
			if err != nil {
				t.Fatalf("exported schema cannot invoke action: %v; inputs: %s", err, raw)
			}
			if len(plan.Actions) != 1 {
				t.Fatal("action was lost")
			}
			for _, parameter := range option.Parameters {
				f := action.Message().Fields().ByName(protoreflect.Name(parameter.Name))
				if f.Message() != nil && parameter.Example == nil {
					t.Errorf("message-valued parameter %s has no callable example", parameter.Name)
				}
			}
			count++
		})
	}
	if count != len(rawMenuActions()) {
		t.Fatalf("only %d published actions exercised", count)
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

func TestMenuMalformedStructuredParameterRefusesBatchBeforeWrites(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	result := multiCall(t, loop, `[{"option_id":"SetField","parameters":{"ref":"Service/app/backend","path":"spec.ports[0].targetPort","value":8082,"reason":"restore"}},{"option_id":"EnsureDatabaseUser","parameters":{"target":{"engine":"mongodb","pod":[],"container":"app","database":"app"},"user":"app","password":{"env":"PRIVATE_CANARY"}}}]`)
	raw, _ := json.Marshal(result)
	if !strings.Contains(string(raw), "target.pod: expected object") || strings.Contains(string(raw), "PRIVATE_CANARY") || len(cluster.patches) != 0 || loop.LoopsUsed() != 0 {
		t.Fatalf("bad batch wrote or lost safe field error: %s", raw)
	}
}
