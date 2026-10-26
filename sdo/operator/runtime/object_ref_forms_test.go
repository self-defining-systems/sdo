package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Hand-written role fixtures exercise the public request decoder and real menu
// plan codec; no API, cluster or model is needed to detect a reference mismatch.
func TestResponderObjectReferenceForms(t *testing.T) {
	for _, raw := range []string{`Service/app/backend`, `svc/app/backend`, `{"api_version":"v1","kind":"Service","namespace":"app","name":"backend"}`, `{"kind":"svc","namespace":"app","name":"backend"}`} {
		t.Run(raw, func(t *testing.T) {
			loop, _ := menuB3Loop(t)
			loop.Request.RepairPolicy = "recorded-actions"
			loop.Executor.Verifier = planFakeVerifier{true}
			menuCall(t, loop, map[string]any{"command": "menu"})
			response := menuCall(t, loop, map[string]any{"command": "mitigate", "selection": map[string]any{"version": 1, "option_id": "SetField", "round_limit": 1}, "parameters": map[string]any{"ref": raw, "path": "spec.ports[0].targetPort", "value": 8082, "reason": "restore route"}})
			if response["error"] != nil || response["report"] == nil || response["targeted_healthy"] != true {
				t.Fatalf("reference did not reach normal execution: %+v", response)
			}
		})
	}
}

func TestLearnedProgramParametersAcceptPrintedBindingObject(t *testing.T) {
	entry := menuEntry{spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "workload", Kind: "Deployment", Required: true}}}}
	want := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}
	for _, raw := range []string{`Deployment/app/worker`, `{"api_version":"apps/v1","kind":"Deployment","namespace":"app","name":"worker"}`, `{"kind":"deploy","namespace":"app","name":"worker"}`} {
		got, err := menuBindProgram(entry, map[string]string{"workload": raw})
		if err != nil || got.Objects["workload"] != want {
			t.Fatalf("printed binding %q: %+v %v", raw, got, err)
		}
	}
}

func TestNestedMenuObjectReferenceForms(t *testing.T) {
	for _, raw := range []string{`"Pod/app/worker"`, `{"kind":"po","namespace":"app","name":"worker"}`} {
		field := rawMenuActions()["EnsureDatabaseUser"].action.Message().Fields().ByName("target")
		got, err := menuActionParameter(field, `{"engine":"mongodb","pod":`+raw+`,"container":"db","database":"app"}`, &program.ValueInputs{})
		if err != nil {
			t.Fatal(err)
		}
		target := got.(map[string]any)
		body, _ := json.Marshal(target["pod"])
		var ref sdk.ObjectRef
		if err := json.Unmarshal(body, &ref); err != nil || ref.Kind != "Pod" || ref.APIVersion != "v1" {
			t.Fatalf("nested ref was not parsed: %s %v", body, err)
		}
	}
}

func TestMenuRejectsAmbiguousObjectBeforeWrites(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	menuCall(t, loop, map[string]any{"command": "menu"})
	before, _ := json.Marshal(cluster.objects[menuService])
	response := menuCall(t, loop, map[string]any{"command": "mitigate", "selection": map[string]any{"version": 1, "option_id": "SetField", "round_limit": 1}, "parameters": map[string]any{"ref": `{"kind":"Service","namespace":"app","name":"backend","names":["peer"]}`, "path": "spec.ports[0].targetPort", "value": 8082}})
	after, _ := json.Marshal(cluster.objects[menuService])
	encoded, _ := json.Marshal(response)
	if !strings.Contains(string(encoded), "Kind/name") || !strings.Contains(string(encoded), "Service/app/backend") || string(before) != string(after) {
		t.Fatalf("ambiguous target must fail before writes: %+v", response)
	}
}

func TestToolObjectReferenceFormsShareParser(t *testing.T) {
	for _, raw := range []string{`"deploy/app/worker"`, `{"kind":"deploy","namespace":"app","name":"worker"}`} {
		for _, document := range []string{
			`{"command":"source","ref":` + raw + `}`,
			`{"command":"pre-apply","refs":[` + raw + `]}`,
			`{"command":"diagnose","bindings":{"workload":` + raw + `}}`,
			`{"command":"plan","bindings":{"objects":{"workload":` + raw + `}}}`,
			`{"command":"menu","selection":{"version":1,"option_id":"repair","bindings":[{"name":"workload","object":` + raw + `}]}}`,
		} {
			var request ToolRequest
			if err := json.Unmarshal([]byte(document), &request); err != nil {
				t.Fatalf("%s: %v", document, err)
			}
			var ref sdk.ObjectRef
			switch {
			case request.Ref != nil:
				ref = *request.Ref
			case len(request.Refs) > 0:
				ref = request.Refs[0]
			case request.Selection != nil:
				object := request.Selection.Bindings[0].GetObject()
				ref = sdk.ObjectRef{APIVersion: object.ApiVersion, Kind: object.Kind, Namespace: object.Namespace, Name: object.Name}
			default:
				ref = request.Bindings.Objects["workload"]
			}
			if ref != (sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}) {
				t.Fatalf("reference bypassed shared parser: %s: %+v", document, ref)
			}
		}
	}
}

func TestToolRejectsDuplicateReferenceComponents(t *testing.T) {
	for _, raw := range []string{
		`{"command":"mitigate","parameters":{"ref":{"kind":"Service","name":"backend","name":"peer"}}}`,
		`{"command":"mitigate","parameters":{"target":{"pod":{"kind":"Pod","name":"worker","name":"peer"}}}}`,
		`{"command":"source","ref":{"kind":"Pod","name":"worker","name":"peer"}}`,
	} {
		var request ToolRequest
		if err := json.Unmarshal([]byte(raw), &request); err == nil {
			t.Fatalf("ambiguous reference crossed tool boundary: %s", raw)
		}
	}
}

func TestNestedJSONTextRejectsDuplicateObjectComponents(t *testing.T) {
	for _, tc := range []struct{ action, field, raw string }{
		{"EnsureDatabaseUser", "target", `{"engine":"mongodb","pod":{"kind":"Pod","namespace":"app","name":"worker","name":"peer"},"container":"db","database":"app"}`},
		{"SetField", "value", `{"source":{"ref":{"kind":"Service","namespace":"app","name":"backend","name":"peer"},"path":"spec.ports[0].targetPort"}}`},
	} {
		field := rawMenuActions()[tc.action].action.Message().Fields().ByName(protoreflect.Name(tc.field))
		_, err := menuActionParameter(field, tc.raw, &program.ValueInputs{})
		if err == nil || !strings.Contains(err.Error(), "duplicate field") || !strings.Contains(err.Error(), "Kind/name") {
			t.Fatalf("%s.%s erased an ambiguous nested reference: %v", tc.action, tc.field, err)
		}
	}
}
