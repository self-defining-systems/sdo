package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

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

