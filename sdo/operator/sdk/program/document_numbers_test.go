package program_test

import (
	"encoding/json"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func TestConfigMapJSONDocumentPreservesUnrelatedNumbers(t *testing.T) {
	object := map[string]any{"data": map[string]any{"flags": `{"gc":true,"id":12345678901234567,"big":1e21,"nested":{"negative":-9007199254740993,"fraction":0.1234567890123456789}}`}}
	action := program.SetField{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}, Path: "data[flags]", DocumentFormat: "json", DocumentPath: []string{"gc"}, Value: testPlanValue(false)}
	if err := program.ApplySetFieldAction(object, action, testResolvedValue(t, action.Value)); err != nil {
		t.Fatal(err)
	}
	got := object["data"].(map[string]any)["flags"].(string)
	for _, want := range []string{"12345678901234567", "1e21", "-9007199254740993", "0.1234567890123456789", `"gc":false`} {
		if !strings.Contains(got, want) {
			t.Fatalf("JSON number or owned entry changed: missing %s", want)
		}
	}
}
func TestConfigMapJSONDocumentAcceptedSourceNumberPrecision(t *testing.T) {
	for _, value := range []any{json.Number("12345678901234567"), int64(12345678901234567)} {
		object := map[string]any{"data": map[string]any{"flags": `{"id":1,"other":1e21}`}}
		action := program.SetField{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}, Path: "data[flags]", DocumentFormat: "json", DocumentPath: []string{"id"}, Value: testPlanValue(value)}
		if err := program.ApplySetFieldAction(object, action, testResolvedValue(t, action.Value)); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(object["data"].(map[string]any)["flags"].(string), "12345678901234567") {
			t.Fatal("literal numeric value rounded")
		}
		action.Value = program.FromSource(action.Ref, action.Path)
		source := map[string]any{"data": map[string]any{"flags": `{"id":9007199254740993}`}}
		resolved, _, err := program.ResolveSetFieldValue(action, program.ValueInputs{SourceCommit: "accepted", Source: func(sdk.ObjectRef) (map[string]any, error) { return source, nil }})
		if err != nil {
			t.Fatal(err)
		}
		if err := program.ApplySetFieldAction(object, action, resolved); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(object["data"].(map[string]any)["flags"].(string), "9007199254740993") {
			t.Fatal("accepted numeric value rounded")
		}
	}
}
