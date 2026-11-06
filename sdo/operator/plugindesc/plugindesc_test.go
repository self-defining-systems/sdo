package plugindesc_test

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/plugindesc"
)

func schemaOf(t *testing.T, s string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		t.Fatalf("schema is not JSON: %v\n%s", err, s)
	}
	return out
}

func TestSchemaNamesProtojsonFieldsAndTypes(t *testing.T) {
	d, err := plugindesc.Describe("helm", (&contracts.HelmRenderConfig{}).ProtoReflect().Descriptor(), "  Renders a chart.\n")
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != "helm" || d.ConfigType != "sdodev.contracts.v1alpha1.HelmRenderConfig" || d.Doc != "Renders a chart." {
		t.Fatalf("descriptor = %+v", d)
	}
	root := schemaOf(t, d.ConfigSchemaJSON)
	defs := root["$defs"].(map[string]any)
	cfg := defs[d.ConfigType].(map[string]any)
	props := cfg["properties"].(map[string]any)
	for _, name := range []string{"chartPath", "valueFiles", "values"} {
		if _, ok := props[name]; !ok {
			t.Errorf("schema lacks property %q: %v", name, props)
		}
	}
	if got := props["valueFiles"].(map[string]any)["type"]; got != "array" {
		t.Errorf("valueFiles type = %v", got)
	}
	if got := props["values"].(map[string]any)["type"]; got != "object" {
		t.Errorf("values (Struct) type = %v", got)
	}
}

func TestSchemaIsDeterministic(t *testing.T) {
	md := (&contracts.BuildKitImageConfig{}).ProtoReflect().Descriptor()
	a, err := plugindesc.Schema(md)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		b, _ := plugindesc.Schema(md)
		if a != b {
			t.Fatal("schema differs between runs")
		}
	}
}

func TestSchemaHandlesWellKnownAndNestedTypes(t *testing.T) {
	s, err := plugindesc.Schema((&contracts.GitPollConfig{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s, "pollInterval") || !strings.Contains(s, "30s") {
		t.Errorf("Duration field missing or undescribed: %s", s)
	}
	if _, err := plugindesc.Schema((&structpb.Struct{}).ProtoReflect().Descriptor()); err != nil {
		t.Errorf("well-known root: %v", err)
	}
}
