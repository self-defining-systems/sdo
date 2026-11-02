package program_test

import (
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestNamedEnvInsertionPreservesUnrelatedEntries(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "api"}
	path := "spec.template.spec.containers[name=api].env[name=DEPENDENCY_ADDR]"
	action := program.SetField{Ref: ref, Path: path, Value: program.FromSource(ref, path)}
	entry := map[string]any{"name": "DEPENDENCY_ADDR", "value": "backend:8080"}
	for _, initial := range []any{nil, []any{}, []any{map[string]any{"name": "EXISTING", "value": "keep"}}} {
		container := map[string]any{"name": "api"}
		if initial != nil {
			container["env"] = initial
		}
		object := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{container}}}}}
		if err := program.ApplySetFieldAction(object, action, entry); err != nil {
			t.Fatal(err)
		}
		value, err := program.FieldValue(object, path)
		if err != nil || !reflect.DeepEqual(value, entry) {
			t.Fatalf("inserted value %v: %v", value, err)
		}
		items := container["env"].([]any)
		if len(items) > 1 && items[0].(map[string]any)["value"] != "keep" {
			t.Fatal("unrelated entry changed")
		}
		if err := program.ApplySetFieldAction(object, action, entry); err != nil {
			t.Fatal(err)
		}
		if len(container["env"].([]any)) != len(items) {
			t.Fatal("idempotent update duplicated named env")
		}
	}
}

func TestNamedEnvInsertionRefusesAmbiguousOrMismatchedIdentity(t *testing.T) {
	path := "spec.template.spec.containers[name=api].env[name=DEPENDENCY_ADDR]"
	for _, tc := range []struct {
		env   []any
		value map[string]any
	}{
		{[]any{}, map[string]any{"name": "OTHER", "value": "backend:8080"}},
		{[]any{map[string]any{"name": "DEPENDENCY_ADDR"}, map[string]any{"name": "DEPENDENCY_ADDR"}}, map[string]any{"name": "DEPENDENCY_ADDR", "value": "backend:8080"}},
	} {
		object := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "api", "env": tc.env}}}}}}
		if err := program.ApplySetField(object, path, tc.value); err == nil {
			t.Fatal("invalid list identity accepted")
		}
	}
	object := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{}}}}}
	if err := program.ApplySetField(object, path, map[string]any{"name": "DEPENDENCY_ADDR", "value": "backend:8080"}); err == nil {
		t.Fatal("missing container was invented")
	}
}
