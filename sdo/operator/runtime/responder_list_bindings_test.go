package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"sdo.dev/operator/sdk/program"
)

// bindAction binds command-line values the way `sdo mitigate NAME --bind ...`
// does and decodes them through the ordinary plan codec.
func bindAction(t *testing.T, name string, parameters map[string]BindValues) (*program.Plan, error) {
	t.Helper()
	entry := rawMenuActions()[name]
	texts, err := actionBindings(entry.action, parameters)
	if err != nil {
		return nil, err
	}
	plan, err := menuRawPlan(entry.action, texts, &program.ValueInputs{})
	return &plan, err
}

// exampleText is one value written the way a responder writes it after --bind.
func exampleText(t *testing.T, value any) string {
	t.Helper()
	if text, ok := value.(string); ok {
		return text
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// syntheticElement stands in for a list element a menu example does not show.
func syntheticElement(t *testing.T, action string, field protoreflect.FieldDescriptor) any {
	t.Helper()
	switch {
	case field.Message() != nil && field.Message().Name() == "ObjectRef":
		return "Service/app/example"
	case field.Message() == nil && field.Kind() == protoreflect.StringKind:
		return "example"
	}
	t.Fatalf("%s.%s: list parameter needs a menu example", action, field.Name())
	return nil
}

func listLength(t *testing.T, plan *program.Plan, field protoreflect.FieldDescriptor) int {
	t.Helper()
	envelope, err := planActionProto(plan.Actions[0])
	if err != nil {
		t.Fatal(err)
	}
	reflected := envelope.ProtoReflect()
	body := reflected.Get(reflected.WhichOneof(reflected.Descriptor().Oneofs().Get(0))).Message()
	return body.Get(field).List().Len()
}

// Every list-typed action parameter, found from the contract, takes repeated
// --bind values, a JSON array, or one element.
func TestEveryListParameterBindsRepeatedValuesJSONArrayOrOneElement(t *testing.T) {
	examples := menuActionExamples()
	lists := 0
	for name, entry := range rawMenuActions() {
		fields := entry.action.Message().Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			if !field.IsList() {
				continue
			}
			lists++
			example, _ := examples[name][string(field.Name())].([]any)
			if len(example) == 0 {
				example = []any{syntheticElement(t, name, field)}
			}
			base := map[string]BindValues{}
			for other, value := range examples[name] {
				if other != string(field.Name()) {
					base[other] = BindValues{exampleText(t, value)}
				}
			}
			for j := 0; examples[name] == nil && j < fields.Len(); j++ {
				if other := fields.Get(j); other.Kind() == protoreflect.StringKind && !other.IsList() {
					base[string(other.Name())] = BindValues{"restore intended behavior"}
				}
			}
			element := exampleText(t, example[0])
			array, _ := json.Marshal([]any{example[0], example[0]})
			for form, values := range map[string]BindValues{
				"repeated --bind": {element, element},
				"JSON array":      {string(array)},
				"one element":     {element},
			} {
				parameters := map[string]BindValues{string(field.Name()): values}
				for other, value := range base {
					parameters[other] = value
				}
				plan, err := bindAction(t, name, parameters)
				if err != nil {
					t.Errorf("%s.%s as %s: %v", name, field.Name(), form, err)
					continue
				}
				want := 2
				if form == "one element" {
					want = 1
				}
				if got := listLength(t, plan, field); got != want {
					t.Errorf("%s.%s as %s: %d elements, want %d", name, field.Name(), form, got, want)
				}
			}
		}
	}
	if lists == 0 {
		t.Fatal("no list parameters found in the action contract")
	}
}

func TestMultiArgumentExecCommandBindsAsRepeatedValues(t *testing.T) {
	plan, err := bindAction(t, "Exec", map[string]BindValues{
		"pod":       {"Pod/app/db-0"},
		"container": {"db"},
		"command":   {"sh", "-c", `psql -c "select 1"`},
	})
	if err != nil {
		t.Fatal(err)
	}
	exec, ok := plan.Actions[0].(program.Exec)
	if !ok {
		if pointer, isPointer := plan.Actions[0].(*program.Exec); isPointer {
			exec, ok = *pointer, true
		}
	}
	if !ok || len(exec.Command) != 3 {
		t.Fatalf("Exec command = %+v", plan.Actions[0])
	}
}

func TestOnlyListParametersTakeRepeatedValues(t *testing.T) {
	_, err := bindAction(t, "Exec", map[string]BindValues{"pod": {"Pod/app/a", "Pod/app/b"}, "container": {"db"}, "command": {"true"}})
	if err == nil || !strings.Contains(err.Error(), "parameter pod takes one value") {
		t.Fatalf("repeated singular parameter: %v", err)
	}
}

func TestMitigateTargetDecodesOneOrRepeatedBindValues(t *testing.T) {
	var target MitigateTarget
	if err := json.Unmarshal([]byte(`{"name":"Exec","parameters":{"pod":"Pod/app/a","command":["sh","-c","true"]}}`), &target); err != nil {
		t.Fatal(err)
	}
	if len(target.Parameters["pod"]) != 1 || len(target.Parameters["command"]) != 3 {
		t.Fatalf("parameters = %+v", target.Parameters)
	}
	if err := json.Unmarshal([]byte(`{"name":"Exec","parameters":{"command":[]}}`), &target); err == nil {
		t.Fatal("an empty value list was accepted")
	}
}
