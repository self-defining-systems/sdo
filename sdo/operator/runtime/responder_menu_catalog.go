package runtime

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// MenuParameter describes input, never authority. Raw actions derive their
// fields from the same generated registry used by plan serialization.
type MenuParameter struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	Repeated bool   `json:"repeated,omitempty"`
	Default  any    `json:"default,omitempty"`
}

func rawMenuActions() map[string]menuEntry {
	entries := map[string]menuEntry{}
	fields := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		entries[string(field.Message().Name())] = menuEntry{programID: "action:" + string(field.Message().Name()), action: field}
	}
	return entries
}

func menuActionParameters(action protoreflect.FieldDescriptor) []MenuParameter {
	fields := action.Message().Fields()
	params := make([]MenuParameter, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		kind := f.Kind().String()
		if f.Message() != nil {
			kind = string(f.Message().Name())
		}
		rules, _ := proto.GetExtension(f.Options(), validate.E_Field).(*validate.FieldRules)
		required := rules.GetRequired() || rules.GetString().GetMinLen() > 0 || rules.GetRepeated().GetMinItems() > 0
		params = append(params, MenuParameter{Name: string(f.Name()), Kind: kind, Required: required, Repeated: f.IsList()})
	}
	return params
}

func menuProgramParameters(spec program.ProgramSpec) []MenuParameter {
	params := make([]MenuParameter, 0, len(spec.Params))
	for _, p := range spec.Params {
		kind := string(p.Kind)
		if p.ValueKind != "" {
			kind = string(p.ValueKind)
		}
		params = append(params, MenuParameter{Name: p.Name, Kind: kind, Required: p.Required, Default: p.Default})
	}
	return params
}

// Decode through the generated Action registry and ordinary plan adapter. No
// menu-specific action switch or alternate execution engine is needed.
func menuRawPlan(field protoreflect.FieldDescriptor, parameters map[string]string, inputs *program.ValueInputs) (program.Plan, error) {
	values := map[string]any{}
	for name, raw := range parameters {
		f := field.Message().Fields().ByName(protoreflect.Name(name))
		if f == nil {
			return program.Plan{}, fmt.Errorf("unknown parameter %q", name)
		}
		value, err := menuActionParameter(f, raw, inputs)
		if err != nil {
			return program.Plan{}, fmt.Errorf("parameter %s: %w", name, err)
		}
		values[name] = value
	}
	body, err := json.Marshal(map[string]any{string(field.Name()): values})
	if err != nil {
		return program.Plan{}, err
	}
	envelope := &contracts.Action{}
	if err := protojson.Unmarshal(body, envelope); err != nil {
		return program.Plan{}, fmt.Errorf("invalid action parameters: %w", err)
	}
	if err := wire.Validate(envelope); err != nil {
		return program.Plan{}, err
	}
	action := planActionState(envelope)
	if action == nil {
		return program.Plan{}, fmt.Errorf("registered action has no executor adapter")
	}
	return program.Plan{Actions: []program.Action{action}}, nil
}

func menuActionParameter(field protoreflect.FieldDescriptor, raw string, inputs *program.ValueInputs) (any, error) {
	if field.Message() != nil && field.Message().Name() == "ObjectRef" {
		ref, err := sdk.ParseObjectRef(raw)
		if err != nil {
			return nil, err
		}
		return map[string]any{"api_version": ref.APIVersion, "kind": ref.Kind, "namespace": ref.Namespace, "name": ref.Name}, nil
	}
	if field.Message() != nil && field.Message().Name() == "PlanValue" {
		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			value = raw
		}
		if field.IsList() {
			items, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("expected a JSON list of values")
			}
			converted := make([]any, 0, len(items))
			for i, item := range items {
				v, err := menuRawValue(item, fmt.Sprintf("%s_%d", field.Name(), i), inputs)
				if err != nil {
					return nil, err
				}
				converted = append(converted, v)
			}
			return converted, nil
		}
		return menuRawValue(value, string(field.Name()), inputs)
	}
	if field.Kind() == protoreflect.BytesKind && !field.IsList() {
		return base64.StdEncoding.EncodeToString([]byte(raw)), nil
	}
	if field.Kind() == protoreflect.StringKind && !field.IsList() {
		return raw, nil
	}
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, fmt.Errorf("expected %s JSON", field.Kind())
	}
	return value, nil
}

func menuScalarParameter(spec program.ParamSpec, raw string) (any, error) {
	var value any = raw
	var err error
	switch spec.ValueKind {
	case program.IntValue:
		value, err = strconv.ParseInt(raw, 10, 64)
	case program.BoolValue:
		value, err = strconv.ParseBool(raw)
	}
	if err != nil {
		return nil, fmt.Errorf("parameter %s requires %s", spec.Name, spec.ValueKind)
	}
	if err := program.ValidateValueBinding(spec, value); err != nil {
		return nil, err
	}
	return value, nil
}

func menuBindProgram(entry menuEntry, parameters map[string]string) (program.Bindings, error) {
	bindings := entry.bindings.Clone()
	if entry.action != nil {
		return bindings, nil
	}
	if bindings.Objects == nil {
		bindings.Objects = program.ObjectBindings{}
	}
	if bindings.Values == nil {
		bindings.Values = map[string]any{}
	}
	declared := map[string]program.ParamSpec{}
	for _, spec := range entry.spec.Params {
		declared[spec.Name] = spec
	}
	for name, raw := range parameters {
		spec, ok := declared[name]
		if !ok {
			return bindings, fmt.Errorf("unknown parameter %q", name)
		}
		if spec.ValueKind != "" {
			value, err := menuScalarParameter(spec, raw)
			if err != nil {
				return bindings, err
			}
			bindings.Values[name] = value
		} else {
			ref, err := sdk.ParseObjectRef(raw)
			if err != nil {
				return bindings, err
			}
			bindings.Objects[name] = ref
		}
	}
	return bindings, nil
}

// Raw scalar inputs become declared parameters; the action still contains only
// an opaque FromParam reference. Source/observed inputs retain their authority.
func menuRawValue(value any, name string, inputs *program.ValueInputs) (any, error) {
	if descriptor, ok := value.(map[string]any); ok {
		for _, origin := range []string{"source", "observed"} {
			if field, ok := descriptor[origin].(map[string]any); ok {
				if text, ok := field["ref"].(string); ok {
					ref, err := sdk.ParseObjectRef(text)
					if err != nil {
						return nil, err
					}
					field["ref"] = map[string]any{"api_version": ref.APIVersion, "kind": ref.Kind, "namespace": ref.Namespace, "name": ref.Name}
				}
				return descriptor, nil
			}
		}
		return nil, fmt.Errorf("value requires a scalar or source/observed field descriptor")
	}
	spec := program.ParamSpec{Name: name, Required: true}
	switch value.(type) {
	case string:
		spec.ValueKind = program.StringValue
	case bool:
		spec.ValueKind = program.BoolValue
	case float64:
		spec.ValueKind = program.IntValue
	default:
		return nil, fmt.Errorf("value requires a string, integer or boolean parameter")
	}
	if err := program.ValidateValueBinding(spec, value); err != nil {
		return nil, err
	}
	inputs.Spec.Params = append(inputs.Spec.Params, spec)
	if inputs.Bindings.Values == nil {
		inputs.Bindings.Values = map[string]any{}
	}
	inputs.Bindings.Values[name] = value
	return map[string]any{"parameter": name}, nil
}
