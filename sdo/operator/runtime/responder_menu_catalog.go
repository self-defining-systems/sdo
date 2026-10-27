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
	Example  any         `json:"example,omitempty"`
	Name     string      `json:"name"`
	Kind     string      `json:"kind"`
	Required bool        `json:"required"`
	Repeated bool        `json:"repeated,omitempty"`
	Default  any         `json:"default,omitempty"`
	Schema   *MenuSchema `json:"schema"`
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
	examples := menuActionExamples()[string(action.Message().Name())]
	params := make([]MenuParameter, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		kind := f.Kind().String()
		if f.Message() != nil {
			kind = string(f.Message().Name())
		}
		if f.IsMap() {
			kind = "map[" + f.MapKey().Kind().String() + "]"
			if f.MapValue().Message() != nil {
				kind += string(f.MapValue().Message().Name())
			} else {
				kind += f.MapValue().Kind().String()
			}
		}
		rules, _ := proto.GetExtension(f.Options(), validate.E_Field).(*validate.FieldRules)
		required := rules.GetRequired() || rules.GetString().GetMinLen() > 0 || rules.GetRepeated().GetMinItems() > 0
		schema := menuInputSchema(f)
		if action.Message().Name() == "SyncWebhookCA" && f.Name() == "secret_key" {
			schema.Description = "Sync a named Secret key into one webhook caBundle privately; receipts expose only the reference and digest."
		}
		if action.Message().Name() == "EnsureDatabaseUser" && f.Name() == "target" {
			schema.Description = "Database Pod/container and catalog; target.auth authenticates catalog reads separately from the create-user password. Failures name the safe execution stage; correct source-declared references before retrying."
		}
		params = append(params, MenuParameter{Name: string(f.Name()), Kind: kind, Required: required, Repeated: f.IsList(), Schema: schema, Example: examples[string(f.Name())]})
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
		schema := &MenuSchema{Type: "string"}
		if p.ValueKind == "" {
			schema = menuReferenceSchema()
		}
		if p.ValueKind == program.IntValue {
			schema.Type = "integer"
		}
		if p.ValueKind == program.BoolValue {
			schema.Type = "boolean"
		}
		params = append(params, MenuParameter{Name: p.Name, Kind: kind, Required: p.Required, Default: p.Default, Schema: schema})
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
			f = field.Message().Fields().ByJSONName(name)
		}
		if f == nil {
			return program.Plan{}, fmt.Errorf("unknown parameter %q", name)
		}
		value, err := menuActionParameter(f, raw, inputs)
		if err != nil {
			return program.Plan{}, fmt.Errorf("parameter %s: %w", name, err)
		}
		if err := menuValidateShape(f, value, name); err != nil {
			return program.Plan{}, err
		}
		values[string(f.Name())] = value
	}
	body, err := json.Marshal(map[string]any{string(field.Name()): values})
	if err != nil {
		return program.Plan{}, err
	}
	envelope := &contracts.Action{}
	if err := protojson.Unmarshal(body, envelope); err != nil {
		return program.Plan{}, fmt.Errorf("invalid action parameters: expected the published field schema")
	}
	if err := wire.Validate(envelope); err != nil {
		return program.Plan{}, err
	}
	action := planActionState(envelope)
	if action == nil {
		if general, err := generalActionState(envelope); err == nil {
			if err := (program.Plan{Actions: []program.Action{general}}).Validate(); err != nil {
				return program.Plan{}, fmt.Errorf("%s parameters: %w", field.Message().Name(), err)
			}
		}
		return program.Plan{}, fmt.Errorf("invalid %s parameters; expected published schema and example", field.Message().Name())
	}
	return program.Plan{Actions: []program.Action{action}}, nil
}

func menuActionParameter(field protoreflect.FieldDescriptor, raw string, inputs *program.ValueInputs) (any, error) {
	// JSON text can arrive inside a string parameter. Check it before decoding
	// into maps, which would silently discard duplicate nested target fields.
	if (field.Message() != nil || field.IsMap() || field.IsList()) && json.Valid([]byte(raw)) {
		if err := menuRejectDuplicateFields([]byte(raw)); err != nil {
			return nil, err
		}
	}
	if field.IsMap() && field.MapValue().Message() != nil && field.MapValue().Message().Name() == "PlanValue" {
		var env map[string]any
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			return nil, fmt.Errorf("expected a JSON map of Values")
		}
		for name, value := range env {
			converted, err := menuRawValue(value, string(field.Name())+"_"+name, inputs)
			if err != nil {
				return nil, err
			}
			env[name] = converted
		}
		return env, nil
	}
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
	if field.Message() != nil && field.Message().FullName() == "google.protobuf.Duration" {
		var value string
		if json.Unmarshal([]byte(raw), &value) == nil {
			return value, nil
		}
		return raw, nil
	}
	if field.Kind() == protoreflect.BytesKind && !field.IsList() {
		return base64.StdEncoding.EncodeToString([]byte(raw)), nil
	}
	if field.Kind() == protoreflect.StringKind && !field.IsList() {
		return raw, nil
	}
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, fmt.Errorf("expected %s JSON (%s)", menuInputSchema(field).Type, field.Kind())
	}
	return menuNormalizeReferences(field, value, string(field.Name()))
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
			if candidate, exists := descriptor[origin]; exists {
				field, ok := candidate.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("%s.%s: expected object", name, origin)
				}
				ref, err := menuParseReference(field["ref"])
				if err != nil {
					return nil, fmt.Errorf("%s.%s.ref: %w", name, origin, err)
				}
				field["ref"] = ref
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

// Recursively normalize every descriptor-owned ObjectRef, including refs nested
// in database targets, jobs, commands and source/observed value descriptors.
func menuNormalizeReferences(field protoreflect.FieldDescriptor, value any, path string) (any, error) {
	if field.IsMap() {
		if object, ok := value.(map[string]any); ok {
			for key, item := range object {
				converted, err := menuNormalizeReferenceValue(field.MapValue(), item, path+"."+key)
				if err != nil {
					return nil, err
				}
				object[key] = converted
			}
		}
		return value, nil
	}
	if field.IsList() {
		if items, ok := value.([]any); ok {
			for i, item := range items {
				converted, err := menuNormalizeReferenceValue(field, item, fmt.Sprintf("%s[%d]", path, i))
				if err != nil {
					return nil, err
				}
				items[i] = converted
			}
		}
		return value, nil
	}
	return menuNormalizeReferenceValue(field, value, path)
}

func menuNormalizeReferenceValue(field protoreflect.FieldDescriptor, value any, path string) (any, error) {
	if field.Message() == nil {
		return value, nil
	}
	if field.Message().Name() == "ObjectRef" {
		converted, err := menuParseReference(value)
		if err != nil {
			return nil, fmt.Errorf("%s: expected object reference: %w", path, err)
		}
		return converted, nil
	}
	if object, ok := value.(map[string]any); ok {
		for name, item := range object {
			nested := field.Message().Fields().ByName(protoreflect.Name(name))
			if nested == nil {
				nested = field.Message().Fields().ByJSONName(name)
			}
			if nested == nil {
				continue
			}
			converted, err := menuNormalizeReferences(nested, item, path+"."+name)
			if err != nil {
				return nil, err
			}
			object[name] = converted
		}
	}
	return value, nil
}

func menuParseReference(value any) (map[string]any, error) {
	raw, ok := value.(string)
	if !ok {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		raw = string(encoded)
	}
	ref, err := sdk.ParseObjectRef(raw)
	if err != nil {
		return nil, err
	}
	return map[string]any{"api_version": ref.APIVersion, "kind": ref.Kind, "namespace": ref.Namespace, "name": ref.Name}, nil
}
