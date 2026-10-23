package runtime

import (
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
		entries[string(field.Message().Name())] = menuEntry{action: field}
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
func menuRawPlan(field protoreflect.FieldDescriptor, parameters map[string]string) (program.Plan, error) {
	values := map[string]any{}
	for name, raw := range parameters {
		f := field.Message().Fields().ByName(protoreflect.Name(name))
		if f == nil {
			return program.Plan{}, fmt.Errorf("unknown parameter %q", name)
		}
		value, err := menuActionParameter(f, raw)
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

func menuActionParameter(field protoreflect.FieldDescriptor, raw string) (any, error) {
	if field.Message() != nil && field.Message().Name() == "ObjectRef" {
		ref, err := sdk.ParseObjectRef(raw)
		if err != nil {
			return nil, err
		}
		return map[string]any{"api_version": ref.APIVersion, "kind": ref.Kind, "namespace": ref.Namespace, "name": ref.Name}, nil
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
