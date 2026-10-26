package runtime

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/dynamicpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"unicode/utf8"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MenuSchema is recursive JSON Schema for the public JSON input, generated from
// the same descriptors as the action codec. It describes inputs, never grants.
type MenuSchema struct {
	Type                 string                 `json:"type,omitempty"`
	Description          string                 `json:"description,omitempty"`
	Properties           map[string]*MenuSchema `json:"properties,omitempty"`
	Required             []string               `json:"required,omitempty"`
	Items                *MenuSchema            `json:"items,omitempty"`
	AdditionalProperties any                    `json:"additionalProperties,omitempty"`
	Enum                 []string               `json:"enum,omitempty"`
	Pattern              string                 `json:"pattern,omitempty"`
	MaxLength            uint64                 `json:"maxLength,omitempty"`
	Const                any                    `json:"const,omitempty"`
	Minimum              any                    `json:"minimum,omitempty"`
	Maximum              any                    `json:"maximum,omitempty"`
	ExclusiveMinimum     any                    `json:"exclusiveMinimum,omitempty"`
	ExclusiveMaximum     any                    `json:"exclusiveMaximum,omitempty"`
	MinLength            uint64                 `json:"minLength,omitempty"`
	MinItems             uint64                 `json:"minItems,omitempty"`
	MaxItems             uint64                 `json:"maxItems,omitempty"`
	OneOf                []*MenuSchema          `json:"oneOf,omitempty"`
	AnyOf                []*MenuSchema          `json:"anyOf,omitempty"`
	AllOf                []*MenuSchema          `json:"allOf,omitempty"`
	Not                  *MenuSchema            `json:"not,omitempty"`
	ContentEncoding      string                 `json:"contentEncoding,omitempty"`
}

func menuFieldRequired(f protoreflect.FieldDescriptor) bool {
	rules, _ := proto.GetExtension(f.Options(), validate.E_Field).(*validate.FieldRules)
	return rules.GetRequired() || rules.GetString().GetMinLen() > 0 || rules.GetRepeated().GetMinItems() > 0
}

func menuMessageSchema(message protoreflect.MessageDescriptor) *MenuSchema {
	switch message.FullName() {
	case "google.protobuf.Value":
		return &MenuSchema{}
	case "google.protobuf.Struct":
		return &MenuSchema{Type: "object", AdditionalProperties: true}
	case "google.protobuf.ListValue":
		return &MenuSchema{Type: "array", Items: &MenuSchema{}}
	case "google.protobuf.Timestamp":
		return &MenuSchema{Type: "string", Description: "RFC3339 timestamp"}
	}
	if message.FullName() == "google.protobuf.Duration" {
		return &MenuSchema{Type: "string", Pattern: `^-?[0-9]+(\.[0-9]{1,9})?s$`, Description: "Duration in seconds, for example 30s or 0.25s."}
	}
	schema := &MenuSchema{Type: "object", Properties: map[string]*MenuSchema{}, AdditionalProperties: false}
	for i := 0; i < message.Fields().Len(); i++ {
		field := message.Fields().Get(i)
		name := string(field.Name())
		schema.Properties[name] = menuFieldSchema(field)
		if menuFieldRequired(field) && field.ContainingOneof() == nil {
			schema.Required = append(schema.Required, name)
		}
	}
	for i := 0; i < message.Oneofs().Len(); i++ {
		group := message.Oneofs().Get(i)
		rules, _ := proto.GetExtension(group.Options(), validate.E_Oneof).(*validate.OneofRules)
		choices := &MenuSchema{}
		absent := &MenuSchema{}
		for j := 0; j < group.Fields().Len(); j++ {
			name := string(group.Fields().Get(j).Name())
			choices.OneOf = append(choices.OneOf, &MenuSchema{Required: []string{name}})
			absent.AnyOf = append(absent.AnyOf, &MenuSchema{Required: []string{name}})
		}
		if !rules.GetRequired() {
			choices.OneOf = append(choices.OneOf, &MenuSchema{Not: absent})
		}
		schema.AllOf = append(schema.AllOf, choices)
	}
	return schema
}

func menuFieldSchema(field protoreflect.FieldDescriptor) *MenuSchema {
	if field.IsMap() {
		return &MenuSchema{Type: "object", AdditionalProperties: menuSingularSchema(field.MapValue())}
	}
	schema := menuSingularSchema(field)
	if field.Message() != nil && field.Message().Name() == "ObjectRef" {
		schema = menuReferenceSchema()
	}
	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	if field.IsList() {
		return &MenuSchema{Type: "array", Items: schema, MinItems: rules.GetRepeated().GetMinItems(), MaxItems: rules.GetRepeated().GetMaxItems()}
	}
	return schema
}

func menuSingularSchema(field protoreflect.FieldDescriptor) *MenuSchema {
	if field.Message() != nil {
		return menuMessageSchema(field.Message())
	}
	schema := &MenuSchema{}
	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	switch field.Kind() {
	case protoreflect.StringKind:
		schema.Type = "string"
		schema.Enum = rules.GetString().GetIn()
		schema.Pattern = rules.GetString().GetPattern()
		schema.MinLength = rules.GetString().GetMinLen()
		schema.MaxLength = rules.GetString().GetMaxLen()
	case protoreflect.BytesKind:
		schema.Type = "string"
		schema.ContentEncoding = "base64"
	case protoreflect.BoolKind:
		schema.Type = "boolean"
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		schema.Type = "number"
	case protoreflect.EnumKind:
		schema.Type = "string"
		for i := 0; i < field.Enum().Values().Len(); i++ {
			schema.Enum = append(schema.Enum, string(field.Enum().Values().Get(i).Name()))
		}
	default:
		schema.Type = "integer"
	}
	menuNumericRules(schema, rules, field.Kind())
	return schema
}

func menuNumericRules(schema *MenuSchema, rules *validate.FieldRules, kind protoreflect.Kind) {
	message := rules.ProtoReflect()
	field := message.Descriptor().Fields().ByName(protoreflect.Name(kind.String()))
	if field == nil || field.Message() == nil || !message.Has(field) {
		return
	}
	numeric := message.Get(field).Message()
	for name, target := range map[string]*any{"const": &schema.Const, "gte": &schema.Minimum, "lte": &schema.Maximum, "gt": &schema.ExclusiveMinimum, "lt": &schema.ExclusiveMaximum} {
		f := numeric.Descriptor().Fields().ByName(protoreflect.Name(name))
		if f != nil && numeric.Has(f) {
			*target = numeric.Get(f).Interface()
		}
	}
}

func menuInputSchema(field protoreflect.FieldDescriptor) *MenuSchema {
	schema := menuFieldSchema(field)
	if field.Message() != nil && field.Message().Name() == "PlanValue" || field.IsMap() && field.MapValue().Message() != nil && field.MapValue().Message().Name() == "PlanValue" {
		value := menuValueSchema()
		if field.IsMap() {
			return &MenuSchema{Type: "object", AdditionalProperties: value}
		}
		if field.IsList() {
			return &MenuSchema{Type: "array", Items: value, MinItems: schema.MinItems}
		}
		return value
	}
	if field.Kind() == protoreflect.BytesKind && !field.IsList() {
		schema.ContentEncoding = ""
		schema.Description = "UTF-8 text; object_json/patch_json/equals_json contain JSON text. Nested bytes fields use base64."
	}
	return schema
}

func menuValueSchema() *MenuSchema {
	field := rawMenuActions()["SetField"].action.Message().Fields().ByName("value")
	descriptor := menuMessageSchema(field.Message())
	delete(descriptor.Properties, "parameter") // raw scalars become controller-bound parameters
	for _, origin := range []string{"source", "observed"} {
		descriptor.Properties[origin].Properties["ref"] = menuReferenceSchema()
	}
	descriptor.AllOf[0].OneOf = descriptor.AllOf[0].OneOf[:2]
	return &MenuSchema{AnyOf: []*MenuSchema{{Type: "string"}, {Type: "integer"}, {Type: "boolean"}, descriptor}, Description: "Literal scalar, or {source: {ref: ObjectRef, path: string}} / {observed: {ref: ObjectRef, path: string}}; ref also accepts Kind/namespace/name."}
}

// Validate only shape here, before ProtoJSON. Never include supplied values in
// diagnostics: nested credential-bearing input must not be echoed on failure.
func menuValidateShape(field protoreflect.FieldDescriptor, value any, path string) error {
	if field.IsMap() {
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object (map)", path)
		}
		for key, item := range object {
			if err := menuValidateSingular(field.MapValue(), item, path+"."+key); err != nil {
				return err
			}
		}
		return nil
	}
	if field.IsList() {
		list, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array", path)
		}
		for i, item := range list {
			if err := menuValidateSingular(field, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	}
	return menuValidateSingular(field, value, path)
}

func menuValidateSingular(field protoreflect.FieldDescriptor, value any, path string) error {
	if field.Message() != nil {
		switch field.Message().FullName() {
		case "google.protobuf.Value":
			return nil
		case "google.protobuf.Struct":
			if _, ok := value.(map[string]any); !ok {
				return fmt.Errorf("%s: expected object", path)
			}
			return nil
		case "google.protobuf.ListValue":
			if _, ok := value.([]any); !ok {
				return fmt.Errorf("%s: expected array", path)
			}
			return nil
		}
		if field.Message().FullName() == "google.protobuf.Duration" {
			raw, _ := json.Marshal(value)
			if protojson.Unmarshal(raw, dynamicpb.NewMessage(field.Message())) != nil {
				return fmt.Errorf("%s: expected duration string (for example 30s)", path)
			}
			return nil
		}
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object (%s)", path, field.Message().Name())
		}
		return menuValidateMessage(field.Message(), object, path)
	}
	expected := menuSingularSchema(field).Type
	valid := false
	switch expected {
	case "string":
		_, valid = value.(string)
	case "boolean":
		_, valid = value.(bool)
	case "integer", "number":
		switch v := value.(type) {
		case float64:
			valid = expected == "number" || v == float64(int64(v))
		case json.Number:
			_, err := v.Int64()
			valid = err == nil || expected == "number"
		case string:
			valid = field.Kind() == protoreflect.Int64Kind || field.Kind() == protoreflect.Uint64Kind
		}
	}
	if !valid {
		return fmt.Errorf("%s: expected %s", path, expected)
	}
	if text, ok := value.(string); ok && field.Kind() == protoreflect.StringKind {
		rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
		if uint64(utf8.RuneCountInString(text)) < rules.GetString().GetMinLen() {
			return fmt.Errorf("%s: expected string with at least %d characters", path, rules.GetString().GetMinLen())
		}
	}
	// Let the generated scalar codec check ranges, base64 and enum spelling,
	// then return our value-free field path rather than its raw-input diagnostic.
	encodedValue := value
	if field.IsList() {
		encodedValue = []any{value}
	}
	raw, _ := json.Marshal(map[string]any{string(field.Name()): encodedValue})
	if protojson.Unmarshal(raw, dynamicpb.NewMessage(field.ContainingMessage())) != nil {
		return fmt.Errorf("%s: expected valid %s (%s)", path, expected, field.Kind())
	}
	return nil
}

func menuValidateMessage(message protoreflect.MessageDescriptor, object map[string]any, path string) error {
	present := map[protoreflect.FieldNumber]bool{}
	for name, value := range object {
		field := message.Fields().ByName(protoreflect.Name(name))
		if field == nil {
			field = message.Fields().ByJSONName(name)
		}
		if field == nil {
			return fmt.Errorf("%s.%s: unknown field; expected fields of %s", path, name, message.Name())
		}
		if present[field.Number()] {
			return fmt.Errorf("%s.%s: duplicate field", path, name)
		}
		present[field.Number()] = true
		if err := menuValidateShape(field, value, strings.TrimPrefix(path+"."+string(field.Name()), ".")); err != nil {
			return err
		}
	}
	for i := 0; i < message.Fields().Len(); i++ {
		field := message.Fields().Get(i)
		if menuFieldRequired(field) && field.ContainingOneof() == nil && !present[field.Number()] {
			return fmt.Errorf("%s.%s: required; expected %s", path, field.Name(), menuFieldSchema(field).Type)
		}
	}
	for i := 0; i < message.Oneofs().Len(); i++ {
		group := message.Oneofs().Get(i)
		names := []string{}
		count := 0
		for j := 0; j < group.Fields().Len(); j++ {
			field := group.Fields().Get(j)
			names = append(names, string(field.Name()))
			if present[field.Number()] {
				count++
			}
		}
		rules, _ := proto.GetExtension(group.Options(), validate.E_Oneof).(*validate.OneofRules)
		if count > 1 || count == 0 && rules.GetRequired() {
			return fmt.Errorf("%s: expected exactly one of %s", path, strings.Join(names, ", "))
		}
	}
	return nil
}

func menuReferenceSchema() *MenuSchema {
	object := menuMessageSchema((&contracts.ObjectRef{}).ProtoReflect().Descriptor())
	object.Required = []string{"kind", "name"}
	return &MenuSchema{AnyOf: []*MenuSchema{{Type: "string", Description: "Kind/name or Kind/namespace/name (kubectl aliases accepted)"}, object}}
}
