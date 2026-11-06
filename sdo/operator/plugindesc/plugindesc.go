// Package plugindesc describes a catalog plugin (a renderer or an image
// source) to an agent: its ID, the proto message it takes as config, a JSON
// Schema generated from that message's descriptor, and the short usage doc the
// implementation package exports. It imports only protobuf, so both plugin
// families use it without importing each other.
package plugindesc

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Descriptor is what an agent is shown about one registered plugin.
type Descriptor struct {
	ID               string
	ConfigType       string // full proto name of the config message
	ConfigSchemaJSON string // JSON Schema of the config in its protojson form
	Doc              string // what the plugin does and when to choose it
}

// Describe builds the Descriptor of a plugin with config message md.
func Describe(id string, md protoreflect.MessageDescriptor, doc string) (Descriptor, error) {
	schema, err := Schema(md)
	if err != nil {
		return Descriptor{}, fmt.Errorf("plugin %q: %w", id, err)
	}
	return Descriptor{ID: id, ConfigType: string(md.FullName()), ConfigSchemaJSON: schema, Doc: strings.TrimSpace(doc)}, nil
}

// Schema returns a JSON Schema (draft 2020-12) for md in the canonical
// protobuf JSON mapping. Field names are the protojson names. Generated
// descriptors carry no comments, so the schema describes shape only; meaning
// belongs in the plugin's Doc. The output is deterministic.
func Schema(md protoreflect.MessageDescriptor) (string, error) {
	g := &generator{defs: map[string]any{}}
	root, err := g.message(md)
	if err != nil {
		return "", err
	}
	out := map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "title": string(md.FullName())}
	for k, v := range root {
		out[k] = v
	}
	if len(g.defs) > 0 {
		out["$defs"] = g.defs
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type generator struct {
	defs map[string]any
}

func (g *generator) message(md protoreflect.MessageDescriptor) (map[string]any, error) {
	if s, ok := wellKnown(md.FullName()); ok {
		return s, nil
	}
	name := string(md.FullName())
	ref := map[string]any{"$ref": "#/$defs/" + name}
	if _, seen := g.defs[name]; seen {
		return ref, nil
	}
	g.defs[name] = nil // reserve against recursion
	props := map[string]any{}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		s, err := g.field(f)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", name, f.Name(), err)
		}
		props[f.JSONName()] = s
	}
	g.defs[name] = map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	return ref, nil
}

func (g *generator) field(f protoreflect.FieldDescriptor) (map[string]any, error) {
	if f.IsMap() {
		v, err := g.single(f.MapValue())
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "object", "additionalProperties": v}, nil
	}
	s, err := g.single(f)
	if err != nil {
		return nil, err
	}
	if f.IsList() {
		return map[string]any{"type": "array", "items": s}, nil
	}
	return s, nil
}

func (g *generator) single(f protoreflect.FieldDescriptor) (map[string]any, error) {
	switch f.Kind() {
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}, nil
	case protoreflect.StringKind:
		return map[string]any{"type": "string"}, nil
	case protoreflect.BytesKind:
		return map[string]any{"type": "string", "contentEncoding": "base64"}, nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return map[string]any{"type": "integer"}, nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return map[string]any{"type": []string{"integer", "string"}}, nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return map[string]any{"type": "number"}, nil
	case protoreflect.EnumKind:
		values := f.Enum().Values()
		names := make([]string, 0, values.Len())
		for i := 0; i < values.Len(); i++ {
			names = append(names, string(values.Get(i).Name()))
		}
		return map[string]any{"type": "string", "enum": names}, nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return g.message(f.Message())
	}
	return nil, fmt.Errorf("unsupported field kind %s", f.Kind())
}

func wellKnown(name protoreflect.FullName) (map[string]any, bool) {
	switch name {
	case "google.protobuf.Struct":
		return map[string]any{"type": "object"}, true
	case "google.protobuf.Value":
		return map[string]any{}, true
	case "google.protobuf.ListValue":
		return map[string]any{"type": "array"}, true
	case "google.protobuf.Duration":
		return map[string]any{"type": "string", "pattern": `^-?[0-9]+(\.[0-9]{1,9})?s$`, "description": "seconds with an s suffix, for example 30s"}, true
	case "google.protobuf.Timestamp":
		return map[string]any{"type": "string", "format": "date-time"}, true
	case "google.protobuf.Empty":
		return map[string]any{"type": "object", "additionalProperties": false}, true
	case "google.protobuf.Any":
		return map[string]any{"type": "object", "required": []string{"@type"}}, true
	case "google.protobuf.StringValue":
		return map[string]any{"type": "string"}, true
	case "google.protobuf.BoolValue":
		return map[string]any{"type": "boolean"}, true
	}
	return nil, false
}
