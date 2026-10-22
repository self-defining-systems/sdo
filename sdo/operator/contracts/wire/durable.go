package wire

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// UnmarshalDurable drops only the retired PlanApplication mutation journal in
// documents from prior builds. Other unknown fields still fail strict decoding.
func UnmarshalDurable(data []byte, message proto.Message) error {
	// Keep the ordinary strict path unchanged, including rejection of malformed
	// documents. Migration is needed only for the two explicitly retired fields.
	if err := Unmarshal(data, message); err == nil {
		return nil
	} else if !strings.Contains(err.Error(), `unknown field "pre_images"`) && !strings.Contains(err.Error(), `unknown field "preImages"`) && !strings.Contains(err.Error(), `unknown field "patches"`) {
		return err
	}
	cleaned, err := discardPlanJournals(data, message.ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("decode durable %s: %w", name(message), err)
	}
	return Unmarshal(cleaned, message)
}

func discardPlanJournals(data json.RawMessage, descriptor protoreflect.MessageDescriptor) (json.RawMessage, error) {
	// Well-known messages have special ProtoJSON shapes and cannot contain an
	// SDO PlanApplication. In particular, leave arbitrary Struct/Value data alone.
	if strings.HasPrefix(string(descriptor.FullName()), "google.protobuf.") || string(data) == "null" {
		return data, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	if descriptor.FullName() == "sdodev.contracts.v1alpha1.PlanApplication" {
		delete(object, "pre_images")
		delete(object, "preImages")
		delete(object, "patches")
	}
	for key, value := range object {
		fields := descriptor.Fields()
		field := fields.ByName(protoreflect.Name(key))
		if field == nil {
			field = fields.ByJSONName(key)
		}
		if field == nil || field.Message() == nil || field.IsMap() {
			continue
		}
		var err error
		if field.IsList() {
			var list []json.RawMessage
			if err = json.Unmarshal(value, &list); err != nil {
				return nil, err
			}
			for i := range list {
				if list[i], err = discardPlanJournals(list[i], field.Message()); err != nil {
					return nil, err
				}
			}
			object[key], err = json.Marshal(list)
		} else {
			object[key], err = discardPlanJournals(value, field.Message())
		}
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(object)
}
