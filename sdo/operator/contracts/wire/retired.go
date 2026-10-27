package wire

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Accepted memory and durable controller state outlive the build that wrote
// them. When the schema renames a field or retires an allowed value, the
// stored documents keep the old spelling, so the reader maps it to its
// successor here instead of rejecting the document. The schema never keeps the
// old name or value; these tables are the only place that remembers them.
// TestRetiredTablesCoverEverySchemaRemoval fails when a removal is not listed.

// retiredFieldNames maps "<message>.<old field>" to the field that replaced it,
// or to "" when the field was removed and its stored value is dropped.
var retiredFieldNames = map[string]string{
	"sdodev.contracts.v1alpha1.ReflectionEligibility.evidence_gap_count":   "no_repair_count",
	"sdodev.contracts.v1alpha1.BrokerLedger.reflection_evidence_gap_count": "reflection_no_repair_count",
}

// retiredValues maps "<message>.<field>" to retired value -> successor, for
// string fields with an allowed-values list and for enum fields.
var retiredValues = map[string]map[string]string{
	"sdodev.contracts.v1alpha1.ReflectionWorkItem.reason":         {"evidence_gap": "no_recorded_repair"},
	"sdodev.contracts.v1alpha1.ReflectionDecision.decline_reason": {"evidence_gap": "no_recorded_repair"},
}

// RetiredValues reports the retired value -> successor table of one field,
// keyed "<message full name>.<field>". Callers must not modify the result.
func RetiredValues() map[string]map[string]string { return retiredValues }

// RetiredFieldNames reports the retired field name -> replacement table keyed
// "<message full name>.<old field>". Callers must not modify the result.
func RetiredFieldNames() map[string]string { return retiredFieldNames }

// UnmarshalLenientMigrating is UnmarshalLenient for stored documents: retired
// field names and values are first mapped to their successors, so a document
// written by an earlier build decodes and validates under the current schema.
func UnmarshalLenientMigrating(data []byte, message proto.Message) error {
	migrated, err := migrateRetired(data, message.ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("decode %s: %w", name(message), err)
	}
	return UnmarshalLenient(migrated, message)
}

func migrateRetired(data json.RawMessage, descriptor protoreflect.MessageDescriptor) (json.RawMessage, error) {
	if descriptor.FullName() == "google.protobuf.Any" {
		return migrateAny(data)
	}
	if strings.HasPrefix(string(descriptor.FullName()), "google.protobuf.") || string(data) == "null" {
		return data, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		// Not an object: leave it for the strict decoder to reject.
		return data, nil
	}
	fields := descriptor.Fields()
	for key, value := range object {
		field := fields.ByName(protoreflect.Name(key))
		if field == nil {
			field = fields.ByJSONName(key)
		}
		if field == nil {
			successor, retired := retiredFieldNames[string(descriptor.FullName())+"."+key]
			if retired && successor == "" {
				delete(object, key)
				continue
			}
			target := fields.ByName(protoreflect.Name(successor))
			if !retired || target == nil {
				continue
			}
			if _, present := object[successor]; present {
				delete(object, key)
				continue
			}
			if _, present := object[target.JSONName()]; present {
				delete(object, key)
				continue
			}
			delete(object, key)
			object[successor] = value
			key, field = successor, target
		}
		migrated, err := migrateField(value, field)
		if err != nil {
			return nil, err
		}
		object[key] = migrated
	}
	return json.Marshal(object)
}

// migrateAny migrates the message packed in an Any, whose fields sit beside
// "@type" in ProtoJSON. An Any naming an unknown type is left for the decoder.
func migrateAny(data json.RawMessage) (json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return data, nil
	}
	var url string
	if err := json.Unmarshal(object["@type"], &url); err != nil || url == "" {
		return data, nil
	}
	messageType, err := protoregistry.GlobalTypes.FindMessageByURL(url)
	if err != nil {
		return data, nil
	}
	delete(object, "@type")
	inner, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	migrated, err := migrateRetired(inner, messageType.Descriptor())
	if err != nil {
		return nil, err
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(migrated, &result); err != nil {
		return nil, err
	}
	result["@type"], _ = json.Marshal(url)
	return json.Marshal(result)
}

func migrateField(value json.RawMessage, field protoreflect.FieldDescriptor) (json.RawMessage, error) {
	switch {
	case field.IsMap():
		if field.MapValue().Message() == nil {
			return value, nil
		}
		var entries map[string]json.RawMessage
		if err := json.Unmarshal(value, &entries); err != nil {
			return value, nil
		}
		for key, entry := range entries {
			migrated, err := migrateRetired(entry, field.MapValue().Message())
			if err != nil {
				return nil, err
			}
			entries[key] = migrated
		}
		return json.Marshal(entries)
	case field.IsList():
		var list []json.RawMessage
		if err := json.Unmarshal(value, &list); err != nil {
			return value, nil
		}
		for i := range list {
			migrated, err := migrateScalarOrMessage(list[i], field)
			if err != nil {
				return nil, err
			}
			list[i] = migrated
		}
		return json.Marshal(list)
	default:
		return migrateScalarOrMessage(value, field)
	}
}

func migrateScalarOrMessage(value json.RawMessage, field protoreflect.FieldDescriptor) (json.RawMessage, error) {
	if field.Message() != nil {
		return migrateRetired(value, field.Message())
	}
	if field.Kind() != protoreflect.StringKind && field.Kind() != protoreflect.EnumKind {
		return value, nil
	}
	var text string
	if err := json.Unmarshal(value, &text); err != nil {
		return value, nil
	}
	if successor, retired := retiredValues[string(field.FullName())][text]; retired {
		return json.Marshal(successor)
	}
	return value, nil
}
