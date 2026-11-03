package memoryhost

import (
	"errors"
	"google.golang.org/protobuf/types/known/structpb"
	"math"
	"unicode/utf8"
)

// Validate the dynamic JSON tree before using its native map representation.
// This preserves protojson's refusal of unset values and invalid UTF-8, without
// routing ordinary JSON data through the protobuf field-name encoder again.
func validStruct(value *structpb.Struct) error {
	for key, field := range value.GetFields() {
		if !utf8.ValidString(key) {
			return errors.New("invalid UTF-8 JSON key")
		}
		if err := validValue(field); err != nil {
			return err
		}
	}
	return nil
}
func validValue(value *structpb.Value) error {
	switch kind := value.GetKind().(type) {
	case *structpb.Value_NumberValue:
		if math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) {
			return errors.New("non-finite dynamic JSON number")
		}
		return nil
	case *structpb.Value_NullValue, *structpb.Value_BoolValue:
		return nil
	case *structpb.Value_StringValue:
		if !utf8.ValidString(kind.StringValue) {
			return errors.New("invalid UTF-8 JSON value")
		}
		return nil
	case *structpb.Value_StructValue:
		return validStruct(kind.StructValue)
	case *structpb.Value_ListValue:
		for _, element := range kind.ListValue.GetValues() {
			if err := validValue(element); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("unset dynamic JSON value")
	}
}
