package wire

import (
	"fmt"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The helpers below read constants out of the protovalidate rules in the
// message descriptors, so a value the .proto already states (a schema version,
// an allowed-values list, an ID pattern) has no second copy in Go. They panic
// on a missing rule: that is a programming error caught by any test run, not a
// runtime condition.

func stringRules(message proto.Message, field string) *validate.StringRules {
	descriptor := message.ProtoReflect().Descriptor()
	fd := descriptor.Fields().ByName(protoreflect.Name(field))
	if fd == nil {
		panic(fmt.Sprintf("%s has no field %q", descriptor.FullName(), field))
	}
	rules, _ := proto.GetExtension(fd.Options(), validate.E_Field).(*validate.FieldRules)
	if rules == nil {
		panic(fmt.Sprintf("%s.%s has no validation rules", descriptor.FullName(), field))
	}
	if rules.GetString() != nil {
		return rules.GetString()
	}
	if items := rules.GetRepeated().GetItems().GetString(); items != nil {
		return items
	}
	panic(fmt.Sprintf("%s.%s has no string rules", descriptor.FullName(), field))
}

// StringConst is the `string.const` rule of message.field.
func StringConst(message proto.Message, field string) string {
	rules := stringRules(message, field)
	if rules.Const == nil {
		panic(fmt.Sprintf("%s.%s has no const rule", message.ProtoReflect().Descriptor().FullName(), field))
	}
	return rules.GetConst()
}

// StringIn is the `string.in` allowed-values list of message.field.
func StringIn(message proto.Message, field string) []string {
	rules := stringRules(message, field)
	if len(rules.GetIn()) == 0 {
		panic(fmt.Sprintf("%s.%s has no in rule", message.ProtoReflect().Descriptor().FullName(), field))
	}
	return append([]string(nil), rules.GetIn()...)
}

// StringPattern is the `string.pattern` rule of message.field (or of each item
// when the field is repeated).
func StringPattern(message proto.Message, field string) string {
	rules := stringRules(message, field)
	if rules.Pattern == nil {
		panic(fmt.Sprintf("%s.%s has no pattern rule", message.ProtoReflect().Descriptor().FullName(), field))
	}
	return rules.GetPattern()
}
