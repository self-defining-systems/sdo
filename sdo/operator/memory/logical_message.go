package memory

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"sdo.dev/operator/memory/layout"
)

// LogicalizeMessage rewrites, in place, every string in message (nested
// messages, lists and map entries included) through layout.LogicalizeText.
// Callers apply it to evidence that can quote agent prose, such as a
// responder result naming the patch file it wrote, before that evidence
// reaches a worker that sees only logical memory. SDO-authored logical
// memory (a snapshot) is not passed here: a physical path there is a leak
// the dispatch guard must still refuse.
func LogicalizeMessage(message proto.Message) {
	if message == nil {
		return
	}
	logicalizeMessage(message.ProtoReflect())
}

func logicalizeMessage(message protoreflect.Message) {
	if !message.IsValid() {
		return
	}
	var fields []protoreflect.FieldDescriptor
	message.Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		fields = append(fields, field)
		return true
	})
	for _, field := range fields {
		switch {
		case field.IsList():
			list := message.Mutable(field).List()
			for i := range list.Len() {
				if item, ok := logicalizeValue(field, list.Get(i)); ok {
					list.Set(i, item)
				}
			}
		case field.IsMap():
			logicalizeMap(field, message.Mutable(field).Map())
		case field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind:
			logicalizeMessage(message.Mutable(field).Message())
		default:
			if item, ok := logicalizeValue(field, message.Get(field)); ok {
				message.Set(field, item)
			}
		}
	}
}

// logicalizeValue returns the rewritten scalar string, or ok=false when value
// is a message (rewritten in place) or holds no physical path.
func logicalizeValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (protoreflect.Value, bool) {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		logicalizeMessage(value.Message())
		return value, false
	case protoreflect.StringKind:
		text := value.String()
		if !layout.ContainsPhysicalPath(text) {
			return value, false
		}
		return protoreflect.ValueOfString(layout.LogicalizeText(text)), true
	}
	return value, false
}

func logicalizeMap(field protoreflect.FieldDescriptor, entries protoreflect.Map) {
	type entry struct {
		key   protoreflect.MapKey
		value protoreflect.Value
	}
	var changed []entry
	entries.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		item, rewritten := logicalizeValue(field.MapValue(), value)
		rekey := field.MapKey().Kind() == protoreflect.StringKind && layout.ContainsPhysicalPath(key.String())
		if rewritten || rekey {
			changed = append(changed, entry{key: key, value: item})
		}
		return true
	})
	for _, item := range changed {
		entries.Clear(item.key)
		key := item.key
		if field.MapKey().Kind() == protoreflect.StringKind {
			key = protoreflect.ValueOfString(layout.LogicalizeText(key.String())).MapKey()
		}
		entries.Set(key, item.value)
	}
}
