package lifecycleio

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MaxConfigMapBytes is the budget for one controller-written ConfigMap document. The API server rejects an
// object near 1 MiB with an opaque 422, so a document over this fails here, naming what filled it.
const MaxConfigMapBytes = 768 << 10

// ConfigMapTooLargeError reports a controller document over MaxConfigMapBytes and its largest fields.
type ConfigMapTooLargeError struct {
	Name    string
	Bytes   int
	Limit   int
	Largest []string
}

func (err *ConfigMapTooLargeError) Error() string {
	return fmt.Sprintf("controller ConfigMap %q document is %d bytes, over the %d byte budget; largest fields: %s", err.Name, err.Bytes, err.Limit, strings.Join(err.Largest, ", "))
}

func checkBudget(name string, payload []byte, message proto.Message) error {
	if len(payload) <= MaxConfigMapBytes {
		return nil
	}
	return &ConfigMapTooLargeError{Name: name, Bytes: len(payload), Limit: MaxConfigMapBytes, Largest: largestFields(message)}
}

// largestFields names the three biggest populated fields by encoded size, descending one message level so a
// large nested field is named rather than its parent.
func largestFields(message proto.Message) []string {
	type field struct {
		name string
		size int
	}
	var fields []field
	var visit func(prefix string, message protoreflect.Message, depth int)
	visit = func(prefix string, message protoreflect.Message, depth int) {
		message.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			name := prefix + string(descriptor.Name())
			if depth > 0 && descriptor.Message() != nil && !descriptor.IsList() && !descriptor.IsMap() {
				visit(name+".", value.Message(), depth-1)
				return true
			}
			single := message.Type().New()
			single.Set(descriptor, value)
			fields = append(fields, field{name, proto.Size(single.Interface())})
			return true
		})
	}
	visit("", message.ProtoReflect(), 1)
	sort.Slice(fields, func(i, j int) bool { return fields[i].size > fields[j].size })
	names := []string{}
	for _, f := range fields[:min(len(fields), 3)] {
		names = append(names, fmt.Sprintf("%s=%d", f.name, f.size))
	}
	return names
}
