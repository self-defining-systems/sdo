package runtime

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sdo.dev/operator/rendered"
)

// normalizeSourceWire reconciles representations on already detached copies.
// Keep explicit source zeros: an omitted live readOnly means false, but a live
// true must still differ from an explicitly declared false.
func normalizeSourceWire(accepted, observed map[string]any, gvk schema.GroupVersionKind) {
	typed, err := scheme.Scheme.New(gvk)
	if err != nil {
		return
	}
	normalizeWireValue(accepted, observed, reflect.TypeOf(typed))
}

func normalizeWireValue(accepted, observed any, typ reflect.Type) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		left, _ := accepted.(map[string]any)
		right, _ := observed.(map[string]any)
		if left == nil || right == nil {
			return
		}
		normalizeWireDefaults(left, typ)
		normalizeWireDefaults(right, typ)
		for i := range typ.NumField() {
			field := typ.Field(i)
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if !field.IsExported() || name == "-" {
				continue
			}
			if name == "" && field.Anonymous {
				normalizeWireValue(left, right, field.Type)
				continue
			}
			if name == "" {
				name = field.Name
			}
			a, declared := left[name]
			b, live := right[name]
			if !declared {
				continue
			}
			if slices.Contains(strings.Split(options, ","), "omitempty") &&
				wireOmitted(a, field.Type) && (!live || wireOmitted(b, field.Type)) {
				right[name] = a
				continue
			}
			// Init containers and environment expansion execute in declaration order.
			if key := field.Tag.Get("patchMergeKey"); key != "" && name != "env" && name != rendered.InitContainers {
				keys := []string{key}
				if name == "ports" && (typ == reflect.TypeFor[corev1.ServiceSpec]() ||
					typ == reflect.TypeFor[corev1.Container]() || typ == reflect.TypeFor[corev1.EphemeralContainerCommon]()) {
					keys = append(keys, "protocol")
				}
				alignWireList(a, b, keys)
			}
			normalizeWireValue(a, b, field.Type)
		}
	case reflect.Map:
		left, _ := accepted.(map[string]any)
		right, _ := observed.(map[string]any)
		for key, a := range left {
			normalizeWireValue(a, right[key], typ.Elem())
		}
	case reflect.Slice, reflect.Array:
		left, _ := accepted.([]any)
		right, _ := observed.([]any)
		for i := range min(len(left), len(right)) {
			normalizeWireValue(left[i], right[i], typ.Elem())
		}
	}
}

// dropSourceWireOmitted deletes, in place, every omitempty field of a built-in
// kind whose value the API server omits on read (a zero or false scalar, an
// empty list or map), giving one object the form a read returns. Pointer
// fields keep an explicit zero and a null stays (a zero timestamp is written
// as null): the server keeps both.
func dropSourceWireOmitted(object map[string]any, gvk schema.GroupVersionKind) {
	typed, err := scheme.Scheme.New(gvk)
	if err != nil {
		return
	}
	dropWireOmitted(object, reflect.TypeOf(typed))
}

func dropWireOmitted(value any, typ reflect.Type) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		fields, _ := value.(map[string]any)
		if fields == nil {
			return
		}
		for i := range typ.NumField() {
			field := typ.Field(i)
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if !field.IsExported() || name == "-" {
				continue
			}
			if name == "" && field.Anonymous {
				dropWireOmitted(fields, field.Type)
				continue
			}
			if name == "" {
				name = field.Name
			}
			child, present := fields[name]
			if !present {
				continue
			}
			if child != nil && slices.Contains(strings.Split(options, ","), "omitempty") && wireOmitted(child, field.Type) {
				delete(fields, name)
				continue
			}
			dropWireOmitted(child, field.Type)
		}
	case reflect.Map:
		fields, _ := value.(map[string]any)
		for _, child := range fields {
			dropWireOmitted(child, typ.Elem())
		}
	case reflect.Slice, reflect.Array:
		items, _ := value.([]any)
		for _, child := range items {
			dropWireOmitted(child, typ.Elem())
		}
	}
}

func wireOmitted(value any, typ reflect.Type) bool {
	if value == nil {
		return true
	}
	switch typ.Kind() {
	case reflect.Bool:
		return value == false
	case reflect.Int, reflect.Int32, reflect.Int64:
		return value == int64(0) || value == float64(0)
	case reflect.String:
		return value == ""
	case reflect.Slice, reflect.Map:
		switch value := value.(type) {
		case []any:
			return len(value) == 0
		case map[string]any:
			return len(value) == 0
		}
	}
	return false
}

// Client-side fallback for built-in defaults that replace explicit zero values.
// Arbitrary strings and pointer zeros keep their API meaning.
func normalizeWireDefaults(fields map[string]any, typ reflect.Type) {
	switch typ {
	case reflect.TypeFor[corev1.Probe]():
		for name, value := range map[string]int64{"timeoutSeconds": 1, "periodSeconds": 10, "successThreshold": 1, "failureThreshold": 3} {
			if zero := fields[name]; zero == nil || zero == int64(0) || zero == float64(0) {
				fields[name] = value
			}
		}
	case reflect.TypeFor[corev1.HTTPGetAction]():
		if fields["scheme"] == "" || fields["scheme"] == nil {
			fields["scheme"] = "HTTP"
		}
		if fields["path"] == "" || fields["path"] == nil {
			fields["path"] = "/"
		}
	case reflect.TypeFor[corev1.ContainerPort](), reflect.TypeFor[corev1.ServicePort]():
		if fields["protocol"] == "" || fields["protocol"] == nil {
			fields["protocol"] = "TCP"
		}
		if target, ok := fields["targetPort"]; typ == reflect.TypeFor[corev1.ServicePort]() && ok &&
			(target == "" || target == int64(0) || target == float64(0)) {
			fields["targetPort"] = fields["port"]
		}
	}
}

// Align the accepted ordering with the observed ordering when both lists
// contain the same unique keys. Drift and settlement paths retain their live
// indices, including when a watcher reports changes elsewhere on the object.
func alignWireList(accepted, observed any, keys []string) {
	left, _ := accepted.([]any)
	right, _ := observed.([]any)
	if len(left) < 2 || len(left) != len(right) {
		return
	}
	byKey := make(map[string]any, len(left))
	for _, item := range left {
		key := wireListKey(item, keys)
		if _, duplicate := byKey[key]; key == "" || duplicate {
			return
		}
		byKey[key] = item
	}
	aligned := make([]any, len(left))
	for i, item := range right {
		key := wireListKey(item, keys)
		matched, found := byKey[key]
		if !found {
			return
		}
		aligned[i] = matched
		delete(byKey, key)
	}
	copy(left, aligned)
}

func wireListKey(value any, keys []string) string {
	item, _ := value.(map[string]any)
	parts := make([]any, 0, len(keys))
	for _, key := range keys {
		part := item[key]
		if key == "protocol" && (part == nil || part == "") {
			part = "TCP"
		}
		if part == nil {
			return ""
		}
		parts = append(parts, part)
	}
	encoded, _ := json.Marshal(parts)
	return string(encoded)
}
