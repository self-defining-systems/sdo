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
				sortWireList(a, keys)
				sortWireList(b, keys)
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

func sortWireList(value any, keys []string) {
	items, _ := value.([]any)
	if len(items) < 2 {
		return
	}
	type keyedItem struct {
		key   string
		value any
	}
	keyed := make([]keyedItem, len(items))
	for i, value := range items {
		item, _ := value.(map[string]any)
		parts := make([]any, 0, len(keys))
		for _, key := range keys {
			part := item[key]
			if key == "protocol" && (part == nil || part == "") {
				part = "TCP"
			}
			if part == nil {
				return
			}
			parts = append(parts, part)
		}
		encoded, _ := json.Marshal(parts)
		keyed[i] = keyedItem{key: string(encoded), value: value}
	}
	slices.SortFunc(keyed, func(a, b keyedItem) int { return strings.Compare(a.key, b.key) })
	for i := 1; i < len(keyed); i++ {
		if keyed[i-1].key == keyed[i].key {
			return
		}
	}
	for i, item := range keyed {
		items[i] = item.value
	}
}
