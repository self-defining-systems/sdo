package runtime

import (
	"fmt"
	"maps"
	"reflect"

	"k8s.io/client-go/kubernetes/scheme"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk"
)

// sourceBookkeepingAnnotations is the single annotation exclusion policy for
// comparisons, acceptance records and replay. Do not ignore whole annotation
// domains: user-authored scheduling and security annotations can be repairs.
var sourceBookkeepingAnnotations = []string{
	"sdo.dev/source-tree",
	acceptedPathsAnnotation,
	"deployment.kubernetes.io/revision",
	"kubectl.kubernetes.io/last-applied-configuration",
	"kubectl.kubernetes.io/restartedAt",
}

func sourceBookkeepingAnnotation(name string) bool {
	return slices.Contains(sourceBookkeepingAnnotations, name)
}

// HPA ownership comes from the captured target identity, not a manager name or
// a live-only API read; offline replay uses exactly the same evidence.
func (v LiveReadView) hpaOwnsReplicas(ref sdk.ObjectRef) bool {
	groupVersion, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return false
	}
	for _, hpa := range v.HorizontalPodAutoscalers() {
		target := hpa.Spec.ScaleTargetRef
		targetVersion, err := schema.ParseGroupVersion(target.APIVersion)
		if err == nil && hpa.Namespace == ref.Namespace && target.Name == ref.Name && target.Kind == ref.Kind && targetVersion.Group == groupVersion.Group {
			return true
		}
	}
	return false
}

func (v LiveReadView) sourceComparableFields(object map[string]any, ref sdk.ObjectRef) map[string]any {
	result := comparableFields(object)
	if v.hpaOwnsReplicas(ref) {
		unstructured.RemoveNestedField(result, "spec", "replicas")
	}
	return result
}

func (v LiveReadView) sourceDriftPaths(paths []string, ref sdk.ObjectRef) []string {
	if v.hpaOwnsReplicas(ref) {
		return slices.DeleteFunc(paths, func(path string) bool { return path == "spec.replicas" })
	}
	return paths
}

// canonicalQuantity retains invalid values so they remain visible as drift.
func canonicalQuantity(value any) any {
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case int64, float64:
		text = fmt.Sprint(typed)
	default:
		return value
	}
	quantity, err := resource.ParseQuantity(text)
	if err != nil {
		return value
	}
	decimal := quantity.AsDec().String()
	if strings.Contains(decimal, ".") {
		decimal = strings.TrimRight(strings.TrimRight(decimal, "0"), ".")
	}
	return decimal
}

// Normalize only fields typed as Kubernetes quantities. Arbitrary strings such
// as env values, image tags and ConfigMap payloads retain their exact meaning.
func normalizeSourceQuantities(object map[string]any) {
	visitSourceQuantities(object, func(_ string, value any, set func(any)) { set(canonicalQuantity(value)) })
}

// Use the registered API schema rather than a second hand-maintained list of
// resource field paths. This includes quantities in resources, volumes and
// resourceFieldRef divisors, while unknown kinds and invalid shapes stay intact.
// Callers inspecting historical values never call set or mutate the snapshot.
func visitSourceQuantities(object map[string]any, visit func(string, any, func(any))) {
	apiVersion, _ := object["apiVersion"].(string)
	kind, _ := object["kind"].(string)
	typed, err := scheme.Scheme.New(schema.FromAPIVersionAndKind(apiVersion, kind))
	if err != nil {
		return
	}
	quantityType := reflect.TypeFor[resource.Quantity]()
	var walk func(any, reflect.Type, string, func(any))
	walk = func(value any, typ reflect.Type, path string, set func(any)) {
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ == quantityType {
			visit(path, value, set)
			return
		}
		childPath := func(name string) string {
			if path == "" {
				return name
			}
			return path + "." + name
		}
		switch typ.Kind() {
		case reflect.Struct:
			fields, ok := value.(map[string]any)
			if !ok {
				return
			}
			for index := 0; index < typ.NumField(); index++ {
				field := typ.Field(index)
				name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
				if name == "-" || !field.IsExported() {
					continue
				}
				if name == "" && field.Anonymous {
					walk(value, field.Type, path, set)
					continue
				}
				if name == "" {
					name = field.Name
				}
				if child, exists := fields[name]; exists {
					walk(child, field.Type, childPath(name), func(next any) { fields[name] = next })
				}
			}
		case reflect.Map:
			fields, ok := value.(map[string]any)
			if !ok {
				return
			}
			for name, child := range fields {
				walk(child, typ.Elem(), childPath(name), func(next any) { fields[name] = next })
			}
		case reflect.Slice, reflect.Array:
			values, ok := value.([]any)
			if !ok {
				return
			}
			for index, child := range values {
				walk(child, typ.Elem(), fmt.Sprintf("%s[%d]", path, index), func(next any) { values[index] = next })
			}
		}
	}
	walk(object, reflect.TypeOf(typed), "", func(any) {})
}

func normalizedAcceptanceValues(observed *unstructured.Unstructured, values map[string]any) map[string]any {
	result := maps.Clone(values)
	visitSourceQuantities(observed.Object, func(path string, _ any, _ func(any)) {
		if value, ok := result[path]; ok {
			result[path] = canonicalQuantity(value)
		}
	})
	return result
}
