package runtime

import (
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// canonicalizePlanObject rewrites the values a plan writes into the form the API
// server stores, so that comparing them with the live object after the write
// reports only real changes. The server stores resource quantities in canonical
// form ("1000m" becomes "1", "0.5Gi" becomes "512Mi") and drops empty optional
// lists.
func canonicalizePlanObject(object *unstructured.Unstructured) {
	for _, field := range []string{"spec", "data", "binaryData"} {
		if value, ok := object.Object[field]; ok {
			object.Object[field] = canonicalizePlanValue(value, "")
		}
	}
}

func canonicalizePlanValue(value any, key string) any {
	switch v := value.(type) {
	case map[string]any:
		for name, child := range v {
			if list, ok := child.([]any); ok && len(list) == 0 {
				delete(v, name)
				continue
			}
			if name == "limits" || name == "requests" {
				v[name] = canonicalizeQuantities(child)
				continue
			}
			v[name] = canonicalizePlanValue(child, name)
		}
	case []any:
		for i, child := range v {
			v[i] = canonicalizePlanValue(child, key)
		}
	}
	return value
}

func canonicalizeQuantities(value any) any {
	quantities, ok := value.(map[string]any)
	if !ok {
		return value
	}
	for name, raw := range quantities {
		switch raw.(type) {
		case string, float64, int64, int:
		default:
			continue
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		var quantity resource.Quantity
		if err := json.Unmarshal(encoded, &quantity); err != nil {
			continue
		}
		quantities[name] = quantity.String()
	}
	return quantities
}

// planOwnedFieldMismatch locates the first value the plan wrote that live does
// not carry, so a concurrent-change error can name it.
type planOwnedFieldMismatch struct {
	Path    string
	Written any
	Live    any
}

func (m planOwnedFieldMismatch) String() string {
	return fmt.Sprintf("%s wrote %s but live is %s", m.Path, planValueText(m.Written), planValueText(m.Live))
}

const maxPlanValueText = 200

func planValueText(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	if len(encoded) > maxPlanValueText {
		return string(encoded[:maxPlanValueText]) + "..."
	}
	return string(encoded)
}

// findPlanOwnedFieldMismatch reports whether live still carries every value the
// plan wrote. Keys the plan did not write are ignored: the API server re-defaults
// the fields a source manifest omits, and controllers own the rest.
func findPlanOwnedFieldMismatch(path string, live, written any) (planOwnedFieldMismatch, bool) {
	switch w := written.(type) {
	case map[string]any:
		l, ok := live.(map[string]any)
		if !ok {
			if len(w) == 0 && live == nil {
				return planOwnedFieldMismatch{}, false
			}
			return planOwnedFieldMismatch{Path: path, Written: written, Live: live}, true
		}
		keys := make([]string, 0, len(w))
		for key := range w {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if mismatch, found := findPlanOwnedFieldMismatch(path+"."+key, l[key], w[key]); found {
				return mismatch, true
			}
		}
		return planOwnedFieldMismatch{}, false
	case []any:
		l, ok := live.([]any)
		if !ok || len(l) != len(w) {
			return planOwnedFieldMismatch{Path: path, Written: written, Live: live}, true
		}
		for i := range w {
			if mismatch, found := findPlanOwnedFieldMismatch(fmt.Sprintf("%s[%d]", path, i), l[i], w[i]); found {
				return mismatch, true
			}
		}
		return planOwnedFieldMismatch{}, false
	default:
		if planValuesEqual(live, written) {
			return planOwnedFieldMismatch{}, false
		}
		return planOwnedFieldMismatch{Path: path, Written: written, Live: live}, true
	}
}
