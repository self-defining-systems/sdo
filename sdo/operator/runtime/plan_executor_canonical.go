package runtime

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// canonicalizePlanObject rewrites the values a plan writes into the form the API
// server stores, so that comparing them with the live object after the write
// reports only real changes. The server stores resource quantities in canonical
// form ("1000m" becomes "1", "0.5Gi" becomes "512Mi") and drops empty optional
// lists.
func canonicalizePlanObject(object *unstructured.Unstructured) {
	if object == nil {
		return
	}
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
