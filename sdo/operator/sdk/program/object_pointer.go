package program

import (
	"fmt"
	"strconv"
	"strings"
)

// A JSON pointer selects a generic Kubernetes field. Identity and write-control
// metadata stay controller-owned; the Kubernetes API validates resource schemas.
func parseObjectPointer(path string) ([]string, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("object field requires a JSON pointer")
	}
	parts := strings.Split(path[1:], "/")
	for i, p := range parts {
		for j := 0; j < len(p); j++ {
			if p[j] == '~' {
				if j+1 >= len(p) || (p[j+1] != '0' && p[j+1] != '1') {
					return nil, fmt.Errorf("invalid JSON pointer escape")
				}
				j++
			}
		}
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(p, "~1", "/"), "~0", "~")
	}
	return parts, nil
}
func objectPointer(path string) ([]string, error) {
	parts, err := parseObjectPointer(path)
	if err != nil {
		return nil, err
	}
	switch parts[0] {
	case "apiVersion", "kind":
		return nil, fmt.Errorf("object identity is immutable")
	case "metadata":
		if len(parts) < 2 || (parts[1] != "labels" && parts[1] != "annotations") {
			return nil, fmt.Errorf("write metadata is controller-owned")
		}
	}
	if parts[0] == "" {
		return nil, fmt.Errorf("empty object field")
	}
	return parts, nil
}
func objectPointerValue(object map[string]any, path string) (any, error) {
	parts, err := parseObjectPointer(path)
	if err != nil {
		return nil, err
	}
	var value any = object
	for _, key := range parts {
		switch v := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = v[key]
			if !ok {
				return nil, fmt.Errorf("object field is absent")
			}
		case []any:
			index, e := strconv.Atoi(key)
			if e != nil || index < 0 || index >= len(v) {
				return nil, fmt.Errorf("array index is absent")
			}
			value = v[index]
		default:
			return nil, fmt.Errorf("field parent is not an object or array")
		}
	}
	return value, nil
}

func applyObjectPointer(object map[string]any, path string, value any) error {
	parts, err := objectPointer(path)
	if err != nil {
		return err
	}
	var parent any = object
	for i, key := range parts {
		final := i == len(parts)-1
		switch v := parent.(type) {
		case map[string]any:
			if final {
				v[key] = value
				return nil
			}
			next, ok := v[key]
			if !ok {
				return fmt.Errorf("object field parent %q is absent", key)
			}
			parent = next
		case []any:
			index, e := strconv.Atoi(key)
			if e != nil || index < 0 || index >= len(v) {
				return fmt.Errorf("invalid array index %q", key)
			}
			if final {
				v[index] = value
				return nil
			}
			parent = v[index]
		default:
			return fmt.Errorf("object field parent is not a map or array")
		}
	}
	return fmt.Errorf("empty object field")
}
