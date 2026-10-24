package program

import (
	"fmt"
	"strconv"
	"strings"
)

type fieldPart struct{ key, selector string }

func parseFieldPath(path string) ([]fieldPart, error) {
	var parts []fieldPart
	start, depth := 0, 0
	for i, r := range path {
		switch r {
		case '[':
			depth++
		case ']':
			depth--
		case '.':
			if depth == 0 {
				parts = append(parts, fieldPart{key: path[start:i]})
				start = i + 1
			}
		}
		if depth < 0 || depth > 1 {
			return nil, fmt.Errorf("invalid field path")
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("invalid field path")
	}
	parts = append(parts, fieldPart{key: path[start:]})
	// Legacy selector map keys can contain dots.
	if len(parts) > 2 && parts[0].key == "spec" && parts[1].key == "selector" {
		keys := []string{}
		for _, p := range parts[2:] {
			keys = append(keys, p.key)
		}
		parts = append(parts[:2], fieldPart{key: strings.Join(keys, ".")})
	}
	for i := range parts {
		key := parts[i].key
		if key == "" {
			return nil, fmt.Errorf("empty field path")
		}
		if j := strings.IndexByte(key, '['); j >= 0 {
			if !strings.HasSuffix(key, "]") || j == 0 {
				return nil, fmt.Errorf("invalid field selector")
			}
			parts[i] = fieldPart{key: key[:j], selector: key[j+1 : len(key)-1]}
			if parts[i].selector == "" {
				return nil, fmt.Errorf("empty field selector")
			}
		}
	}
	return parts, nil
}

func selectedField(value any, selector string) (any, error) {
	if m, ok := value.(map[string]any); ok {
		v, exists := m[selector]
		if !exists {
			return nil, fmt.Errorf("map entry is absent")
		}
		return v, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("selector requires a list or map")
	}
	if strings.HasPrefix(selector, "name=") {
		var found any
		count := 0
		for _, item := range items {
			m, ok := item.(map[string]any)
			if ok && m["name"] == strings.TrimPrefix(selector, "name=") {
				found = item
				count++
			}
		}
		if count != 1 {
			return nil, fmt.Errorf("named list identity must match exactly once")
		}
		return found, nil
	}
	index, err := strconv.Atoi(selector)
	if err != nil || index < 0 || index >= len(items) {
		return nil, fmt.Errorf("list index is absent")
	}
	return items[index], nil
}

// FieldValue resolves numeric indexes, unique [name=NAME] list identities and
// bracketed ConfigMap keys. It does not invent missing intermediate objects.
func FieldValue(object map[string]any, path string) (any, error) {
	if strings.HasPrefix(path, "/") {
		return objectPointerValue(object, path)
	}
	parts, err := parseFieldPath(path)
	if err != nil {
		return nil, err
	}
	var current any = object
	for _, p := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field parent is not an object")
		}
		v, exists := m[p.key]
		if !exists {
			return nil, fmt.Errorf("field is absent")
		}
		current = v
		if p.selector != "" {
			current, err = selectedField(current, p.selector)
			if err != nil {
				return nil, err
			}
		}
	}
	return current, nil
}

// ApplySetField mutates one field. Named list selectors must resolve uniquely,
// except a missing final env[name=NAME] can insert an entry with that same name.
func ApplySetField(object map[string]any, path string, value any) error {
	if strings.HasPrefix(path, "/") {
		return applyObjectPointer(object, path, value)
	}

	parts, err := parseFieldPath(path)
	if err != nil {
		return err
	}
	var current any = object
	for _, p := range parts[:len(parts)-1] {
		m, ok := current.(map[string]any)
		if !ok {
			return fmt.Errorf("field parent is not an object")
		}
		current = m[p.key]
		if p.selector != "" {
			current, err = selectedField(current, p.selector)
			if err != nil {
				return err
			}
		}
	}
	last := parts[len(parts)-1]
	m, ok := current.(map[string]any)
	if !ok {
		return fmt.Errorf("field parent is absent")
	}
	if last.selector == "" {
		m[last.key] = value
		return nil
	}
	if last.key == "data" && m[last.key] == nil {
		m[last.key] = map[string]any{}
	}
	if last.key == "env" && strings.HasPrefix(last.selector, "name=") {
		entry, ok := value.(map[string]any)
		name := strings.TrimPrefix(last.selector, "name=")
		if !ok || name == "" || entry["name"] != name {
			return fmt.Errorf("env entry must preserve its named identity")
		}
		var items []any
		if m[last.key] != nil {
			var list bool
			items, list = m[last.key].([]any)
			if !list {
				return fmt.Errorf("env field is not a list")
			}
		}
		count, index := 0, -1
		for i, item := range items {
			if object, ok := item.(map[string]any); ok && object["name"] == name {
				count++
				index = i
			}
		}
		if count > 1 {
			return fmt.Errorf("named list identity must match at most once")
		}
		if count == 0 {
			m[last.key] = append(items, value)
		} else {
			items[index] = value
		}
		return nil
	}
	if mapping, ok := m[last.key].(map[string]any); ok {
		mapping[last.selector] = value
		return nil
	}
	items, ok := m[last.key].([]any)
	if !ok {
		return fmt.Errorf("field is not a list")
	}
	selected, err := selectedField(items, last.selector)
	if err != nil {
		return err
	}
	if strings.HasPrefix(last.selector, "name=") {
		for i, item := range items {
			if itemMap, ok := item.(map[string]any); ok && itemMap["name"] == selected.(map[string]any)["name"] {
				items[i] = value
				return nil
			}
		}
	}
	index, _ := strconv.Atoi(last.selector)
	items[index] = value
	return nil
}
