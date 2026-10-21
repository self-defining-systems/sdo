package program

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var planIndex = regexp.MustCompile(`^([^\[]+)\[([0-9]+)\]$`)

// ApplySetField mutates an object using the plan executor's field-path semantics.
func ApplySetField(object map[string]any, path string, value any) error {
	parts := strings.Split(path, ".")
	if len(parts) > 2 && parts[0] == "spec" && parts[1] == "selector" {
		parts = append(parts[:2], strings.Join(parts[2:], "."))
	}
	var current any = object
	for _, part := range parts[:len(parts)-1] {
		name, index, indexed, err := planPathPart(part)
		if err != nil {
			return err
		}
		mapping, ok := current.(map[string]any)
		if !ok {
			return fmt.Errorf("path %q has no object at %q", path, part)
		}
		child, ok := mapping[name]
		if !ok {
			return fmt.Errorf("path %q has no field %q", path, name)
		}
		if indexed {
			items, ok := child.([]any)
			if !ok || index >= len(items) {
				return fmt.Errorf("path %q has no index %d", path, index)
			}
			child = items[index]
		}
		current = child
	}
	last, index, indexed, err := planPathPart(parts[len(parts)-1])
	if err != nil {
		return err
	}
	mapping, ok := current.(map[string]any)
	if !ok {
		return fmt.Errorf("path %q has no target object", path)
	}
	if indexed {
		items, ok := mapping[last].([]any)
		if !ok || index >= len(items) {
			return fmt.Errorf("path %q has no index %d", path, index)
		}
		items[index] = value
	} else {
		mapping[last] = value
	}
	return nil
}

func planPathPart(part string) (string, int, bool, error) {
	match := planIndex.FindStringSubmatch(part)
	if match == nil {
		return part, 0, false, nil
	}
	index, err := strconv.Atoi(match[2])
	return match[1], index, true, err
}
