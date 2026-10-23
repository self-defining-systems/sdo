package runtime

import (
	"encoding/json"
	"sdo.dev/operator/sdk/program"
	"sort"
)

// ResponderMenuConfig enables a convenience catalog over the ordinary executor.
type ResponderMenuConfig struct{ Plans []ResponderMenuPlan }
type ResponderMenuPlan struct {
	ID        string
	ProgramID string
	Bindings  program.Bindings
	Plan      program.Plan
}

func (c *ResponderMenuConfig) valid() bool { return c != nil }
func menuChangedFields(patch []byte) []string {
	var values map[string]any
	if json.Unmarshal(patch, &values) != nil {
		return nil
	}
	var fields []string
	var visit func(map[string]any, string)
	visit = func(values map[string]any, prefix string) {
		for key, value := range values {
			if prefix == "" && key == "metadata" {
				continue
			}
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			if nested, ok := value.(map[string]any); ok {
				visit(nested, path)
			} else {
				fields = append(fields, path)
			}
		}
	}
	visit(values, "")
	sort.Strings(fields)
	return fields
}
