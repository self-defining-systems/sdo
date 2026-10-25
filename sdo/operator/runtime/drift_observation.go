package runtime

import (
	"sdo.dev/operator/sdk"
	"sort"
	"strings"
)

func driftEventKey(event sdk.DriftEvent) string {
	paths := make([]string, 0, len(event.Fields))
	for _, field := range event.Fields {
		paths = append(paths, field.Path)
	}
	sort.Strings(paths)
	return strings.Join([]string{event.Ref.APIVersion, event.Ref.Kind, event.Ref.Namespace, event.Ref.Name, strings.Join(paths, "\x00")}, "\x00")
}

func sameDriftObservation(left, right sdk.DriftEvent) bool {
	if left.Manager != right.Manager || len(left.Fields) != len(right.Fields) {
		return false
	}
	fields := make(map[string]sdk.DriftField, len(left.Fields))
	for _, field := range left.Fields {
		fields[field.Path] = field
	}
	for _, field := range right.Fields {
		previous, found := fields[field.Path]
		if !found || previous.New != field.New || previous.Source != field.Source {
			return false
		}
	}
	return true
}
