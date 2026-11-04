package runtime

import (
	"slices"
	"testing"

	"sdo.dev/operator/memory/validate"
)

// The memory check rejects programs that look up a stripped annotation in a
// source comparison; it must strip-list exactly what comparisons strip.
func TestValidatorKnowsEveryStrippedBookkeepingAnnotation(t *testing.T) {
	var names []string
	for _, policy := range sourceBookkeepingAnnotations {
		names = append(names, policy.name)
	}
	want := slices.Clone(validate.SourceBookkeepingAnnotations)
	slices.Sort(names)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("validate.SourceBookkeepingAnnotations = %v, comparison policy strips %v", want, names)
	}
}
