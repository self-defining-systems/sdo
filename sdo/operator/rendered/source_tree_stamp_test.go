package rendered

import (
	"strings"
	"testing"
)

func TestSourceTreeStampRoundTripsBothHashLengths(t *testing.T) {
	for _, tree := range []string{strings.Repeat("a", 40), strings.Repeat("b", 64)} {
		labels := map[string]any{}
		for k, v := range SourceTreeLabels(tree) {
			if len(v) > 63 {
				t.Fatalf("label %s value has %d characters", k, len(v))
			}
			labels[k] = v
		}
		if got := StampedTree(map[string]any{"labels": labels}); got != tree {
			t.Errorf("round trip of %d-character tree = %q", len(tree), got)
		}
	}
}

func TestStampedTreeReadsLegacyAnnotation(t *testing.T) {
	got := StampedTree(map[string]any{"annotations": map[string]any{SourceTreeKey: "abc"}})
	if got != "abc" {
		t.Errorf("legacy annotation read as %q", got)
	}
}
