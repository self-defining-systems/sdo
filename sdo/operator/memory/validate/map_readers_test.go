package validate

import (
	"slices"
	"strings"
	"testing"
)

func TestMapReadersNote(t *testing.T) {
	yaml := strings.Replace(mapYAML, "services: [db], holds_data: true}", "services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}", 1)
	blocking, notes := staticMapCheck(t, mapTree(t, &yaml), HealthJudge)
	if slices.Contains(mapRules(blocking), RuleMapReaders) {
		t.Fatalf("the reader note blocked: %v", blocking)
	}
	if !slices.ContainsFunc(notes, func(d Diagnostic) bool { return d.Rule == RuleMapReaders && strings.Contains(d.Message, "db") }) {
		t.Fatalf("no map.readers note: %v", notes)
	}
}
