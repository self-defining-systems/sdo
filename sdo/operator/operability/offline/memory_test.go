package offline_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
)

func finding(rule, path, msg string) offline.Finding {
	return offline.Finding{Rule: rule, Path: path, Line: 3, Message: msg}
}

// The validator's rule IDs are stable and classified by family; a new family
// that nobody classified lands in O5 (memory consistency), never nowhere.
func TestItemOfRules(t *testing.T) {
	for rule, item := range map[string]operability.ItemID{
		"map.uncovered-operation":      operability.O3,
		"map.schema":                   operability.O3,
		validate.RuleBootstrapClosed:   operability.O6,
		validate.RuleVerifiedSource:    operability.O6,
		"ownership.denied":             operability.O6,
		"ownership.protected-detector": operability.O6,
		"id.duplicate":                 operability.O5,
		"detector.vet":                 operability.O5,
		"deployment.invalid":           operability.O5,
		"patch.invalid":                operability.O5,
		"layout.unknown-path":          operability.O5,
		"brand.new-family":             operability.O5,
		"":                             operability.O5,
	} {
		if got := offline.ItemOf(rule); got != item {
			t.Errorf("ItemOf(%q) = %s, want %s", rule, got, item)
		}
	}
}

func TestCheckMemoryCleanRun(t *testing.T) {
	got := byID(t, offline.CheckMemory(offline.Memory{OwnershipChecked: true}))
	want(t, got, "O3.coverage", operability.StatusPass, "")
	want(t, got, "O5.memory", operability.StatusPass, "")
	want(t, got, "O6.ownership", operability.StatusPass, "")
	if len(got) != 3 {
		t.Fatalf("clean run produced %v", got)
	}
}

func TestOwnershipUnchecked(t *testing.T) {
	got := byID(t, offline.CheckMemory(offline.Memory{}))
	want(t, got, "O6.ownership", operability.StatusSkipped, "")
}

func TestCheckMemoryClassifiesByFamily(t *testing.T) {
	got := byID(t, offline.CheckMemory(offline.Memory{OwnershipChecked: true, Blocking: []offline.Finding{
		finding("map.uncovered-operation", ".sdo/arch.md", "operation checkout is not covered"),
		finding("map.unknown-name", ".sdo/diagnostics/a.go", "no such name"),
		finding("id.duplicate", ".sdo/diagnostics/b.go", "duplicate id"),
		finding("detector.vet", ".sdo/diagnostics/b.go", "vet failed"),
		finding("ownership.denied", ".sdo/goal.md", "actor may not edit goal"),
	}}))
	c := want(t, got, "O3.coverage", operability.StatusFail, operability.CauseCoverage)
	if !strings.Contains(c.Detail, "[map.uncovered-operation]") || !strings.Contains(c.Detail, "[map.unknown-name]") {
		t.Fatalf("coverage detail = %q", c.Detail)
	}
	want(t, got, "O5.memory.id", operability.StatusFail, operability.CauseMemoryInvalid)
	c = want(t, got, "O5.memory.detector", operability.StatusFail, operability.CauseMemoryInvalid)
	if len(c.Paths) != 1 || c.Paths[0] != ".sdo/diagnostics/b.go" {
		t.Fatalf("paths = %v", c.Paths)
	}
	want(t, got, "O6.ownership", operability.StatusFail, operability.CauseScope)
	if _, clean := got["O5.memory"]; clean {
		t.Fatal("O5 passed beside failing families")
	}
}

func TestNotesNeverBlockAndAreSplitByFamily(t *testing.T) {
	checks := offline.CheckMemory(offline.Memory{OwnershipChecked: true, Notes: []offline.Finding{
		finding("map.unexercised-edge", ".sdo/arch.md", "edge never exercised"),
		finding("reflection.sdk-gap-unavailable", ".sdo/x", "gap"),
	}})
	got := byID(t, checks)
	want(t, got, "O3.coverage-gaps", operability.StatusNote, "")
	want(t, got, "O5.gaps", operability.StatusNote, "")
	r, err := operability.New(operability.StageComplete, operability.Subject{
		AcceptedCommit: "c", BaseTree: "b", EffectiveTree: "e", PatchesDigest: "p", RenderDigest: "r", EnvDigest: "v", DiagnosticsTree: "d",
	}).With(checks...)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if d := r.Decide(operability.O3, operability.O5, operability.O6); d.Kind != operability.Accepted {
		t.Fatalf("notes blocked: %+v", d)
	}
}

func TestCheckMemoryBoundsDetailAndPaths(t *testing.T) {
	var fs []offline.Finding
	for i := 0; i < 40; i++ {
		fs = append(fs, offline.Finding{Rule: "id.pattern", Path: strings.Repeat("d", 30) + string(rune('a'+i%26)) + string(rune('a'+i/26)), Message: strings.Repeat("m", 300)})
	}
	c := byID(t, offline.CheckMemory(offline.Memory{Blocking: fs}))["O5.memory.id"]
	if len(c.Detail) > operability.MaxDetailBytes || len(c.Paths) != 10 {
		t.Fatalf("detail %d bytes, %d paths", len(c.Detail), len(c.Paths))
	}
}

func TestItems(t *testing.T) {
	if got := offline.Items(operability.StageDeployment); len(got) != 3 {
		t.Fatalf("deployment items = %v", got)
	}
	got := offline.Items(operability.StageComplete)
	for _, id := range got {
		if id == operability.O4 {
			t.Fatal("O4 needs a cluster")
		}
	}
	if len(got) != 5 {
		t.Fatalf("complete items = %v", got)
	}
}
