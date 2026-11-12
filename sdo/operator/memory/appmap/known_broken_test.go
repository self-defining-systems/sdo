package appmap

import (
	"strings"
	"testing"
)

// brokenShop adds a review operation that is broken at baseline in the
// application itself: its only probe is a known-broken call scenario.
func brokenShop(probes string) string {
	return strings.Replace(shopYAML, "probes:\n", `  - id: review
    writes: true
    entry: {service: web, port: 80, method: POST, path: /review}
    path: [web, api, db]
probes:
`+probes, 1)
}

func brokenWorkloads(knownBroken ...string) []Workload {
	return []Workload{
		{Name: "probe-call", Scenarios: []string{"call-all", "call-review"}, KnownBroken: knownBroken},
		{Name: "probe-write", Scenarios: []string{"write-item"}},
		{Name: "probe-read-later", Scenarios: []string{"items-later"}},
	}
}

// An operation whose only probe is declared known-broken is covered: it needs
// no gating call or write-read probe, and its writes owe no read-later probe.
func TestKnownBrokenProbeCoversItsOperation(t *testing.T) {
	m := mustParse(t, brokenShop("  - {scenario: call-review, kind: call, operations: [review]}\n"))
	var findings []Finding
	for _, f := range m.Check(ParseInventory(shopInventory), brokenWorkloads("call-review")) {
		if f.Rule != RuleCoverage {
			findings = append(findings, f)
		}
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %v, want none", findings)
	}
	// Without the declaration the same map still owes the write-read probe.
	if findings := m.Check(ParseInventory(shopInventory), brokenWorkloads()); !has(findings, RuleUncoveredWrite, "review") {
		t.Fatalf("findings = %v, want review uncovered by write-read", findings)
	}
}

// A known-broken probe whose every operation a gated probe already covers
// hides a failure of working operations: it is refused.
func TestKnownBrokenProbeOverGatedOperationsIsRefused(t *testing.T) {
	m := mustParse(t, brokenShop("  - {scenario: call-review, kind: call, operations: [list-items]}\n"))
	findings := m.Check(ParseInventory(shopInventory), brokenWorkloads("call-review"))
	if !has(findings, RuleKnownBroken, "call-review") {
		t.Fatalf("findings = %v, want %s naming call-review", findings, RuleKnownBroken)
	}
}

// Declaring most operations broken would turn detection off: known-broken
// operations may be at most half of the map's operations.
func TestKnownBrokenOperationsAreAtMostHalf(t *testing.T) {
	text := strings.Replace(brokenShop("  - {scenario: call-review, kind: call, operations: [review, add-item]}\n"),
		"  - {scenario: call-all, kind: call, operations: [list-items, add-item]}\n  - {scenario: write-item, kind: write-read, operations: [add-item, list-items]}\n",
		"  - {scenario: call-all, kind: call, operations: [list-items]}\n", 1)
	m := mustParse(t, text)
	workloads := []Workload{
		{Name: "probe-call", Scenarios: []string{"call-all", "call-review"}, KnownBroken: []string{"call-review"}},
		{Name: "probe-read-later", Scenarios: []string{"items-later"}},
	}
	findings := m.Check(ParseInventory(shopInventory), workloads)
	if !has(findings, RuleKnownBroken, "2 of 3") {
		t.Fatalf("findings = %v, want %s for 2 of 3 operations", findings, RuleKnownBroken)
	}
}
