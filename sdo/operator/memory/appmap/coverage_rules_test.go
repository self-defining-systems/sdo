package appmap

import (
	"strings"
	"testing"
)

func ruleMessages(findings []Finding, rule string) []string {
	var out []string
	for _, f := range findings {
		if f.Rule == rule {
			out = append(out, f.Message)
		}
	}
	return out
}

func TestUnprobedEntryIsRejected(t *testing.T) {
	inv := ParseInventory(exposedInventory)
	m := mustParse(t, strings.Replace(exposedYAML, "services: [front], holds_data: false}", "services: [front], holds_data: false, unprobed: login needs a device}", 1))
	got := ruleMessages(m.Check(inv, scrapeWorkloads), RuleUnprobedEntry)
	if len(got) != 1 || !strings.Contains(got[0], "component front owns user entry Service front") {
		t.Fatalf("map.unprobed-entry = %v", got)
	}
	// An internal component may still be unprobed.
	m = mustParse(t, strings.Replace(exposedYAML, "services: [api], holds_data: false}", "services: [api], holds_data: false, unprobed: internal job}", 1))
	if got := ruleMessages(m.Check(inv, scrapeWorkloads), RuleUnprobedEntry); len(got) != 0 {
		t.Fatalf("an internal unprobed component raised %v", got)
	}
}

func TestUnprobedShareBlocksMoreThanHalfOfTheUserPath(t *testing.T) {
	inv := ParseInventory(exposedInventory)
	// front, api and db are on the user path; front is the entry.
	two := mustParse(t, strings.Replace(strings.Replace(exposedYAML,
		"services: [api], holds_data: false}", "services: [api], holds_data: false, unprobed: internal job}", 1),
		"services: [db], holds_data: true}", "services: [db], holds_data: true, unprobed: no synthetic route}", 1))
	got := ruleMessages(two.Check(inv, scrapeWorkloads), RuleUnprobedShare)
	if len(got) != 1 || !strings.Contains(got[0], "2 of 3 user-path components are unprobed") {
		t.Fatalf("map.unprobed-share = %v", got)
	}
	one := mustParse(t, strings.Replace(exposedYAML, "services: [api], holds_data: false}", "services: [api], holds_data: false, unprobed: internal job}", 1))
	if got := ruleMessages(one.Check(inv, scrapeWorkloads), RuleUnprobedShare); len(got) != 0 {
		t.Fatalf("1 of 3 raised %v", got)
	}
}

func recordsScenarios(edit func(*Scenario)) []Scenario {
	scenarios := []Scenario{
		{ID: "call-all", SideEffect: "read", DependsOn: []string{"web", "api", "db"}},
		{ID: "write-item", SideEffect: "idempotent-write", DependsOn: []string{"web", "api", "db"}},
		{ID: "items-later", SideEffect: "read", DependsOn: []string{"web", "api", "db"}, SetupSteps: 1},
	}
	if edit != nil {
		edit(&scenarios[0])
	}
	return scenarios
}

func TestRecordsUnreadRule(t *testing.T) {
	m := mustParse(t, shopYAML)
	marked := func(step ExistingRecordsStep) func(*Scenario) {
		return func(s *Scenario) { s.ExistingRecords = []ExistingRecordsStep{step} }
	}
	for name, tc := range map[string]struct {
		edit    func(*Scenario)
		blocked bool
		mention string
	}{
		"no marked step":     {nil, true, "no gated probe has a step that reads existing records"},
		"marked and checked": {marked(ExistingRecordsStep{Name: "list", ChecksContent: true}), false, ""},
		"no content check":   {marked(ExistingRecordsStep{Name: "list"}), true, "no valid member pattern"},
		"templates own key":  {marked(ExistingRecordsStep{Name: "open", ChecksContent: true, OwnKeys: []string{"own_id"}}), true, "templates own_id"},
		"known broken probe": {func(s *Scenario) {
			s.KnownBroken = true
			marked(ExistingRecordsStep{Name: "list", ChecksContent: true})(s)
		}, true, "no gated probe"},
	} {
		t.Run(name, func(t *testing.T) {
			got := ruleMessages(m.CheckScenarios(recordsScenarios(tc.edit)), RuleRecordsUnread)
			if tc.blocked != (len(got) == 1) || (tc.mention != "" && !strings.Contains(strings.Join(got, ""), tc.mention)) {
				t.Fatalf("map.records-unread = %v", got)
			}
		})
	}
}

func TestNoUserRecordsIsTheOnlyEscapeAndIsANote(t *testing.T) {
	m := mustParse(t, shopYAML+"no_user_records: users keep nothing in this app\n")
	findings := m.CheckScenarios(recordsScenarios(nil))
	if len(ruleMessages(findings, RuleRecordsUnread)) != 0 {
		t.Fatalf("the escape did not satisfy the rule: %+v", findings)
	}
	if got := ruleMessages(findings, RuleNoUserRecords); len(got) != 1 || !strings.Contains(got[0], "users keep nothing") {
		t.Fatalf("no note: %+v", findings)
	}
}

func TestRecordsUnreadSkipsAMapWithoutData(t *testing.T) {
	m := mustParse(t, strings.ReplaceAll(shopYAML, "holds_data: true", "holds_data: false"))
	if got := ruleMessages(m.CheckScenarios(recordsScenarios(nil)), RuleRecordsUnread); len(got) != 0 {
		t.Fatalf("a map with no data raised %v", got)
	}
}
