package appmap

import (
	"strings"
	"testing"
)

// withData replaces the db and cache component lines of shopYAML.
func withData(db, cache string) string {
	text := strings.Replace(shopYAML, "  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true}", db, 1)
	return strings.Replace(text, "  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false}", cache, 1)
}

const (
	dbAuthoritative = "  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true, data: {engine: postgres, role: authoritative}}"
	cacheDerived    = "  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false, data: {engine: redis, role: derived, derived_from: [db]}}"
)

func dataFindings(t *testing.T, text string) []Finding {
	t.Helper()
	var out []Finding
	for _, f := range mustParse(t, text).Check(ParseInventory(shopInventory), shopWorkloads) {
		if f.Rule == RuleSchema || f.Rule == RuleUnknownName {
			out = append(out, f)
		}
	}
	return out
}

func TestDataRolesAcceptAValidMap(t *testing.T) {
	text := withData(dbAuthoritative, cacheDerived)
	if got := dataFindings(t, text); len(got) != 0 {
		t.Fatalf("findings = %+v", got)
	}
	for _, comp := range mustParse(t, text).Components {
		if comp.Name == "cache" && (comp.Data == nil || comp.Data.Engine != EngineRedis || comp.Data.Role != RoleDerived || len(comp.Data.DerivedFrom) != 1) {
			t.Fatalf("cache data = %+v", comp.Data)
		}
	}
}

func TestMapWithoutDataRolesStillParses(t *testing.T) {
	if got := dataFindings(t, shopYAML); len(got) != 0 {
		t.Fatalf("a map without data roles: %+v", got)
	}
}

func TestDataRolesAreValidated(t *testing.T) {
	for name, test := range map[string]struct {
		db, cache, mention string
	}{
		"unknown engine": {
			strings.Replace(dbAuthoritative, "engine: postgres", "engine: cassandra-ish", 1), cacheDerived, "engine",
		},
		"missing engine": {
			strings.Replace(dbAuthoritative, "engine: postgres, ", "", 1), cacheDerived, "engine",
		},
		"unknown role": {
			strings.Replace(dbAuthoritative, "role: authoritative", "role: primary", 1), cacheDerived, "role",
		},
		"derived from an unknown component": {
			dbAuthoritative, strings.Replace(cacheDerived, "derived_from: [db]", "derived_from: [nowhere]", 1), "nowhere",
		},
		"derived from a component that is not authoritative": {
			strings.Replace(dbAuthoritative, ", data: {engine: postgres, role: authoritative}", "", 1), cacheDerived, "authoritative",
		},
		"derived with no source": {
			dbAuthoritative, strings.Replace(cacheDerived, ", derived_from: [db]", "", 1), "derived_from",
		},
		"authoritative with derived_from": {
			strings.Replace(dbAuthoritative, "role: authoritative", "role: authoritative, derived_from: [cache]", 1), cacheDerived, "derived_from",
		},
		"derived that holds data": {
			dbAuthoritative, strings.Replace(cacheDerived, "holds_data: false", "holds_data: true", 1), "holds_data",
		},
		"derived from itself": {
			dbAuthoritative, strings.Replace(cacheDerived, "derived_from: [db]", "derived_from: [db, cache]", 1), "itself",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := dataFindings(t, withData(test.db, test.cache))
			found := false
			for _, f := range got {
				found = found || strings.Contains(f.Message, test.mention)
			}
			if !found {
				t.Fatalf("no finding mentioning %q: %+v", test.mention, got)
			}
		})
	}
}

func TestDataRolesRejectUnknownFields(t *testing.T) {
	if _, err := Parse(withData(strings.Replace(dbAuthoritative, "role: authoritative", "role: authoritative, flavor: x", 1), cacheDerived)); err == nil {
		t.Fatal("an unknown data field parsed")
	}
}
