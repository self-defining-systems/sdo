package storestate_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// groupedPostgres is a postgres read whose catalog was too large to list item
// by item: each line digests one group of tables (by a hash of the table name)
// as "<group> <digest of the table names>" and "<tables> <digest of their
// constraints, indexes and triggers>". Held locks are listed in full.
func groupedPostgres(groups map[string]string, locks ...string) string {
	var b strings.Builder
	for _, l := range locks {
		b.WriteString(l + "\n")
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "group\t%s\t\t%s\n", k, groups[k])
	}
	b.WriteString("grant\tappuser\t\tconnect=true select=2 insert=2 update=2 delete=2\n")
	return strings.Replace(healthyPostgres, healthyPostgres[strings.Index(healthyPostgres, "== catalog exampledb\n"):strings.Index(healthyPostgres, "== catalog postgres\n")], "== catalog exampledb\n"+b.String(), 1)
}

func largeSchemaGroups() map[string]string {
	groups := map[string]string{}
	for g := 0; g < 16; g++ {
		groups[fmt.Sprintf("%02d %08x", g, 0xa0000000+g)] = fmt.Sprintf("%d %08x", 40+g, 0xb0000000+g)
	}
	return groups
}

// A catalog too large to list (hundreds of tables, a thousand constraints) is
// read as group digests, never cut: a constraint added to or dropped from a
// live table changes its group's digest and is a catalog change; a new table
// (a time partition) moves its group to a new table digest and is churn; a
// held lock is still read.
func TestPostgresLargeCatalogIsReadAsGroupDigests(t *testing.T) {
	healthy := groupedPostgres(largeSchemaGroups())
	f := parsePostgres(t, healthy)
	if slices.Contains(f.Unread, storestate.KindValidator) {
		t.Fatalf("a grouped catalog leaves validators unread: %v", f.Unread)
	}
	if !slices.Contains(f.Unread, storestate.KindNamespace) || !slices.Contains(f.Unread, storestate.KindIndex) {
		t.Fatalf("a grouped catalog claims per-table items: %v", f.Unread)
	}
	_, detect := storeDetector(t, storestate.EnginePostgres, healthy)

	constraintAdded := largeSchemaGroups()
	constraintAdded["05 a0000005"] = "45 c0ffee00"
	findings := detect(groupedPostgres(constraintAdded))
	if ruleSet(findings)[storestate.RuleCatalogChanged] != 1 || !strings.Contains(findings[0].Summary, "05") {
		t.Fatalf("a constraint added to a live table: findings = %+v", findings)
	}

	newPartition := largeSchemaGroups()
	delete(newPartition, "05 a0000005")
	newPartition["05 d0000005"] = "46 c0ffee00"
	if findings := detect(groupedPostgres(newPartition)); len(findings) != 0 {
		t.Fatalf("a new table raised %+v", findings)
	}

	locked := detect(groupedPostgres(largeSchemaGroups(), "lock\tpublic.issues\tAccessExclusiveLock\theld"))
	if ruleSet(locked)[storestate.RuleCatalogChanged] != 1 {
		t.Fatalf("a held lock on a large schema: findings = %+v", locked)
	}
}

// Switching between the itemized and the grouped read (a schema that grows
// past the itemized size, or shrinks below it) changes nothing that is judged.
func TestPostgresCatalogReadModeSwitchIsNotAChange(t *testing.T) {
	grouped := groupedPostgres(largeSchemaGroups())
	_, detect := storeDetector(t, storestate.EnginePostgres, healthyPostgres)
	if findings := detect(grouped); len(findings) != 0 {
		t.Fatalf("itemized -> grouped raised %+v", findings)
	}
	_, detect = storeDetector(t, storestate.EnginePostgres, grouped)
	if findings := detect(healthyPostgres); len(findings) != 0 {
		t.Fatalf("grouped -> itemized raised %+v", findings)
	}
}

// The postgres catalog query bounds its own output (itemized below a size,
// grouped above) instead of cutting rows, and digests only what the
// application declared: a constraint cloned onto a table from a partitioned
// parent is excluded (it changes whenever the parent gains a partition).
func TestPostgresCatalogQueryGroupsInsteadOfCutting(t *testing.T) {
	argv := strings.Join(adapterFor(t, storestate.EnginePostgres).ReadCommand(storestate.Endpoint{Port: 5432}), " ")
	// Grants on every table read as "all", so a new partition the role owns
	// is not a principal change.
	for _, want := range []string{"'group'", "conparentid=0", "md5(", "THEN 'all'"} {
		if !strings.Contains(argv, want) {
			t.Errorf("postgres read lacks %q", want)
		}
	}
}

// mysql: a schema with more indexes than the read lists item by item is read
// as group digests; locks are read before the catalog so a large catalog
// never hides them.
func TestMySQLLargeIndexCatalogIsReadAsGroupDigests(t *testing.T) {
	grouped := func(digest, extraMDL string) string {
		return strings.Replace(healthyMySQL, "== indexes\nshop\titems\tPRIMARY\tunique b80bb774\n",
			extraMDL+"== indexes\nshop\t\t03 a0000003\t12 "+digest+"\nshop\t\t07 a0000007\t9 b0000007\n", 1)
	}
	healthy := grouped("b0000003", "")
	f := parseAs(t, storestate.EngineMySQL, healthy)
	if !slices.Contains(f.Unread, storestate.KindIndex) {
		t.Fatalf("a grouped index read claims per-index items: %v", f.Unread)
	}
	_, detect := storeDetector(t, storestate.EngineMySQL, healthy)
	if findings := detect(grouped("c0ffee00", "")); ruleSet(findings)[storestate.RuleCatalogChanged] != 1 {
		t.Fatalf("an index changed on a large schema: findings = %+v", findings)
	}
	if findings := detect(grouped("b0000003", "TABLE\tshop\titems\tEXCLUSIVE\n")); ruleSet(findings)[storestate.RuleCatalogChanged] != 1 {
		t.Fatalf("a metadata lock on a large schema: findings = %+v", findings)
	}
	if !strings.Contains(strings.Join(adapterFor(t, storestate.EngineMySQL).ReadCommand(storestate.Endpoint{Port: 3306}), " "), "== mdl';q \"$8\";echo '== indexes'") {
		t.Error("mysql read does not read locks before the catalog")
	}
}

// mongodb: a catalog larger than the output budget is read as group digests
// of each database's collections (validators and indexes), not cut.
func TestMongoLargeCatalogIsReadAsGroupDigests(t *testing.T) {
	grouped := func(digest string) string {
		return strings.Replace(healthyMongo, "coll\texampledb\titems\t12\t-\nindex\texampledb\titems\tsku_1\tsku:1 unique\ncoll\texampledb\torders\t3\tmoderate/warn\nindex\texampledb\torders\tplaced_1\tplaced:-1 ttl=3600\n",
			"group\texampledb\t03 a0000003\t12 "+digest+"\n", 1)
	}
	healthy := grouped("b0000003")
	f := parseMongo(t, healthy)
	if !slices.Contains(f.Unread, storestate.KindNamespace) || !slices.Contains(f.Unread, storestate.KindValidator) {
		t.Fatalf("a grouped catalog claims per-collection items: %v", f.Unread)
	}
	_, detect := storeDetector(t, storestate.EngineMongoDB, healthy)
	if findings := detect(grouped("c0ffee00")); ruleSet(findings)[storestate.RuleCatalogChanged] != 1 {
		t.Fatalf("a validator changed on a large catalog: findings = %+v", findings)
	}
	if findings := detect(healthyMongo); len(findings) != 0 {
		t.Fatalf("grouped -> itemized raised %+v", findings)
	}
	if !strings.Contains(strings.Join(adapterFor(t, storestate.EngineMongoDB).ReadCommand(storestate.Endpoint{Port: 27017}), ""), `"group"`) {
		t.Error("mongodb read never groups its catalog")
	}
}
