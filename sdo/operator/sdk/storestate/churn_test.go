package storestate_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storestate"
)

// postgresChurnFindings runs a postgres store-state detector whose baseline is
// healthyPostgres against current and returns the rule IDs it reports.
func postgresChurnFindings(t *testing.T, current string) map[string]int {
	t.Helper()
	baseline := parsePostgres(t, healthyPostgres)
	baseline.Component = "db"
	baseline.Pod = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "cache-0"}
	c := config(t, baseline)
	c.Component, c.Engine = "db", storestate.EnginePostgres
	d := detector(t, c)
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(current)))
	if err != nil {
		t.Fatal(err)
	}
	rules := map[string]int{}
	for _, f := range findings {
		rules[f.RuleID]++
	}
	return rules
}

// The application's own churn (lock waits under load, objects it creates such
// as a new time partition, a catalog too large for one read) is never a
// catalog change; a held lock or a constraint dropped from a live table is.
func TestCatalogChurnIsNotACatalogChange(t *testing.T) {
	newPartition := "constraint\tpart.events_202611\tfk_events_202611\tf 1234abcd\nconstraint\tpublic.items"
	for name, test := range map[string]struct {
		current string
		want    int
	}{
		"writers waiting":               {strings.Replace(healthyPostgres, "waiting\t0", "waiting\t4", 1), 0},
		"new partition with its fk":     {strings.Replace(healthyPostgres, "constraint\tpublic.items", newPartition, 1), 0},
		"table locked":                  {strings.Replace(healthyPostgres, "table\tpublic.items", "lock\tpublic.items\tAccessExclusiveLock\theld\ntable\tpublic.items", 1), 1},
		"check dropped from live table": {strings.Replace(healthyPostgres, "constraint\tpublic.items\titems_qty_check\tc 064079ee\n", "", 1), 1},
	} {
		if got := postgresChurnFindings(t, test.current)[storestate.RuleCatalogChanged]; got != test.want {
			t.Errorf("%s: catalog-changed findings = %d, want %d", name, got, test.want)
		}
	}
}

func TestPostgresCatalogCutAtItsRowLimitIsUnread(t *testing.T) {
	var rows strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&rows, "constraint\tpart.t%03d\tc%03d\tf 1234abcd\n", i, i)
	}
	cut := strings.Replace(healthyPostgres, "== catalog exampledb\n", "== catalog exampledb\n"+rows.String(), 1)
	f := parsePostgres(t, cut)
	unread := map[storestate.ItemKind]bool{}
	for _, k := range f.Unread {
		unread[k] = true
	}
	if !unread[storestate.KindValidator] || !unread[storestate.KindIndex] || !unread[storestate.KindNamespace] {
		t.Fatalf("unread = %v, want the catalog kinds of a cut catalog", f.Unread)
	}
	if got := postgresChurnFindings(t, cut); len(got) != 0 {
		t.Fatalf("a cut catalog raised %v", got)
	}
}
