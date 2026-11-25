package runtime

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// A change is dated by the readings around it, kept across evaluations and
// across a partition that arrives in the same table group later.
func TestStoreChangesCarryWhenTheyWereFirstObserved(t *testing.T) {
	query := podread.Query{ID: storestate.QueryID(storestate.EngineRedis, "db")}
	redis := func(maxclients string) string {
		return strings.Replace(syntheticRedis, "maxclients\n10000", "maxclients\n"+maxclients, 1)
	}
	at := func(seconds int) time.Time { return detectOnset.Add(time.Duration(seconds) * time.Second) }
	b := NewPodReadBaseline(0)
	read := func(seconds int, stdout string) map[string]podread.Result {
		return map[string]podread.Result{query.ID: {Query: query, Readings: []podread.Reading{{Namespace: "app", Pod: "db-0", ObservedAt: at(seconds), Stdout: stdout}}}}
	}
	b.Promote(read(-300, redis("10000")))
	rows := func(results map[string]podread.Result) []IncidentChange {
		b.attach(results)
		return storeChangeRows(storestate.Changes(sdktest.Snapshot{NamespaceName: "app", PodReads: results}))
	}
	if got := rows(read(-30, redis("10000"))); len(got) != 0 {
		t.Fatalf("no change gave %+v", got)
	}
	rows(read(12, redis("20")))
	// A failed read in between dates nothing and forgets nothing.
	rows(read(42, "garbage"))
	got := rows(read(72, redis("20")))
	if len(got) != 1 || !got[0].FirstObserved.Equal(at(12)) || !got[0].LastWithout.Equal(at(-30)) || !got[0].At.IsZero() {
		t.Fatalf("rows = %+v", got)
	}
	evidence, _ := rowEvidence(got[0], detectOnset)
	if !strings.Contains(evidence.Text, "first observed "+hms(at(12))+", +12s, not yet "+hms(at(-30))) {
		t.Fatalf("evidence = %q", evidence.Text)
	}
	if csv := string(renderChanges(got, detectOnset)); !strings.Contains(csv, at(12).Format(time.RFC3339)+",12,"+at(-30).Format(time.RFC3339)) {
		t.Fatalf("sdo changes:\n%s", csv)
	}
	// A change that went away and came back is dated by its return.
	rows(read(102, redis("10000")))
	if got := rows(read(132, redis("20"))); !got[0].FirstObserved.Equal(at(132)) || !got[0].LastWithout.Equal(at(102)) {
		t.Fatalf("returned: %+v", got)
	}
}

// The kind of object that changed survives the candidates line whole, however
// long the database name is, and routine growth is not unexplained evidence.
func TestStoreEvidenceIsNeverCutInsideAWord(t *testing.T) {
	group := func(value string) *storestate.Item {
		return &storestate.Item{Kind: "validator", Scope: "a_long_database_production.{table group 07 684b516f}", Name: "definitions", Value: value}
	}
	rows := storeChangeRows([]storestate.StoreChanges{{Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{Kind: "Pod", Name: "a-long-database-pod-name-0"}, Baselined: true,
		Changes: []storestate.Change{{Before: group("139 i578:aaaa c362:bbbb t47:cccc"), After: group("139 i578:aaaa c363:ffff t48:ffff")},
			{Before: &storestate.Item{Kind: "namespace", Scope: "appdb", Name: "t", Value: "100"}, After: &storestate.Item{Kind: "namespace", Scope: "appdb", Name: "t", Value: "110"}}}}})
	evidence, _ := rowEvidence(rows[0], detectOnset)
	if !strings.Contains(evidence.Text, "+1 trigger, +1 constraint in a_long_database_production table group 07 of 16: 47 triggers, 362 constraints → 48 triggers, 363 constraints") || strings.Contains(evidence.Text, "…") {
		t.Fatalf("evidence = %q", evidence.Text)
	}
	if growth, _ := rowEvidence(rows[1], detectOnset); growth.Unexplained || !growth.Routine {
		t.Fatalf("growth = %+v", growth)
	}
}

func TestStoreChangeRowsReadTheCatalogAndFlagNoise(t *testing.T) {
	group := func(value string) *storestate.Item {
		return &storestate.Item{Kind: "validator", Scope: "appdb.{table group 07 684b516f}", Name: "definitions", Value: value}
	}
	stores := []storestate.StoreChanges{{
		Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{Kind: "Pod", Name: "db-0"}, ObservedAt: detectOnset, Baselined: true,
		Changes: []storestate.Change{
			{Before: group("137 89f0cb69 i412 c88 t14"), After: group("137 f4d76c7e i412 c88 t15")},
			{Before: &storestate.Item{Kind: "key-pattern", Scope: "db0", Name: "lease:*", Value: "10"}, After: &storestate.Item{Kind: "key-pattern", Scope: "db0", Name: "lease:*", Value: "99"}, Noisy: true},
		},
	}}
	rows := storeChangeRows(stores)
	if len(rows) != 2 || !strings.Contains(rows[0].Key, "+1 trigger") || strings.Contains(rows[0].Key+rows[0].Before+rows[0].After, "684b516f") || !rows[1].Noisy {
		t.Fatalf("rows = %+v", rows)
	}
	if !rows[0].At.IsZero() || !rows[0].ReadAt.Equal(detectOnset) {
		t.Fatalf("a store diff is untimed: it carries only the time it was read: %+v", rows[0])
	}
	evidence, _ := rowEvidence(rows[0], detectOnset)
	if !strings.Contains(evidence.Text, "+1 trigger") || !strings.Contains(evidence.Text, "read 07:28:09Z, change untimed") || strings.Contains(evidence.Text, "+0s") {
		t.Fatalf("evidence line = %q", evidence.Text)
	}
	csv := string(renderChanges(rows, detectOnset))
	if !strings.Contains(csv, "lease:*") || !strings.Contains(csv, ",true,2026-") {
		t.Fatalf("sdo changes keeps the noisy row:\n%s", csv)
	}
}
