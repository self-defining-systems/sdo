package runtime

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storestate"
)

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
