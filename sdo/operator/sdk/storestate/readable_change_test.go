package storestate_test

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// A changed group is described by what changed (a trigger added), never by
// its table and definition hashes.
func TestDescribeNamesTheKindOfObjectAGroupGained(t *testing.T) {
	item := func(value string) *storestate.Item {
		return &storestate.Item{Kind: storestate.KindValidator, Scope: "appdb.{table group 07 684b516f}", Name: "definitions", Value: value}
	}
	key, before, after := storestate.Change{Before: item("137 89f0cb69 i412 c88 t14"), After: item("137 f4d76c7e i412 c88 t15")}.Describe()
	if !strings.Contains(key, "+1 trigger") || !strings.Contains(key, "appdb") || before != "137 tables, 412 indexes, 88 constraints, 14 triggers" || !strings.Contains(after, "15 triggers") {
		t.Fatalf("described %q, %q, %q", key, before, after)
	}
	for _, text := range []string{key, before, after} {
		if strings.Contains(text, "684b516f") || strings.Contains(text, "f4d76c7e") {
			t.Fatalf("a hash reached the description: %q", text)
		}
	}
	key, _, _ = storestate.Change{Before: item("137 89f0cb69 i412 c88 t14"), After: item("137 f4d76c7e i412 c88 t14")}.Describe()
	if !strings.Contains(key, "changed in place") {
		t.Fatalf("same counts: %q", key)
	}
	key, before, _ = storestate.Change{Before: item("45 c0ffee00"), After: item("46 c0ffee01")}.Describe()
	if strings.Contains(key, "c0ffee") || !strings.Contains(key, "definition changed") || before != "" {
		t.Fatalf("a value without counts: %q, %q", key, before)
	}
	plain := storestate.Item{Kind: storestate.KindSetting, Name: "maxclients", Value: "20"}
	if key, _, after := (storestate.Change{After: &plain}).Describe(); key != "setting maxclients" || after != "20" {
		t.Fatalf("plain item: %q %q", key, after)
	}
}

// An item that changed in most healthy readings (a key count) is flagged
// noisy from that history; one that held still is not.
func TestChangesFlagItemsThatMovedWhileHealthy(t *testing.T) {
	id := storestate.QueryID(storestate.EngineRedis, "kv")
	at := func(i int, out string) podread.Reading {
		return podread.Reading{Namespace: "app", Pod: "kv-0", ObservedAt: time.Unix(int64(100+30*i), 0).UTC(), Stdout: out}
	}
	var history []podread.Reading
	for i := range 6 {
		history = append(history, at(i, redisKeys("lease:*", 10+i, "user:*", 40)))
	}
	current := at(9, redisKeys("lease:*", 99, "user:*", 3))
	result := podread.Result{Query: podread.Query{ID: id}, Readings: []podread.Reading{current}, Baseline: history[5:], History: history}
	noisy := map[string]bool{}
	for _, store := range storestate.Changes(snapshotOf(id, result)) {
		for _, change := range store.Changes {
			noisy[change.Key().Name] = change.Noisy
		}
	}
	if len(noisy) != 2 || !noisy["lease:*"] || noisy["user:*"] {
		t.Fatalf("noisy = %v", noisy)
	}
}
