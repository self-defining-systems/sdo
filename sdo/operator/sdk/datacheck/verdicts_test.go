package datacheck_test

import (
	"testing"

	"sdo.dev/operator/sdk/datacheck"
)

const brokenItems = "R\t1\t{\"item_id\":1,\"owner_legacy\":{\"user_id\":7},\"title\":\"a\",\"tags\":[],\"stock\":1}\nE\n"

func TestVerdictsRejudgeAcceptedChecksOverFrozenReads(t *testing.T) {
	d := detector(t, "items", itemShape)
	snapshot := view(t, d, brokenItems)
	if got := datacheck.Verdicts(snapshot); len(got) != 1 || got[0].Error == "" || got[0].Firing() {
		t.Fatalf("a view whose accepted checks are unavailable gives one error verdict, got %+v", got)
	}
	snapshot.AcceptedDataChecks = map[string]string{"items": itemShape, "groups": groupReference}
	got := datacheck.Verdicts(snapshot)
	if len(got) != 2 || got[0].ID != "groups" || got[1].ID != "items" {
		t.Fatalf("verdicts = %+v", got)
	}
	groups, items := got[0], got[1]
	if groups.Error == "" || groups.Firing() {
		t.Fatalf("a check whose reads are missing must report an error, never a clear: %+v", groups)
	}
	if groups.Component != "group-cache" || groups.Lookup != "item-store" {
		t.Fatalf("groups names = %+v", groups)
	}
	if items.Error != "" || !items.Firing() || items.Component != "item-store" || items.Findings[0].RuleID != datacheck.RuleShape {
		t.Fatalf("items verdict = %+v", items)
	}
	healthy := view(t, d, healthyItems)
	healthy.AcceptedDataChecks = map[string]string{"items": itemShape}
	if got := datacheck.Verdicts(healthy); len(got) != 1 || got[0].Firing() || got[0].Error != "" {
		t.Fatalf("healthy verdicts = %+v", got)
	}
}
