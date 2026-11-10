package podread_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/podread"
)

func lookup() podread.Query {
	return podread.Query{
		ID: "lookup", Target: podread.Target{Image: "synthetic-index"}, Interval: time.Minute,
		Args: []string{"index-cli", "get", podread.KeysPlaceholder},
		Keys: &podread.Keys{From: "policy", Prefix: "K\t", Max: 4},
	}
}

func TestLookupDeclarationsAreBounded(t *testing.T) {
	for name, change := range map[string]func(*podread.Query){
		"own source":           func(q *podread.Query) { q.Keys.From = q.ID },
		"no source":            func(q *podread.Query) { q.Keys.From = "" },
		"no prefix":            func(q *podread.Query) { q.Keys.Prefix = "" },
		"multi-line prefix":    func(q *podread.Query) { q.Keys.Prefix = "K\n" },
		"no keys":              func(q *podread.Query) { q.Keys.Max = 0 },
		"too many keys":        func(q *podread.Query) { q.Keys.Max = podread.MaxKeys + 1 },
		"no placeholder":       func(q *podread.Query) { q.Args = []string{"index-cli", "get"} },
		"placeholder unwanted": func(q *podread.Query) { q.Keys = nil },
	} {
		t.Run(name, func(t *testing.T) {
			q := lookup()
			keys := *q.Keys
			q.Keys = &keys
			change(&q)
			if err := podread.ValidateQueries([]podread.Query{query(), q}); err == nil {
				t.Fatal("ValidateQueries accepted the lookup")
			}
		})
	}
	if err := podread.ValidateQueries([]podread.Query{query(), lookup()}); err != nil {
		t.Fatal(err)
	}
	other := lookup()
	other.Interval = 2 * time.Minute
	if err := podread.ValidateQueries([]podread.Query{query(), other}); err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("a lookup runs with its source, so the intervals must agree: %v", err)
	}
	if err := podread.ValidateQueries([]podread.Query{lookup()}); err == nil {
		t.Fatal("a lookup of an undeclared source was accepted")
	}
	chained := lookup()
	chained.ID, chained.Keys = "second", &podread.Keys{From: "lookup", Prefix: "K\t", Max: 1}
	if err := podread.ValidateQueries([]podread.Query{query(), lookup(), chained}); err == nil {
		t.Fatal("a lookup of a lookup was accepted")
	}
}

func TestLookupKeysComeFromACompleteSourceInOrder(t *testing.T) {
	source := podread.Result{Readings: []podread.Reading{
		{Pod: "a", Stdout: "R\tignored\nK\t7\nK\tx y\nK\t3\nK\t7\n"},
		{Pod: "b", Stdout: "K\t'quoted'\nK\t9\r\nK\t11\nK\t12\n"},
	}}
	keys, err := podread.KeysOf(source, *lookup().Keys)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"7", "3", "9", "11"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys = %v, want %v (valid, first-seen order, capped)", keys, want)
	}
	if got := podread.ExpandKeys(lookup().Args, keys); !reflect.DeepEqual(got, []string{"index-cli", "get", "7 3 9 11"}) {
		t.Fatalf("expanded args = %v", got)
	}
	for name, broken := range map[string]podread.Result{
		"failed":    {Error: "no running Pod holds the target role"},
		"empty":     {},
		"truncated": {Readings: []podread.Reading{{Pod: "a", Stdout: "K\t1", Truncated: true}}},
		"no answer": {Readings: []podread.Reading{{Pod: "a", Error: "command gave no answer"}}},
	} {
		if _, err := podread.KeysOf(broken, *lookup().Keys); err == nil {
			t.Fatalf("%s source gave keys", name)
		}
	}
	for _, key := range []string{"", "a b", "a'b", "a\"b", "a*b", "$x", "a;b", strings.Repeat("k", 65)} {
		if podread.ValidKey(key) {
			t.Fatalf("ValidKey(%q) = true", key)
		}
	}
}
