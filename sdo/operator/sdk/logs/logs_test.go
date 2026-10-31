package logs_test

import (
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	good := logs.Query{ID: "errors", Patterns: []logs.Pattern{{ID: "bad", RE2: "error|panic"}}}
	if err := logs.ValidateQueries([]logs.Query{good}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []logs.Query{{}, {ID: "x", Patterns: []logs.Pattern{{ID: "bad", RE2: "["}}}, {ID: "x", Patterns: []logs.Pattern{{ID: "bad", RE2: "(?=x)"}}}, {ID: "x", Patterns: []logs.Pattern{{ID: "a", RE2: "x"}, {ID: "a", RE2: "y"}}}} {
		if logs.ValidateQueries([]logs.Query{q}) == nil {
			t.Fatalf("accepted %+v", q)
		}
	}
	if logs.ValidateQueries([]logs.Query{good, good}) == nil {
		t.Fatal("duplicate")
	}
	if logs.ValidateQueries(make([]logs.Query, logs.MaxQueries+1)) == nil {
		t.Fatal("unbounded")
	}
}
func TestHelpersAndSourceErrors(t *testing.T) {
	at := time.Unix(100, 0)
	w := logs.Window{Cycles: []logs.Cycle{
		{Start: at, End: at.Add(time.Second), Running: true, Lines: 10, Matches: map[string]logs.Match{"bad": {Count: 0}}},
		{Start: at.Add(time.Second), End: at.Add(2 * time.Second), Running: true, Lines: 2, Matches: map[string]logs.Match{"bad": {Count: 2, Excerpts: []string{"error"}}}},
	}}
	if ok, e := logs.Appeared(w, "bad"); !ok || e.Count != 2 || len(e.Excerpts) != 1 {
		t.Fatalf("%v %+v", ok, e)
	}
	if rate, e := logs.Rate(w, "bad"); rate != 1 || e.Lines != 12 {
		t.Fatalf("%v %+v", rate, e)
	}
	w.Cycles = append(w.Cycles, logs.Cycle{Running: true}, logs.Cycle{Running: true})
	if ok, _ := logs.Silent(w, 2); !ok {
		t.Fatal("silence missed")
	}
	w.Cycles[len(w.Cycles)-1].Truncated = true
	if ok, _ := logs.Silent(w, 2); ok {
		t.Fatal("truncation proves no silence")
	}
	if _, err := logs.Require(sdktest.Snapshot{}, "x"); err == nil {
		t.Fatal("missing source clear")
	}
	if _, err := logs.Require(sdktest.Snapshot{Logs: map[string]logs.Result{"x": {Error: "denied"}}}, "x"); err == nil {
		t.Fatal("source error clear")
	}
}

func TestLimitsAndConservativeHelpers(t *testing.T) {
	for _, q := range []logs.Query{
		{ID: "q", Patterns: make([]logs.Pattern, logs.MaxPatterns+1)},
		{ID: "q", Patterns: []logs.Pattern{{ID: "p", RE2: strings.Repeat("x", logs.MaxPatternBytes+1)}}},
		{ID: strings.Repeat("q", logs.MaxIDBytes+1)},
	} {
		if q.Validate() == nil {
			t.Fatalf("accepted bounds violation %+v", q)
		}
	}
	at := time.Unix(100, 0)
	w := logs.Window{Cycles: []logs.Cycle{{Start: at, End: at.Add(time.Second), Truncated: true, Matches: map[string]logs.Match{"p": {Count: 0}}}, {Start: at.Add(time.Second), End: at.Add(2 * time.Second), Matches: map[string]logs.Match{"p": {Count: 1}}}}}
	if ok, e := logs.Appeared(w, "p"); ok || !e.Truncated {
		t.Fatal("incomplete absence established appearance")
	}
	w.Cycles[0].Error = "denied"
	if ok, _ := logs.Appeared(w, "p"); ok {
		t.Fatal("source error established appearance")
	}
	if ok, _ := logs.Silent(w, 0); ok {
		t.Fatal("invalid silence count")
	}
}
