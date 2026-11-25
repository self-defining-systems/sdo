package storestate_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

func storeClues(changes ...storestate.Change) []storestate.Clue {
	return storestate.StoreChanges{Engine: storestate.EnginePostgres, Changes: changes}.Clues()
}

func group(number, tables, value string) *storestate.Item {
	return &storestate.Item{Kind: storestate.KindValidator, Scope: "appdb.{table group " + number + " " + tables + "}", Name: "definitions", Value: value}
}

func keysOf(clues []storestate.Clue) string {
	var keys []string
	for _, c := range clues {
		keys = append(keys, c.Key)
	}
	return strings.Join(keys, "\n")
}

// A changed group is described by what changed, kind first, never by its hashes.
func TestCluesNameTheKindOfObjectAGroupChanged(t *testing.T) {
	const before = "137 i412:aaaa c88:bbbb t14:cccc n900:dddd"
	for name, test := range map[string]struct{ before, after, want string }{
		"trigger added":      {before, "137 i412:aaaa c88:bbbb t15:ffff n900:dddd", "+1 trigger in appdb table group 07 of 16"},
		"constraint dropped": {before, "137 i412:aaaa c87:ffff t14:cccc n900:dddd", "-1 constraint in appdb table group 07 of 16"},
		"column added":       {before, "137 i412:aaaa c88:bbbb t14:cccc n901:ffff", "+1 column in appdb table group 07 of 16"},
		"index in place":     {before, "137 i412:ffff c88:bbbb t14:cccc n900:dddd", "index changed in place in appdb table group 07 of 16"},
		"two kinds":          {before, "137 i412:aaaa c88:ffff t13:ffff n900:dddd", "-1 trigger, constraint changed in place in"},
		"no kind recorded":   {"45 c0ffee00", "45 c0ffee01", "definition changed (kind not recorded) in appdb"},
		"older value":        {"137 89f0cb69 i412 c88 t14", "137 f4d76c7e i412 c88 t15", "+1 trigger in appdb"},
	} {
		clues := storeClues(storestate.Change{Before: group("07", "684b516f", test.before), After: group("07", "684b516f", test.after)})
		if len(clues) != 1 || !strings.Contains(clues[0].Key, test.want) || clues[0].Routine {
			t.Errorf("%s: %+v", name, clues)
			continue
		}
		for _, hash := range []string{"684b516f", "ffff", "f4d76c7e", "c0ffee"} {
			if strings.Contains(clues[0].Key+clues[0].Before+clues[0].After, hash) {
				t.Errorf("%s: a hash reached the clue: %+v", name, clues[0])
			}
		}
	}
	clues := storeClues(storestate.Change{Before: group("07", "x", before), After: group("07", "x", "137 i412:aaaa c88:bbbb t15:ffff n900:dddd")})
	if clues[0].Before != "14 triggers" || clues[0].After != "15 triggers" {
		t.Fatalf("values = %+v", clues[0])
	}
	if got := storeClues(); len(got) != 0 {
		t.Fatalf("no change gave %+v", got)
	}
}

// Every other kind keeps its own name: a setting, a trigger and a constraint
// of a catalog listed by item, a database's functions, and a kind this build
// does not know.
func TestCluesNameTheKindOfAnItem(t *testing.T) {
	for want, item := range map[string]storestate.Item{
		"setting maxclients":                  {Kind: storestate.KindSetting, Name: "maxclients", Value: "20"},
		"trigger appdb.public.t/cut":          {Kind: storestate.KindValidator, Scope: "appdb.public.t", Name: "cut", Value: "trigger O abcd"},
		"constraint appdb.public.t/short":     {Kind: storestate.KindValidator, Scope: "appdb.public.t", Name: "short", Value: "c abcd"},
		"function definitions of appdb":       {Kind: storestate.KindValidator, Scope: "appdb.{functions}", Name: "functions", Value: "3:abcd"},
		"column definitions of appdb":         {Kind: storestate.KindValidator, Scope: "appdb.{columns}", Name: "columns", Value: "9:abcd"},
		"unknown appdb/thing":                 {Kind: "thing", Scope: "appdb", Name: "thing", Value: "x"},
		"write-block appdb/something-blocked": {Kind: storestate.KindWriteBlock, Scope: "appdb", Name: "something-blocked", Value: "active"},
	} {
		clues := storeClues(storestate.Change{After: &item})
		if len(clues) != 1 || clues[0].Key != want {
			t.Errorf("want %q, got %+v", want, clues)
		}
	}
	before := storestate.Item{Kind: storestate.KindValidator, Scope: "appdb.{functions}", Name: "functions", Value: "3:abcd"}
	after := before
	after.Value = "3:ffff"
	if clues := storeClues(storestate.Change{Before: &before, After: &after}); clues[0].Key != "function definitions of appdb changed in place" || clues[0].After != "3" {
		t.Fatalf("function body: %+v", clues)
	}
}

// A new table moves its group to a new scope, so the diff holds a removed and
// an added item: both halves are one clue, a trigger that came with it is
// named, and tables that only brought their own objects are one counted line.
func TestCluesKeepATriggerAmongPartitionNoise(t *testing.T) {
	var changes []storestate.Change
	for _, n := range []string{"00", "01", "03", "06", "09", "10", "11", "12"} {
		changes = append(changes, storestate.Change{Before: group(n, "0ld", "100 i300:aaaa c90:bbbb t10:cccc n700:dddd")},
			storestate.Change{After: group(n, "new", "101 i304:aa00 c93:bb00 t10:cc00 n715:dd00")})
	}
	changes = append(changes, storestate.Change{Before: group("07", "0ld", "139 i578:aaaa c362:bbbb t47:cccc n900:dddd")},
		storestate.Change{After: group("07", "new", "140 i585:aa00 c365:bb00 t48:cc00 n915:dd00")})
	changes = append(changes, storestate.Change{Before: &storestate.Item{Kind: storestate.KindNamespace, Scope: "appdb", Name: "public.events", Value: "100"}, After: &storestate.Item{Kind: storestate.KindNamespace, Scope: "appdb", Name: "public.events", Value: "120"}})
	clues := storeClues(changes...)
	if len(clues) != 2 || !strings.HasPrefix(clues[0].Key, "+1 trigger, +3 constraints") || !strings.Contains(clues[0].Key, "table group 07") {
		t.Fatalf("clues:\n%s", keysOf(clues))
	}
	if !clues[1].Routine || !strings.HasPrefix(clues[1].Key, "routine growth: 9 changes") {
		t.Fatalf("routine line: %+v", clues[1])
	}
	// A dropped or replaced table, and a constraint lost with a new table, are not growth.
	for name, after := range map[string]string{"dropped": "99 i300:aaaa c90:bbbb t10:cccc n700:dddd", "replaced": "100 i300:aa00 c90:bb00 t10:cc00 n700:dd00", "constraint lost": "101 i304:aa00 c89:bb00 t10:cc00 n715:dd00"} {
		clues := storeClues(storestate.Change{Before: group("00", "0ld", "100 i300:aaaa c90:bbbb t10:cccc n700:dddd")}, storestate.Change{After: group("00", "new", after)})
		if len(clues) != 1 || clues[0].Routine {
			t.Errorf("%s: %+v", name, clues)
		}
	}
}

// The rarest kind leads, a change on tables that all still exist goes before
// one that came with a new table, and a kind that lost most of its count is not
// growth.
func TestCluesRankByRarity(t *testing.T) {
	changes := []storestate.Change{
		{Before: group("01", "a", "10 i5:aaaa c5:bbbb t1:cccc"), After: group("01", "a", "10 i5:aaaa c6:ffff t1:cccc")},
		{Before: group("02", "a", "10 i5:aaaa c5:bbbb t1:cccc"), After: group("02", "a", "10 i5:aaaa c6:ffff t1:cccc")},
		{Before: group("03", "a", "10 i5:aaaa c5:bbbb t1:cccc"), After: group("03", "a", "10 i5:aaaa c5:bbbb t2:ffff")},
		{Before: group("04", "a", "10 i5:aaaa c5:bbbb t1:cccc")}, {After: group("04", "b", "11 i6:aa00 c5:bb00 t2:cc00")},
		{Before: &storestate.Item{Kind: storestate.KindNamespace, Scope: "appdb", Name: "public.t", Value: "100"}, After: &storestate.Item{Kind: storestate.KindNamespace, Scope: "appdb", Name: "public.t", Value: "10"}},
	}
	clues := storeClues(changes...)
	if len(clues) != 5 || !strings.Contains(clues[0].Key, "group 03") || !strings.HasPrefix(clues[1].Key, "table appdb/public.t") || !strings.Contains(clues[4].Key, "group 04") {
		t.Fatalf("order:\n%s", keysOf(clues))
	}
}

// However many changes there are, whole clues are kept within the bounds, the
// last line counts the rest, and nothing is cut inside a word.
func TestCluesAreBoundedAndNeverCutAWord(t *testing.T) {
	var changes []storestate.Change
	for i := range 300 {
		changes = append(changes, storestate.Change{After: &storestate.Item{Kind: storestate.KindSetting, Scope: strings.Repeat("s", i%40), Name: fmt.Sprintf("setting_%03d", i), Value: strings.TrimSpace(strings.Repeat("word ", i))}})
	}
	changes = append(changes, storestate.Change{After: &storestate.Item{Kind: storestate.KindSetting, Name: strings.Repeat("x", 5000), Value: "1"}})
	clues := storeClues(changes...)
	size := 0
	for _, c := range clues {
		size += len(c.Key) + len(c.Before) + len(c.After)
		for _, word := range strings.Fields(c.After) {
			if word != "word" && !strings.HasPrefix(word, "(+") && word != "bytes)" {
				t.Fatalf("cut inside a word: %q", c.After)
			}
		}
	}
	last := clues[len(clues)-1].Key
	if len(clues) > storestate.MaxClues || size > storestate.MaxClueBytes || !strings.Contains(last, fmt.Sprintf("+%d more store changes", 301-(len(clues)-1))) || !strings.Contains(last, "setting") {
		t.Fatalf("%d clues, %d bytes, last %q", len(clues), size, last)
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

// Kinds of one group first observed at different times are separate clues, each
// with its own time; kinds observed together stay one. A function digest and a
// setting are dated by their value.
func TestCluesDateEachChangeOnItsOwn(t *testing.T) {
	id := storestate.QueryID(storestate.EnginePostgres, "db")
	read := func(seconds int, value, functions string) podread.Reading {
		out := strings.Replace(groupedPostgres(map[string]string{"07 a0000000": value}), "== catalog postgres", "digest\tfunctions\t\t"+functions+"\n== catalog postgres", 1)
		return podread.Reading{Namespace: "app", Pod: "db-0", ObservedAt: time.Unix(int64(1000+seconds), 0).UTC(), Stdout: out}
	}
	base := []podread.Reading{read(-300, "40 i5:aaaa c5:aaaa t5:aaaa n9:aaaa", "3:aaaa")}
	observed, readAt := map[string]map[string]podread.Observed(nil), map[string]time.Time{}
	var last podread.Result
	for _, r := range []podread.Reading{
		read(-150, "40 i6:bbbb c5:aaaa t5:aaaa n9:aaaa", "3:bbbb"),
		read(12, "40 i6:bbbb c6:cccc t6:cccc n9:aaaa", "3:cccc"),
	} {
		last = podread.Result{Query: podread.Query{ID: id}, Readings: []podread.Reading{r}, Baseline: base}
		last.Observed = storestate.ObserveChanges(id, last, observed, readAt)
		observed = last.Observed
	}
	got := map[string]int64{}
	for _, c := range storestate.Changes(snapshotOf(id, last))[0].Clues() {
		got[c.Key] = c.FirstObserved.Unix() - 1000
	}
	want := map[string]int64{"+1 trigger, +1 constraint in exampledb table group 07 of 16": 12, "+1 index in exampledb table group 07 of 16": -150, "function definitions of exampledb changed in place": 12}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
