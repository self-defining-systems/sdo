package storeops_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storeops"
)

func engine(t *testing.T, name string) storeops.Engine {
	t.Helper()
	e, ok := storeops.For(name)
	if !ok {
		t.Fatalf("no record operations for %s", name)
	}
	return e
}

func TestEnginesWithRecordOperations(t *testing.T) {
	for _, name := range storeops.Engines() {
		if e := engine(t, name); e.Name() != name || e.DefaultPort() == 0 || e.Image() == "" {
			t.Fatalf("engine %s is incomplete", name)
		}
	}
	for _, name := range []string{"memcached", "consul", "other", ""} {
		if _, ok := storeops.For(name); ok {
			t.Fatalf("%q must have no record operations", name)
		}
	}
}

func TestScopeValidation(t *testing.T) {
	for name, test := range map[string]struct {
		engine string
		scope  storeops.Scope
		ok     bool
	}{
		"redis default":            {"redis", storeops.Scope{}, true},
		"redis glob":               {"redis", storeops.Scope{Database: "2", Selector: "user:*"}, true},
		"redis named database":     {"redis", storeops.Scope{Database: "cache"}, false},
		"redis collection":         {"redis", storeops.Scope{Collection: "x"}, false},
		"redis multi-line glob":    {"redis", storeops.Scope{Selector: "a\nb"}, false},
		"mongodb filter":           {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"kind": "a"}`}, true},
		"mongodb empty filter":     {"mongodb", storeops.Scope{Database: "app", Collection: "items"}, true},
		"mongodb no collection":    {"mongodb", storeops.Scope{Database: "app"}, false},
		"mongodb system":           {"mongodb", storeops.Scope{Database: "app", Collection: "system.users"}, false},
		"mongodb array filter":     {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `[1]`}, false},
		"mongodb dotted database":  {"mongodb", storeops.Scope{Database: "a.b", Collection: "items"}, false},
		"postgres where":           {"postgres", storeops.Scope{Collection: "public.items", Selector: "kind = 'a'"}, true},
		"postgres bad table":       {"postgres", storeops.Scope{Collection: `items"; drop`}, false},
		"postgres separator":       {"postgres", storeops.Scope{Collection: "items", Selector: "TRUE; DELETE FROM items"}, false},
		"postgres comment":         {"postgres", storeops.Scope{Collection: "items", Selector: "TRUE --"}, false},
		"postgres meta command":    {"postgres", storeops.Scope{Collection: "items", Selector: `\! sh`}, false},
		"limit above the maximum":  {"redis", storeops.Scope{Limit: storeops.MaxLimit + 1}, false},
		"negative limit":           {"postgres", storeops.Scope{Collection: "items", Limit: -1}, false},
		"limit at the maximum":     {"redis", storeops.Scope{Limit: storeops.MaxLimit}, true},
		"postgres schema.table ok": {"postgres", storeops.Scope{Database: "app", Collection: "s.t"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			err := engine(t, test.engine).ValidateScope(test.scope)
			if (err == nil) != test.ok {
				t.Fatalf("ValidateScope(%+v) = %v, want ok=%v", test.scope, err, test.ok)
			}
		})
	}
}

func TestStatementValidation(t *testing.T) {
	for name, test := range map[string]struct {
		engine, statement string
		ok                bool
	}{
		"redis member removal":       {"redis", "SREM poisoned", true},
		"redis lower case":           {"redis", "hdel field", true},
		"redis delete":               {"redis", "DEL", true},
		"redis hash counter":         {"redis", "HINCRBY stock -1", true},
		"redis list trim":            {"redis", "LTRIM 0 99", true},
		"redis multi-key command":    {"redis", "RENAME other", false},
		"redis flush":                {"redis", "FLUSHALL", false},
		"redis script":               {"redis", "EVAL x 0", false},
		"redis empty":                {"redis", " ", false},
		"mongodb rename":             {"mongodb", `{"$rename": {"addr": "address"}}`, true},
		"mongodb pipeline":           {"mongodb", `[{"$set": {"a": 1}}]`, true},
		"mongodb delete":             {"mongodb", "delete", true},
		"mongodb replacement":        {"mongodb", `{"a": 1}`, false},
		"mongodb not json":           {"mongodb", `db.items.drop()`, false},
		"postgres set":               {"postgres", "SET status = 'open'", true},
		"postgres delete":            {"postgres", "delete", true},
		"postgres own where":         {"postgres", "SET a = 1 WHERE TRUE", false},
		"postgres correlated":        {"postgres", "SET total = (SELECT sum(n) FROM lines WHERE lines.order_id = orders.id)", true},
		"postgres quoted where":      {"postgres", "SET note = 'fixed where needed'", true},
		"postgres where after paren": {"postgres", "SET a = (1) WHERE TRUE", false},
		"postgres comment escapes":   {"postgres", "SET a = 1 --", false},
		"postgres second statement":  {"postgres", "SET a = 1; DROP TABLE items", false},
		"postgres not a set clause":  {"postgres", "TRUNCATE items", false},
		"postgres returning clause":  {"postgres", "SET a = 1 RETURNING *", false},
		"postgres line break escape": {"postgres", "SET a = 1\n\\! sh", false},
	} {
		t.Run(name, func(t *testing.T) {
			err := engine(t, test.engine).ValidateStatement(test.statement)
			if (err == nil) != test.ok {
				t.Fatalf("ValidateStatement(%q) = %v, want ok=%v", test.statement, err, test.ok)
			}
		})
	}
}

func TestCovers(t *testing.T) {
	redis, mongo := engine(t, "redis"), engine(t, "mongodb")
	for name, test := range map[string]struct {
		e              storeops.Engine
		backup, target storeops.Scope
		want           bool
	}{
		"same selector":         {redis, storeops.Scope{Selector: "a:*"}, storeops.Scope{Selector: "a:*"}, true},
		"whole database":        {redis, storeops.Scope{}, storeops.Scope{Selector: "a:*"}, true},
		"explicit whole":        {redis, storeops.Scope{Database: "0", Selector: "*"}, storeops.Scope{Selector: "b:*"}, true},
		"other database":        {redis, storeops.Scope{Database: "1"}, storeops.Scope{Selector: "a:*"}, false},
		"narrower backup":       {redis, storeops.Scope{Selector: "a:1*"}, storeops.Scope{Selector: "a:*"}, false},
		"whole collection":      {mongo, storeops.Scope{Database: "app", Collection: "items"}, storeops.Scope{Database: "app", Collection: "items", Selector: `{"a":1}`}, true},
		"other collection":      {mongo, storeops.Scope{Database: "app", Collection: "other"}, storeops.Scope{Database: "app", Collection: "items"}, false},
		"different filter text": {mongo, storeops.Scope{Database: "app", Collection: "items", Selector: `{"a":2}`}, storeops.Scope{Database: "app", Collection: "items", Selector: `{"a":1}`}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := storeops.Covers(test.e, test.backup, test.target); got != test.want {
				t.Fatalf("Covers = %v, want %v", got, test.want)
			}
		})
	}
}

func TestParseReadsTheLastMarkerOnly(t *testing.T) {
	for name, test := range map[string]struct {
		stdout, marker string
		n              int
		ok             bool
	}{
		"count":             {"sdo-count 12\n", storeops.MarkerCount, 12, true},
		"last of two":       {"sdo-affected 1\nsdo-affected 3\n", storeops.MarkerAffected, 3, true},
		"trailing words":    {"sdo-affected 2 failed 0\n", storeops.MarkerAffected, 2, true},
		"dump end":          {"a\nb\n#sdo-end 2\n", storeops.MarkerEnd, 2, true},
		"no marker":         {"ERR unknown command\n", storeops.MarkerCount, 0, false},
		"not a number":      {"sdo-count x\n", storeops.MarkerCount, 0, false},
		"negative":          {"sdo-count -1\n", storeops.MarkerCount, -1, false},
		"other marker only": {"sdo-expect-failed 4\n", storeops.MarkerAffected, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			n, ok := storeops.Parse(test.stdout, test.marker)
			if ok != test.ok || (ok && n != test.n) {
				t.Fatalf("Parse = %d %v, want %d %v", n, ok, test.n, test.ok)
			}
		})
	}
}

// Values reach a template only as positional arguments or escaped literals:
// a hostile selector never becomes shell program text.
func TestCommandsPassValuesAsArguments(t *testing.T) {
	hostile := `$(touch /tmp/x)'"`
	redis := engine(t, "redis")
	for _, c := range []storeops.Command{
		redis.Count(6379, storeops.Scope{Selector: hostile}),
		redis.Backup(6379, storeops.Scope{Selector: hostile}),
		redis.Write(6379, storeops.Scope{Selector: hostile}, "SREM "+hostile, 0, 1),
	} {
		if c.Args[0] != "sh" || c.Args[1] != "-c" || strings.Contains(c.Args[2], hostile) {
			t.Fatalf("the shell program must not carry the value: %q", c.Args[2])
		}
	}
	mongo := engine(t, "mongodb")
	scope := storeops.Scope{Database: "app", Collection: "items", Selector: `{"a": "` + strings.ReplaceAll(hostile, `"`, `\"`) + `"}`}
	for _, c := range []storeops.Command{mongo.Count(27017, scope), mongo.Backup(27017, scope), mongo.Write(27017, scope, "delete", 0, 1)} {
		if strings.Contains(c.Args[2], "touch") {
			t.Fatalf("the shell program must not carry the value: %q", c.Args[2])
		}
	}
}

func TestDerivedInvalidationIsRedisOnly(t *testing.T) {
	if _, err := engine(t, "redis").Invalidate(6379, storeops.Scope{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mongodb", "postgres"} {
		if _, err := engine(t, name).Invalidate(1, storeops.Scope{Database: "app", Collection: "items"}); err == nil {
			t.Fatalf("%s must refuse invalidation", name)
		}
	}
}

func TestRestoreFeedsTheDumpOnStdin(t *testing.T) {
	dump := "6b 0 00\n#sdo-end 1\n"
	for _, name := range storeops.Engines() {
		c, err := engine(t, name).Restore(1, storeops.Scope{Database: "app", Collection: "items"}, dump)
		if err != nil {
			t.Fatal(err)
		}
		if c.Stdin != dump {
			t.Fatalf("%s restore must read the dump on stdin", name)
		}
	}
}
