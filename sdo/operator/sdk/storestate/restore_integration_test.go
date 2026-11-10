//go:build integration

package storestate_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// runStub runs argv (sh -c ...) with stub CLIs on PATH: each stub appends its
// arguments, one per line, to a record and prints reply. It returns the
// command's output and the record. Values are synthetic.
func runStub(t *testing.T, argv []string, env map[string]string, reply string, clis ...string) (string, string) {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	dir := t.TempDir()
	record := filepath.Join(dir, "record")
	stub := "#!/bin/sh\nfor a in \"$@\";do printf '%s\\n' \"$a\" >>\"" + record + "\";done\necho --- >>\"" + record + "\"\ncat \"" + filepath.Join(dir, "reply") + "\"\n"
	if err := os.WriteFile(filepath.Join(dir, "reply"), []byte(reply), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, cli := range clis {
		if err := os.WriteFile(filepath.Join(dir, cli), []byte(stub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if argv[0] != "sh" {
		t.Fatalf("argv = %q", argv)
	}
	cmd := exec.Command(sh, argv[1:]...)
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, _ := cmd.CombinedOutput()
	data, _ := os.ReadFile(record)
	return string(out), string(data)
}

func restore(t *testing.T, engine storestate.Engine, baseline, current storestate.Facts, key storestate.Key, payload storestate.PayloadLookup) [][]string {
	t.Helper()
	for _, c := range storestate.Diff(baseline, current) {
		if c.Key() != key {
			continue
		}
		if err := storestate.Restorable(engine, c, baseline); err != nil {
			t.Fatalf("Restorable(%s) = %v", key, err)
		}
		cmds, err := storestate.RestoreCommands(engine, storestate.Endpoint{}, c, baseline, storestate.RestoreOptions{Payload: payload})
		if err != nil {
			t.Fatalf("RestoreCommands(%s) = %v", key, err)
		}
		return cmds
	}
	t.Fatalf("no change of %s in %v", key, storestate.Diff(baseline, current))
	return nil
}

func swap(t *testing.T, s string, pairs ...string) string {
	t.Helper()
	for i := 0; i+1 < len(pairs); i += 2 {
		if !strings.Contains(s, pairs[i]) {
			t.Fatalf("fixture lacks %q", pairs[i])
		}
		s = strings.Replace(s, pairs[i], pairs[i+1], 1)
	}
	return s
}

// Every redis store-setting fault the store-state read shows has a typed
// inverse that runs the engine's own command with the baseline value.
func TestRedisRestoreTable(t *testing.T) {
	healthy := parseAs(t, storestate.EngineRedis, healthyRedis)
	for name, test := range map[string]struct {
		fault string
		pairs []string
		key   storestate.Key
		want  string
	}{
		"min-replicas setting": {pairs: []string{"min-replicas-to-write\n0", "min-replicas-to-write\n1"}, key: storestate.Key{Kind: storestate.KindSetting, Name: "min-replicas-to-write"}, want: "CONFIG\nSET\nmin-replicas-to-write\n0\n"},
		"min-replicas block":   {pairs: []string{"min-replicas-to-write\n0", "min-replicas-to-write\n1"}, key: storestate.Key{Kind: storestate.KindWriteBlock, Name: "min-replicas"}, want: "CONFIG\nSET\nmin-replicas-to-write\n0\n"},
		"made replica":         {pairs: []string{"role:master", "role:slave\nmaster_link_status:down", "== role\nmaster", "== role\nslave"}, key: storestate.Key{Kind: storestate.KindRole, Name: "role"}, want: "REPLICAOF\nNO\nONE\n"},
		"replica write block":  {pairs: []string{"role:master", "role:slave", "== role\nmaster", "== role\nslave"}, key: storestate.Key{Kind: storestate.KindWriteBlock, Name: "replica-read-only"}, want: "REPLICAOF\nNO\nONE\n"},
		"memory full":          {pairs: []string{"maxmemory:0", "maxmemory:1", "maxmemory\n0", "maxmemory\n1"}, key: storestate.Key{Kind: storestate.KindSetting, Name: "maxmemory"}, want: "CONFIG\nSET\nmaxmemory\n0\n"},
		"client pause":         {pairs: []string{"aof_last_write_status:ok", "aof_last_write_status:ok\npaused_actions:write"}, key: storestate.Key{Kind: storestate.KindWriteBlock, Name: "client-pause"}, want: "CLIENT\nUNPAUSE\n"},
		"auth revoked":         {pairs: []string{"~* &* +@all", "~* &* +@all -@read -@write"}, key: storestate.Key{Kind: storestate.KindPrincipal, Name: "default"}, want: "ACL\nSETUSER\ndefault\nresetkeys\nresetchannels\n-@all\non\nnopass\nsanitize-payload\n~*\n&*\n+@all\n"},
		"requirepass set":      {pairs: []string{"requirepass\nempty", "requirepass\nset"}, key: storestate.Key{Kind: storestate.KindPrincipal, Name: "requirepass"}, want: "CONFIG\nSET\nrequirepass\n\n"},
	} {
		t.Run(name, func(t *testing.T) {
			faulty := parseAs(t, storestate.EngineRedis, swap(t, healthyRedis, test.pairs...))
			cmds := restore(t, storestate.EngineRedis, healthy, faulty, test.key, nil)
			if len(cmds) != 1 {
				t.Fatalf("commands = %q", cmds)
			}
			out, record := runStub(t, cmds[0], nil, "OK\n", "redis-cli")
			if !strings.Contains(record, test.want) || !strings.Contains(out, storestate.RestoredMarker) {
				t.Fatalf("record = %q, output = %q", record, out)
			}
		})
	}
}

func TestRedisRestoreRefusesWhatItCannotPutBackExactly(t *testing.T) {
	healthy := parseAs(t, storestate.EngineRedis, healthyRedis)
	removed := parseAs(t, storestate.EngineRedis, swap(t, healthyRedis, "user default on nopass sanitize-payload ~* &* +@all", "user default on nopass ~* &* +@all\nuser app on ~app:* +@read"))
	withPassword := parseAs(t, storestate.EngineRedis, swap(t, healthyRedis, "user default on nopass sanitize-payload ~* &* +@all", "user default on nopass ~* &* +@all\nuser app on ~app:* +@read"))
	gone := parseAs(t, storestate.EngineRedis, healthyRedis)
	for _, c := range storestate.Diff(withPassword, gone) {
		if c.Key().Name == "app" {
			if err := storestate.Restorable(storestate.EngineRedis, c, withPassword); !storestate.IsNotRestorable(err) {
				t.Errorf("removed user with a password: %v", err)
			}
		}
	}
	// An added user is deleted; the default user never is.
	for _, c := range storestate.Diff(healthy, removed) {
		if c.Key().Name == "app" {
			cmds, err := storestate.RestoreCommands(storestate.EngineRedis, storestate.Endpoint{}, c, healthy, storestate.RestoreOptions{})
			if err != nil || !strings.Contains(strings.Join(cmds[0], " "), "ACL DELUSER app") {
				t.Errorf("added user: %q %v", cmds, err)
			}
		}
	}
	loading := parseAs(t, storestate.EngineRedis, swap(t, healthyRedis, "aof_last_write_status:ok", "aof_last_write_status:err"))
	for _, c := range storestate.Diff(healthy, loading) {
		if err := storestate.Restorable(storestate.EngineRedis, c, healthy); !storestate.IsNotRestorable(err) {
			t.Errorf("%s: %v", c, err)
		}
	}
}

// A password is restored only from the store container's own environment.
func TestRedisRequirepassRestoresOnlyTheContainersPassword(t *testing.T) {
	baseline := parseAs(t, storestate.EngineRedis, swap(t, healthyRedis, "requirepass\nempty", "requirepass\nset"))
	faulty := parseAs(t, storestate.EngineRedis, healthyRedis)
	cmds := restore(t, storestate.EngineRedis, baseline, faulty, storestate.Key{Kind: storestate.KindPrincipal, Name: "requirepass"}, nil)
	joined := strings.Join(cmds[0], " ")
	if strings.Contains(joined, "synthetic") {
		t.Fatalf("command carries a value: %s", joined)
	}
	out, record := runStub(t, cmds[0], map[string]string{"REDIS_PASSWORD": "synthetic-pw"}, "OK\n", "redis-cli")
	if !strings.Contains(record, "requirepass\nsynthetic-pw\n") || strings.Contains(out, "synthetic-pw") || !strings.Contains(out, storestate.RestoredMarker) {
		t.Fatalf("record = %q, output = %q", record, out)
	}
	out, record = runStub(t, cmds[0], nil, "OK\n", "redis-cli")
	if record != "" || strings.Contains(out, storestate.RestoredMarker) {
		t.Fatalf("restored without a password: %q %q", record, out)
	}
}

func TestPostgresRestoreTable(t *testing.T) {
	healthy := parseAs(t, storestate.EnginePostgres, healthyPostgres)
	for name, test := range map[string]struct {
		pairs []string
		key   storestate.Key
		want  []string
	}{
		"database read-only": {pairs: []string{"exampledb\t*\tsearch_path", "exampledb\t*\tdefault_transaction_read_only\ton\nexampledb\t*\tsearch_path"}, key: storestate.Key{Kind: storestate.KindWriteBlock, Scope: "exampledb", Name: "default-transaction-read-only"}, want: []string{`ALTER DATABASE "exampledb" RESET default_transaction_read_only`, "-c default_transaction_read_only=off"}},
		"database setting":   {pairs: []string{"exampledb\t*\tsearch_path", "exampledb\t*\tdefault_transaction_read_only\ton\nexampledb\t*\tsearch_path"}, key: storestate.Key{Kind: storestate.KindSetting, Scope: "exampledb", Name: "default_transaction_read_only"}, want: []string{`ALTER DATABASE "exampledb" RESET default_transaction_read_only`}},
		"role expired":       {pairs: []string{"appuser\tlogin", "appuser\tlogin expired"}, key: storestate.Key{Kind: storestate.KindPrincipal, Name: "appuser"}, want: []string{`ALTER ROLE "appuser" NOSUPERUSER LOGIN NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1 VALID UNTIL 'infinity'`}},
		"role nologin":       {pairs: []string{"appuser\tlogin", "appuser\tnologin member-of=admin"}, key: storestate.Key{Kind: storestate.KindPrincipal, Name: "appuser"}, want: []string{`ALTER ROLE "appuser" NOSUPERUSER LOGIN`, `REVOKE "admin" FROM "appuser"`}},
		"server setting":     {pairs: []string{"set\tstatement_timeout\t0", "set\tstatement_timeout\t1"}, key: storestate.Key{Kind: storestate.KindSetting, Name: "statement_timeout"}, want: []string{"ALTER SYSTEM RESET statement_timeout", "ALTER SYSTEM SET statement_timeout TO '0'"}},
		"connection limit":   {pairs: []string{"exampledb\tt\t-1", "exampledb\tt\t0"}, key: storestate.Key{Kind: storestate.KindSetting, Scope: "exampledb", Name: "connection-limit"}, want: []string{`ALTER DATABASE "exampledb" CONNECTION LIMIT -1`}},
		"constraint added":   {pairs: []string{"constraint\tpublic.items\titems_qty_check\tc 064079ee", "constraint\tpublic.items\titems_qty_check\tc 064079ee\nconstraint\tpublic.items\tblock\tc 11111111"}, key: storestate.Key{Kind: storestate.KindValidator, Scope: "exampledb.public.items", Name: "block"}, want: []string{"exampledb", `ALTER TABLE "public"."items" DROP CONSTRAINT "block"`}},
	} {
		t.Run(name, func(t *testing.T) {
			faulty := parseAs(t, storestate.EnginePostgres, swap(t, healthyPostgres, test.pairs...))
			cmds := restore(t, storestate.EnginePostgres, healthy, faulty, test.key, nil)
			joined := strings.Join(cmds[0], "\n")
			for _, w := range test.want {
				if !strings.Contains(joined, w) {
					t.Errorf("commands lack %q:\n%s", w, joined)
				}
			}
			out, record := runStub(t, cmds[0], map[string]string{"POSTGRES_USER": "owner"}, "0\n", "psql", "sleep")
			if !strings.Contains(record, "-U\nowner\n") || !strings.Contains(out, storestate.RestoredMarker) {
				t.Fatalf("record = %q, output = %q", record, out)
			}
		})
	}
}

func TestPostgresRestoreEndsSessionsOnlyWhenAsked(t *testing.T) {
	healthy := parseAs(t, storestate.EnginePostgres, healthyPostgres)
	faulty := parseAs(t, storestate.EnginePostgres, swap(t, healthyPostgres, "exampledb\t*\tsearch_path", "exampledb\t*\tdefault_transaction_read_only\ton\nexampledb\t*\tsearch_path"))
	for _, c := range storestate.Diff(healthy, faulty) {
		if c.Key().Kind != storestate.KindSetting {
			continue
		}
		plain, _ := storestate.RestoreCommands(storestate.EnginePostgres, storestate.Endpoint{}, c, healthy, storestate.RestoreOptions{})
		ending, _ := storestate.RestoreCommands(storestate.EnginePostgres, storestate.Endpoint{}, c, healthy, storestate.RestoreOptions{EndSessions: true})
		if strings.Contains(strings.Join(plain[0], " "), "pg_terminate_backend") || !strings.Contains(strings.Join(ending[0], " "), "pg_terminate_backend(pid)) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid() AND datname='exampledb'") {
			t.Fatalf("plain %q, ending %q", plain, ending)
		}
	}
}

func TestPostgresRestoreQuotesNames(t *testing.T) {
	c := storestate.Change{Before: &storestate.Item{Kind: storestate.KindSetting, Scope: `x"; DROP TABLE t; --@db`, Name: "work_mem", Value: "4'MB"},
		After: &storestate.Item{Kind: storestate.KindSetting, Scope: `x"; DROP TABLE t; --@db`, Name: "work_mem", Value: "1"}}
	cmds, err := storestate.RestoreCommands(storestate.EnginePostgres, storestate.Endpoint{}, c, storestate.Facts{}, storestate.RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(cmds[0], "\n"); !strings.Contains(joined, `ALTER ROLE "x""; DROP TABLE t; --" IN DATABASE "db" SET work_mem TO '4''MB'`) {
		t.Fatalf("%s", joined)
	}
	bad := c
	bad.Before = &storestate.Item{Kind: storestate.KindSetting, Name: "work_mem; DROP", Value: "1"}
	bad.After = nil
	if err := storestate.Restorable(storestate.EnginePostgres, bad, storestate.Facts{}); !storestate.IsNotRestorable(err) {
		t.Fatalf("unsafe name: %v", err)
	}
}

const mongoPayloadOutput = `some shell warning
== payload shell
validator	exampledb	orders	moderate/warn/0a0b0c0d	{ "$jsonSchema" : { "required" : [ "placed" ] } }
index	exampledb	items	sku_1	sku:1 unique	{ "key" : { "sku" : 1 }, "name" : "sku_1", "unique" : true }
== end
`

func mongoPayload(t *testing.T) storestate.Payload {
	t.Helper()
	reader, ok := storestate.PayloadReaderFor(storestate.EngineMongoDB)
	if !ok {
		t.Fatal("mongodb has no payload reader")
	}
	p, err := reader.ParsePayload(mongoPayloadOutput)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMongoPayloadParsesAndRefusesAnIncompleteRead(t *testing.T) {
	p := mongoPayload(t)
	if entry, ok := p.Lookup(storestate.Key{Kind: storestate.KindIndex, Scope: "exampledb.items", Name: "sku_1"}); !ok || entry.Value != "sku:1 unique" || entry.Format != storestate.PayloadShell {
		t.Fatalf("index entry = %+v %v", entry, ok)
	}
	reader, _ := storestate.PayloadReaderFor(storestate.EngineMongoDB)
	if _, err := reader.ParsePayload(strings.TrimSuffix(mongoPayloadOutput, "== end\n")); err == nil {
		t.Fatal("an unfinished payload read parsed")
	}
	if _, ok := storestate.PayloadReaderFor(storestate.EngineRedis); ok {
		t.Fatal("redis needs no payload")
	}
	cmd := reader.PayloadCommand(storestate.Endpoint{Port: 27017})
	for _, forbidden := range []string{"insert", "remove", "update", "drop", "find("} {
		if strings.Contains(strings.Join(cmd, " "), forbidden) {
			t.Errorf("payload read contains %q", forbidden)
		}
	}
}

func TestMongoRestoreTable(t *testing.T) {
	healthyText := swap(t, healthyMongo, "moderate/warn", "moderate/warn/0a0b0c0d")
	healthy := parseAs(t, storestate.EngineMongoDB, healthyText)
	payload := mongoPayload(t)
	for name, test := range map[string]struct {
		pairs []string
		key   storestate.Key
		want  []string
	}{
		"fsync lock":        {pairs: []string{"fsync\t0\nwaiting\t0", "fsync\t1\nwaiting\t3"}, key: storestate.Key{Kind: storestate.KindLock, Name: "fsync"}, want: []string{"fsyncUnlock"}},
		"fsync write block": {pairs: []string{"fsync\t0", "fsync\t1"}, key: storestate.Key{Kind: storestate.KindWriteBlock, Name: "fsync-lock"}, want: []string{"fsyncUnlock"}},
		"validator added":   {pairs: []string{"coll\texampledb\titems\t12\t-", "coll\texampledb\titems\t12\tstrict/error/deadbeef"}, key: storestate.Key{Kind: storestate.KindValidator, Scope: "exampledb", Name: "items"}, want: []string{`collMod:"items",validator:{},validationLevel:"strict"`}},
		"validator changed": {pairs: []string{"moderate/warn/0a0b0c0d", "strict/error/deadbeef"}, key: storestate.Key{Kind: storestate.KindValidator, Scope: "exampledb", Name: "orders"}, want: []string{`collMod:"orders",validator:eval(`, `required`, `validationLevel:"moderate",validationAction:"warn"`}},
		"index added":       {pairs: []string{"index\texampledb\titems\tsku_1\tsku:1 unique", "index\texampledb\titems\tsku_1\tsku:1 unique\nindex\texampledb\titems\tname_1\tname:1 unique"}, key: storestate.Key{Kind: storestate.KindIndex, Scope: "exampledb.items", Name: "name_1"}, want: []string{`dropIndexes:"items",index:"name_1"`}},
		"index dropped":     {pairs: []string{"index\texampledb\titems\tsku_1\tsku:1 unique\n", ""}, key: storestate.Key{Kind: storestate.KindIndex, Scope: "exampledb.items", Name: "sku_1"}, want: []string{`createIndexes:"items"`, `unique`}},
		"parameter":         {pairs: []string{"set\tnotablescan\tfalse", "set\tnotablescan\ttrue"}, key: storestate.Key{Kind: storestate.KindSetting, Name: "notablescan"}, want: []string{`p["notablescan"]=false`}},
		"user roles":        {pairs: []string{"appuser\treadWrite@exampledb", "appuser\tread@exampledb"}, key: storestate.Key{Kind: storestate.KindPrincipal, Scope: "exampledb", Name: "appuser"}, want: []string{`updateUser:"appuser",roles:[{role:"readWrite",db:"exampledb"}]`}},
	} {
		t.Run(name, func(t *testing.T) {
			faulty := parseAs(t, storestate.EngineMongoDB, swap(t, healthyText, test.pairs...))
			cmds := restore(t, storestate.EngineMongoDB, healthy, faulty, test.key, payload.Lookup)
			joined := strings.Join(cmds[0], "\n")
			for _, w := range test.want {
				if !strings.Contains(joined, w) {
					t.Errorf("commands lack %q:\n%s", w, joined)
				}
			}
			out, record := runStub(t, cmds[0], map[string]string{"MONGO_INITDB_ROOT_USERNAME": "admin", "MONGO_INITDB_ROOT_PASSWORD": "synthetic"}, storestate.RestoredMarker+"\n", "mongo")
			if !strings.Contains(record, "-u\nadmin\n") || !strings.Contains(out, storestate.RestoredMarker) {
				t.Fatalf("record = %q, output = %q", record, out)
			}
		})
	}
}

// A payload that no longer describes the baseline item, or none at all, is
// never used.
func TestMongoRestoreRefusesAMissingOrStalePayload(t *testing.T) {
	healthy := parseAs(t, storestate.EngineMongoDB, swap(t, healthyMongo, "moderate/warn", "moderate/warn/99999999"))
	faulty := parseAs(t, storestate.EngineMongoDB, swap(t, healthyMongo, "moderate/warn", "strict/error/deadbeef"))
	for _, c := range storestate.Diff(healthy, faulty) {
		if c.Key().Kind != storestate.KindValidator {
			continue
		}
		for name, lookup := range map[string]storestate.PayloadLookup{"stale": mongoPayload(t).Lookup, "missing": nil} {
			_, err := storestate.RestoreCommands(storestate.EngineMongoDB, storestate.Endpoint{}, c, healthy, storestate.RestoreOptions{Payload: lookup})
			if !storestate.IsNotRestorable(err) {
				t.Errorf("%s payload: %v", name, err)
			}
		}
	}
}
