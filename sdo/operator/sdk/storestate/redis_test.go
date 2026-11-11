package storestate_test

import (
	"regexp"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// healthyRedis is synthetic output of the redis read command on a standalone
// primary with no memory cap and no password.
const healthyRedis = `== info
role:master
connected_slaves:0
connected_clients:3
maxclients:10000
used_memory:1048576
maxmemory:0
maxmemory_policy:noeviction
rdb_last_bgsave_status:ok
aof_last_write_status:ok
db0:keys=12,expires=0,avg_ttl=0
== dbsize
12
== role
master
== config
maxmemory
0
maxmemory-policy
noeviction
maxclients
10000
min-replicas-to-write
0
min-replicas-max-lag
10
replica-read-only
yes
stop-writes-on-bgsave-error
yes
appendonly
no
requirepass
empty
== acl
user default on nopass sanitize-payload ~* &* +@all
`

func redisAdapter(t *testing.T) storestate.Adapter {
	t.Helper()
	a, ok := storestate.AdapterFor(storestate.EngineRedis)
	if !ok {
		t.Fatal("no redis adapter")
	}
	return a
}

func parse(t *testing.T, out string) storestate.Facts {
	t.Helper()
	f, err := redisAdapter(t).Parse(out)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return f
}

func item(f storestate.Facts, kind storestate.ItemKind, scope, name string) (storestate.Item, bool) {
	for _, it := range f.Items {
		if it.Kind == kind && it.Scope == scope && it.Name == name {
			return it, true
		}
	}
	return storestate.Item{}, false
}

func TestRedisParsesHealthyFacts(t *testing.T) {
	f := parse(t, healthyRedis)
	if f.Engine != storestate.EngineRedis {
		t.Fatalf("engine = %q", f.Engine)
	}
	if f.Writable == nil || !*f.Writable {
		t.Fatalf("writable = %v", f.Writable)
	}
	if f.Connections == nil || *f.Connections != (storestate.Gauge{Used: 3, Max: 10000}) {
		t.Fatalf("connections = %+v", f.Connections)
	}
	if f.Memory == nil || *f.Memory != (storestate.Gauge{Used: 1048576, Max: 0}) {
		t.Fatalf("memory = %+v", f.Memory)
	}
	for _, want := range []storestate.Item{
		{Kind: storestate.KindRole, Name: "role", Value: "master"},
		{Kind: storestate.KindSetting, Name: "min-replicas-to-write", Value: "0"},
		{Kind: storestate.KindSetting, Name: "maxmemory-policy", Value: "noeviction"},
		{Kind: storestate.KindPrincipal, Name: "default", Value: "on nopass sanitize-payload ~* &* +@all"},
		{Kind: storestate.KindPrincipal, Name: "requirepass", Value: "empty"},
		{Kind: storestate.KindNamespace, Scope: "db0", Name: "keys", Value: "12"},
	} {
		got, ok := item(f, want.Kind, want.Scope, want.Name)
		if !ok || got != want {
			t.Errorf("item %s/%s/%s = %+v, %v", want.Kind, want.Scope, want.Name, got, ok)
		}
	}
	for _, it := range f.Items {
		if it.Kind == storestate.KindWriteBlock || it.Kind == storestate.KindRefusal {
			t.Errorf("healthy facts carry %+v", it)
		}
	}
	for i := 1; i < len(f.Items); i++ {
		if !f.Items[i-1].Less(f.Items[i]) {
			t.Fatalf("items not sorted at %d: %+v", i, f.Items)
		}
	}
}

func TestRedisWriteBlocks(t *testing.T) {
	for name, test := range map[string]struct {
		from, to, block string
	}{
		"min replicas unmet":   {"min-replicas-to-write\n0", "min-replicas-to-write\n1", "min-replicas"},
		"replica":              {"role:master", "role:slave\nmaster_link_status:down", "replica-read-only"},
		"memory full":          {"used_memory:1048576\nmaxmemory:0", "used_memory:1048576\nmaxmemory:1", "oom"},
		"failed snapshot":      {"rdb_last_bgsave_status:ok", "rdb_last_bgsave_status:err", "bgsave-error"},
		"paused clients":       {"connected_clients:3", "connected_clients:3\npaused_actions:write", "client-pause"},
		"failed append write":  {"aof_last_write_status:ok", "aof_last_write_status:err", "aof-error"},
		"replica role command": {"== role\nmaster", "== role\nslave", "replica-read-only"},
	} {
		t.Run(name, func(t *testing.T) {
			out := strings.Replace(healthyRedis, test.from, test.to, 1)
			if test.block == "replica-read-only" && name == "replica role command" {
				out = strings.Replace(out, "role:master\n", "", 1)
			}
			f := parse(t, out)
			if f.Writable == nil || *f.Writable {
				t.Fatalf("writable = %v", f.Writable)
			}
			if _, ok := item(f, storestate.KindWriteBlock, "", test.block); !ok {
				t.Fatalf("no write-block %s in %+v", test.block, f.Items)
			}
		})
	}
	// A replica that accepts writes is not blocked.
	f := parse(t, strings.Replace(strings.Replace(healthyRedis, "role:master", "role:slave", 1), "replica-read-only\nyes", "replica-read-only\nno", 1))
	if f.Writable == nil || !*f.Writable {
		t.Fatalf("writable replica = %v", f.Writable)
	}
}

func TestRedisRecordsRefusalsAsFacts(t *testing.T) {
	f := parse(t, strings.Replace(healthyRedis, "== dbsize\n12", "== dbsize\nNOPERM User default has no permissions to run the 'dbsize' command", 1))
	if got, ok := item(f, storestate.KindRefusal, "", "dbsize"); !ok || got.Value != "no-permission" {
		t.Fatalf("refusal = %+v %v", got, ok)
	}
	var lines []string
	for _, line := range strings.Split(healthyRedis, "\n") {
		if strings.HasPrefix(line, "== ") {
			lines = append(lines, line, "NOAUTH Authentication required.")
		}
	}
	f = parse(t, strings.Join(lines, "\n"))
	if f.Writable != nil || f.Connections != nil || f.Memory != nil {
		t.Fatalf("a refused read claims facts: %+v", f)
	}
	for _, c := range storestate.Diff(parse(t, healthyRedis), f) {
		if c.After == nil {
			t.Errorf("a refused read reports %s as removed", c.Key())
		}
	}
	for _, section := range []string{"info", "dbsize", "role", "config", "acl"} {
		if got, ok := item(f, storestate.KindRefusal, "", section); !ok || got.Value != "auth-required" {
			t.Errorf("refusal %s = %+v %v", section, got, ok)
		}
	}
	f = parse(t, strings.Replace(healthyRedis, "== acl\nuser default on nopass sanitize-payload ~* &* +@all", "== acl\nERR unknown command 'ACL', with args beginning with: 'LIST'", 1))
	if got, _ := item(f, storestate.KindRefusal, "", "acl"); got.Value != "unsupported" {
		t.Fatalf("unsupported acl = %+v", got)
	}
	f = parse(t, strings.Replace(healthyRedis, "== info\n", "== info\nERR max number of clients reached\n", 1))
	if got, _ := item(f, storestate.KindRefusal, "", "info"); got.Value != "max-clients" {
		t.Fatalf("max clients = %+v", got)
	}
}

func TestRedisUnavailableReadIsAnError(t *testing.T) {
	refused := ""
	for _, section := range []string{"info", "dbsize", "role", "config", "acl"} {
		refused += "== " + section + "\nCould not connect to Redis at 127.0.0.1:6379: Connection refused\n"
	}
	for name, out := range map[string]string{
		"empty":            "",
		"no sections":      "sh: redis-cli: not found\n",
		"connection":       refused,
		"missing sections": "== info\nrole:master\n",
	} {
		t.Run(name, func(t *testing.T) {
			if f, err := redisAdapter(t).Parse(out); err == nil {
				t.Fatalf("Parse accepted an unavailable read: %+v", f)
			}
		})
	}
}

func TestRedisNeverRecordsCredentials(t *testing.T) {
	out := strings.Replace(healthyRedis, "user default on nopass sanitize-payload ~* &* +@all",
		"user default on #5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 >plain <old ~* &* +@all\nuser app on #0123 ~app:* +get", 1)
	f := parse(t, out)
	for _, it := range f.Items {
		if strings.ContainsAny(it.Value, "#><") {
			t.Errorf("item %+v carries a credential", it)
		}
	}
	if got, _ := item(f, storestate.KindPrincipal, "", "default"); got.Value != "on ~* &* +@all" {
		t.Fatalf("default = %q", got.Value)
	}
	if _, ok := item(f, storestate.KindPrincipal, "", "app"); !ok {
		t.Fatal("second user missing")
	}
}

func TestRedisReadCommandIsBoundedAndValueFree(t *testing.T) {
	argv := redisAdapter(t).ReadCommand(storestate.Endpoint{Port: 6380})
	// sh -c <credential prelude> $0 <engine script> <holders awk> <TCP fallback> <key-pattern Lua, split>
	if len(argv) < 8 || argv[0] != "sh" || argv[1] != "-c" {
		t.Fatalf("argv = %q", argv)
	}
	script := argv[4]
	if len(script) > 1024 {
		t.Fatalf("script is %d bytes", len(script))
	}
	for _, want := range []string{"-p 6380", "INFO", "DBSIZE", "ROLE", "CONFIG GET", "ACL LIST", "min-replicas-to-write"} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q", want)
		}
	}
	if !strings.Contains(argv[5], "CLIENT LIST") || !strings.Contains(argv[6], "/proc/net/tcp") {
		t.Errorf("holders arguments = %q, %q", argv[5], argv[6])
	}
	for _, forbidden := range []string{"SET ", "FLUSH", "DEL ", "KEYS", "SCAN", "GET requirepass|"} {
		if strings.Contains(script, forbidden) {
			t.Errorf("script contains %q", forbidden)
		}
	}
	// The key-pattern scan only reads key metadata: SCAN, and PTTL to leave
	// expiring keys out.
	lua := strings.Join(argv[5:], "")
	for _, arg := range argv[5:] {
		if len(arg) > 1024 {
			t.Fatalf("a key-pattern argument is %d bytes", len(arg))
		}
	}
	calls := regexp.MustCompile(`redis\.(p?call)\('([A-Z]+)'`).FindAllStringSubmatch(lua, -1)
	if len(calls) == 0 {
		t.Fatal("the key-pattern scan calls nothing")
	}
	for _, call := range calls {
		if call[2] != "SCAN" && call[2] != "PTTL" {
			t.Errorf("the key-pattern scan calls %s", call[2])
		}
	}
}

func TestEveryKnownEngineHasAnAdapter(t *testing.T) {
	for _, engine := range []storestate.Engine{storestate.EnginePostgres, storestate.EngineMySQL, storestate.EngineMongoDB, storestate.EngineRedis, storestate.EngineMemcached, storestate.EngineConsul} {
		if a, ok := storestate.AdapterFor(engine); !ok || a.Engine() != engine || a.DefaultPort() == 0 {
			t.Errorf("%s has no complete adapter", engine)
		}
	}
	if _, ok := storestate.AdapterFor("unknown"); ok {
		t.Error("an unknown engine has an adapter")
	}
}
