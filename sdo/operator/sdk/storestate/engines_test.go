package storestate_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storestate"
)

// Synthetic reads of stock images: a memcached cache, a MySQL primary and a
// single consul server agent.
const healthyMemcached = `== stats
STAT version 1.6.7
STAT curr_connections 6
STAT rejected_connections 0
STAT limit_maxbytes 67108864
STAT accepting_conns 1
STAT bytes 1024
STAT curr_items 12
STAT evictions 0
END
== settings
STAT maxbytes 67108864
STAT maxconns 1024
STAT evictions on
STAT item_size_max 1048576
STAT lru_crawler yes
END
`

const healthyMySQL = "== vars\nmax_connections\t151\noffline_mode\tOFF\nread_only\tOFF\nsuper_read_only\tOFF\ninnodb_read_only\tOFF\nsql_mode\tSTRICT_TRANS_TABLES\n" +
	"== status\nThreads_connected\t3\nThreads_running\t1\n== role\nprimary\n" +
	"== users\napp\t%\t2026-01-01T00:00:00\tunlocked global=1 schema=18 table=0\nroot\t%\t2026-01-01T00:00:00\tunlocked global=28 schema=0 table=0\n" +
	"== locks\nwaiting\t0\ntrx-waiting\t0\n== mdl\n== indexes\nshop\titems\tPRIMARY\tunique b80bb774\n== tables\nshop\titems\t40\nshop\torders\t80\n"

const healthyConsul = "== info\nleader = true\nleader_addr = 127.0.0.1:8300\nserver = true\nnum_peers = 0\nstate = Leader\n" +
	"== services\nconsul\nsrv-a\nsrv-b\n== instances\nconsul\t1\nsrv-a\t2\nsrv-b\t1\n== nodes\nnode-1\n" +
	"== members\nnode-1\talive\tserver\n== kv\nprefix\tconfig/\t12\nprefix\tflags\t1\ntotal\t13\n== acl\ndisabled\n"

func lookup(t *testing.T, f storestate.Facts, kind storestate.ItemKind, scope, name string) string {
	t.Helper()
	it, ok := f.Lookup(storestate.Key{Kind: kind, Scope: scope, Name: name})
	if !ok {
		t.Fatalf("no %s %s/%s in %+v", kind, scope, name, f.Items)
	}
	return it.Value
}

func TestMemcachedFacts(t *testing.T) {
	f := parseAs(t, storestate.EngineMemcached, healthyMemcached)
	if f.Writable == nil || !*f.Writable || f.Connections == nil || f.Connections.Max != 1024 || f.Memory == nil || f.Memory.Max != 67108864 {
		t.Fatalf("facts = %+v", f)
	}
	if lookup(t, f, storestate.KindNamespace, "", "curr_items") != "12" || lookup(t, f, storestate.KindSetting, "", "evictions") != "on" {
		t.Fatalf("items = %+v", f.Items)
	}
	full := strings.NewReplacer("STAT bytes 1024", "STAT bytes 67108864", "STAT evictions on", "STAT evictions off").Replace(healthyMemcached)
	blocked := parseAs(t, storestate.EngineMemcached, full)
	if blocked.Writable == nil || *blocked.Writable || lookup(t, blocked, storestate.KindWriteBlock, "", "oom") != "active" {
		t.Fatalf("a full cache without eviction is writable: %+v", blocked)
	}
	if _, err := adapterFor(t, storestate.EngineMemcached).Parse("== stats\nbash: connect: Connection refused\n== settings\n"); err == nil {
		t.Fatal("an unreachable cache parsed")
	}
}

// A full cache that evicts is healthy; one that cannot evict is reported.
func TestMemcachedMemoryNearLimitOnlyWithoutEviction(t *testing.T) {
	c := config(t)
	c.Engine = storestate.EngineMemcached
	d := detector(t, c)
	full := strings.Replace(healthyMemcached, "STAT bytes 1024", "STAT bytes 67000000", 1)
	if findings, err := d.Detect(context.Background(), snapshot(t, d, reading(full))); err != nil || len(findings) != 0 {
		t.Fatalf("an evicting full cache fired: %+v, %v", findings, err)
	}
	noEviction := strings.Replace(full, "STAT evictions on", "STAT evictions off", 1)
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(noEviction)))
	if err != nil || len(findings) != 1 || findings[0].RuleID != storestate.RuleMemoryNearLimit {
		t.Fatalf("findings = %+v, %v", findings, err)
	}
}

func TestMySQLStoreFaultsAreTypedChanges(t *testing.T) {
	baseline := parseAs(t, storestate.EngineMySQL, healthyMySQL)
	if baseline.Writable == nil || !*baseline.Writable || baseline.Connections == nil || *baseline.Connections != (storestate.Gauge{Used: 3, Max: 151}) {
		t.Fatalf("baseline = %+v", baseline)
	}
	for name, test := range map[string]struct {
		read string
		want []wantChange
	}{
		"writes blocked": {strings.Replace(healthyMySQL, "super_read_only\tOFF", "super_read_only\tON", 1), []wantChange{
			{storestate.KindSetting, "", "super_read_only", "OFF", "ON"}, {storestate.KindWriteBlock, "", "super-read-only", "", "active"}}},
		"account locked": {strings.Replace(healthyMySQL, "00:00:00\tunlocked global=1", "00:00:00\tlocked global=1", 1), []wantChange{
			{storestate.KindPrincipal, "", "app@%", "unlocked global=1 schema=18 table=0", "locked global=1 schema=18 table=0"}}},
		"password changed": {strings.Replace(healthyMySQL, "app\t%\t2026-01-01T00:00:00", "app\t%\t2026-02-02T00:00:00", 1), []wantChange{
			{storestate.KindSetting, "app@%", "password-changed", "2026-01-01T00:00:00", "2026-02-02T00:00:00"}}},
		"grants revoked": {strings.Replace(healthyMySQL, "schema=18", "schema=0", 1), []wantChange{
			{storestate.KindPrincipal, "", "app@%", "unlocked global=1 schema=18 table=0", "unlocked global=1 schema=0 table=0"}}},
		"global read lock": {strings.Replace(healthyMySQL, "== mdl\n", "== mdl\nGLOBAL\t\t\tSHARED\n", 1), []wantChange{
			{storestate.KindLock, "", "global-read-lock", "", "held"}, {storestate.KindWriteBlock, "", "global-read-lock", "", "active"}}},
		"table renamed": {strings.Replace(healthyMySQL, "shop\torders\t80", "shop\torders_old\t80", 1), []wantChange{
			{storestate.KindNamespace, "shop", "orders", "80", ""}, {storestate.KindNamespace, "shop", "orders_old", "", "80"}}},
		"index dropped": {strings.Replace(healthyMySQL, "shop\titems\tPRIMARY\tunique b80bb774\n", "", 1), []wantChange{
			{storestate.KindIndex, "shop.items", "PRIMARY", "unique b80bb774", ""}}},
	} {
		t.Run(name, func(t *testing.T) {
			assertChanges(t, baseline, parseAs(t, storestate.EngineMySQL, test.read), test.want)
		})
	}
	flood := strings.Replace(healthyMySQL, "Threads_connected\t3", "Threads_connected\t151", 1)
	if f := parseAs(t, storestate.EngineMySQL, flood); !f.Connections.Near(storestate.NearLimit) {
		t.Fatalf("a connection flood is not near the cap: %+v", f.Connections)
	}
}

func TestMySQLRecordsRefusalsAsFacts(t *testing.T) {
	var refused strings.Builder
	for _, s := range []string{"vars", "status", "role", "users", "tables", "indexes", "locks", "mdl"} {
		refused.WriteString("== " + s + "\nERROR 1040 (HY000): Too many connections\n")
	}
	f := parseAs(t, storestate.EngineMySQL, refused.String())
	if lookup(t, f, storestate.KindRefusal, "", "vars") != "max-clients" || f.Writable != nil {
		t.Fatalf("facts = %+v", f)
	}
	unreachable := strings.ReplaceAll(refused.String(), "ERROR 1040 (HY000): Too many connections", "ERROR 2003 (HY000): Can't connect to MySQL server on '127.0.0.1:3306' (111)")
	if _, err := adapterFor(t, storestate.EngineMySQL).Parse(unreachable); err == nil {
		t.Fatal("an unreachable server parsed")
	}
	// The budget ran out in indexes: indexes and the absent tables are
	// unread, the locks read before them are not.
	cut := healthyMySQL[:strings.Index(healthyMySQL, "== tables")] + "== more\n"
	if f := parseAs(t, storestate.EngineMySQL, cut); strings.Join(kinds(f.Unread), ",") != "index,namespace" {
		t.Fatalf("a cut catalog: unread = %v, want index,namespace", f.Unread)
	}
}

func kinds(ks []storestate.ItemKind) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

func TestConsulFacts(t *testing.T) {
	f := parseAs(t, storestate.EngineConsul, healthyConsul)
	if lookup(t, f, storestate.KindRole, "", "leader") != "self" || lookup(t, f, storestate.KindRegistryEntry, "service", "srv-a") != "2" ||
		lookup(t, f, storestate.KindRegistryEntry, "", "services") != "3" || lookup(t, f, storestate.KindNamespace, "kv", "config/") != "12" ||
		lookup(t, f, storestate.KindSetting, "", "acl") != "disabled" || lookup(t, f, storestate.KindRegistryEntry, "member", "node-1") != "alive server" {
		t.Fatalf("facts = %+v", f.Items)
	}
	noLeader := strings.Replace(strings.Replace(healthyConsul, "leader = true\nleader_addr = 127.0.0.1:8300\n", "leader = false\n", 1), "state = Leader", "state = Candidate", 1)
	if f := parseAs(t, storestate.EngineConsul, noLeader); f.Writable == nil || *f.Writable {
		t.Fatalf("a cluster without a leader is writable: %+v", f)
	}
	if _, err := adapterFor(t, storestate.EngineConsul).Parse(strings.Replace(healthyConsul, "leader = true", "ERR Error querying agent: dial tcp 127.0.0.1:8500: connect: connection refused", 1)); err == nil {
		t.Fatal("an unreachable agent parsed")
	}
}

func storeDetector(t *testing.T, engine storestate.Engine, healthy string) (sdk.Detector, func(string) []sdk.Finding) {
	t.Helper()
	baseline := parseAs(t, engine, healthy)
	baseline.Component = "cache"
	baseline.Pod = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "cache-0"}
	c := config(t, baseline)
	c.Engine = engine
	d := detector(t, c)
	return d, func(read string) []sdk.Finding {
		t.Helper()
		findings, err := d.Detect(context.Background(), snapshot(t, d, reading(read)))
		if err != nil {
			t.Fatal(err)
		}
		return findings
	}
}

func ruleSet(findings []sdk.Finding) map[string]int {
	out := map[string]int{}
	for _, f := range findings {
		out[f.RuleID]++
	}
	return out
}

// Losing a registered service is a registry finding; a new service is not.
func TestConsulRegistrationsLostIsAFinding(t *testing.T) {
	_, detect := storeDetector(t, storestate.EngineConsul, healthyConsul)
	if f := detect(healthyConsul); len(f) != 0 {
		t.Fatalf("healthy: %+v", f)
	}
	lost := strings.NewReplacer("srv-a\nsrv-b\n", "", "srv-a\t2\nsrv-b\t1\n", "").Replace(healthyConsul)
	findings := detect(lost)
	if ruleSet(findings)[storestate.RuleRegistryChanged] != 2 || !strings.Contains(findings[0].Summary, "disappeared") {
		t.Fatalf("findings = %+v", findings)
	}
	fewer := strings.Replace(healthyConsul, "srv-a\t2", "srv-a\t1", 1)
	if f := detect(fewer); ruleSet(f)[storestate.RuleRegistryChanged] != 1 {
		t.Fatalf("an instance loss: %+v", f)
	}
	added := strings.NewReplacer("srv-b\n", "srv-b\nsrv-c\n", "srv-b\t1\n", "srv-b\t1\nsrv-c\t1\n").Replace(healthyConsul)
	if f := detect(added); ruleSet(f)[storestate.RuleRegistryChanged] != 0 {
		t.Fatalf("a new service fired: %+v", f)
	}
}

// A table that disappeared or shrank by more than half is a namespace finding;
// one that appeared is cited, not judged by itself. Growth is not judged: an
// application writes its tables, so a count grows with time and traffic past
// any multiple of a healthy reading (a timeline collection grew tenfold in 25
// minutes of ordinary posting).
func TestNamespaceChangedRule(t *testing.T) {
	_, detect := storeDetector(t, storestate.EngineMySQL, healthyMySQL)
	for name, test := range map[string]struct {
		read string
		want int
		in   string
	}{
		"healthy":           {healthyMySQL, 0, ""},
		"renamed":           {strings.Replace(healthyMySQL, "shop\torders\t80", "shop\torders_old\t80", 1), 1, "appeared since the healthy baseline: namespace shop/orders_old (80)"},
		"emptied":           {strings.Replace(healthyMySQL, "shop\torders\t80", "shop\torders\t10", 1), 1, "dropped from 80 to 10"},
		"grew tenfold":      {strings.Replace(healthyMySQL, "shop\torders\t80", "shop\torders\t10080", 1), 0, ""},
		"within bounds":     {strings.Replace(healthyMySQL, "shop\torders\t80", "shop\torders\t60", 1), 0, ""},
		"new table":         {healthyMySQL + "shop\tsessions\t5\n", 0, ""},
		"estimated growth":  {strings.Replace(healthyMySQL, "shop\torders\t80", "shop\torders\t~900000", 1), 0, ""},
		"small table moved": {strings.Replace(healthyMySQL, "shop\titems\t40", "shop\titems\t1", 1), 1, "dropped from 40 to 1"},
	} {
		t.Run(name, func(t *testing.T) {
			findings := detect(test.read)
			if got := ruleSet(findings)[storestate.RuleNamespaceChanged]; got != test.want {
				t.Fatalf("namespace findings = %d, want %d: %+v", got, test.want, findings)
			}
			if test.in != "" && !strings.Contains(findings[0].Summary+" "+findings[0].Evidence, test.in) {
				t.Fatalf("finding = %+v, want %q", findings[0], test.in)
			}
		})
	}
}

// Redis key patterns: a rename of every "{n}" key to "{n}:*" is a
// namespace finding, and a read without the optional section still parses.
func TestRedisKeyPatterns(t *testing.T) {
	withKeys := healthyRedis + "== keys\ncomplete\n{n}\t40\nuser:*\t12\n"
	f := parse(t, withKeys)
	if lookup(t, f, storestate.KindKeyPattern, "db0", "{n}") != "40" {
		t.Fatalf("items = %+v", f.Items)
	}
	if old := parse(t, healthyRedis); !strings.Contains(strings.Join(kinds(old.Unread), ","), "key-pattern") {
		t.Fatalf("a read without key patterns claims them: %v", old.Unread)
	}
	if partial := parse(t, healthyRedis+"== keys\npartial\n{n}\t40\n"); len(partial.Items) != len(parse(t, healthyRedis).Items) {
		t.Fatalf("a partial scan reported patterns: %+v", partial.Items)
	}
	_, detect := storeDetector(t, storestate.EngineRedis, withKeys)
	renamed := healthyRedis + "== keys\ncomplete\n{n}:*\t40\nuser:*\t12\n"
	findings := detect(renamed)
	if ruleSet(findings)[storestate.RuleNamespaceChanged] != 1 || !strings.Contains(findings[0].Evidence, "key-pattern db0/{n}:*") {
		t.Fatalf("findings = %+v", findings)
	}
}

func TestNewReadCommandsAreBoundedAndReadOnly(t *testing.T) {
	my := readCommandIsBounded(t, storestate.EngineMySQL, 3307)
	for _, want := range []string{`-P "$PORT"`, "PORT=3307", "super_read_only", "account_locked", "information_schema.tables", "metadata_locks", "--defaults-extra-file=/dev/fd/3"} {
		if !strings.Contains(my, want) {
			t.Errorf("mysql read lacks %q", want)
		}
	}
	for _, forbidden := range regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|DROP|ALTER|GRANT|REVOKE|TRUNCATE|KILL|FLUSH)\b|authentication_string|-p"?\$`).FindAllString(my, -1) {
		t.Errorf("mysql read contains %q", forbidden)
	}
	for engine, writes := range map[storestate.Engine]*regexp.Regexp{
		storestate.EngineMemcached: regexp.MustCompile(`\b(set|add|replace|append|prepend|cas|delete|incr|decr|touch|flush_all|verbosity|shutdown)\b `),
		storestate.EngineConsul:    regexp.MustCompile(`\b(kv (put|delete)|services (register|deregister)|leave|force-leave|acl (token|policy) (create|update|delete)|snapshot)\b|kv get -recurse|SecretID`),
	} {
		argv := adapterFor(t, engine).ReadCommand(storestate.Endpoint{Port: 1})
		for _, arg := range argv {
			if len(arg) > 1024 {
				t.Fatalf("%s read argument is %d bytes", engine, len(arg))
			}
		}
		if found := writes.FindAllString(strings.Join(argv, " "), -1); len(found) > 0 {
			t.Errorf("%s read contains %q", engine, found)
		}
	}
}
