package storestate_test

import (
	"fmt"
	"context"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// healthyMongo is synthetic output of the mongodb read command on a
// standalone server with one application database.
const healthyMongo = `== server
role	standalone
conn	4	838860
set	authorization	enabled
set	replSetName	-
set	storageReadOnly	false
== params
set	cursorTimeoutMillis	600000
set	notablescan	false
== locks
fsync	0
waiting	0
== users
user	admin	root	root@admin
user	exampledb	appuser	readWrite@exampledb
== catalog
db	admin	0
db	exampledb	0
coll	exampledb	items	12	-
index	exampledb	items	sku_1	sku:1 unique
coll	exampledb	orders	3	moderate/warn
index	exampledb	orders	placed_1	placed:-1 ttl=3600
`

func adapterFor(t *testing.T, engine storestate.Engine) storestate.Adapter {
	t.Helper()
	a, ok := storestate.AdapterFor(engine)
	if !ok {
		t.Fatalf("no %s adapter", engine)
	}
	return a
}

func parseAs(t *testing.T, engine storestate.Engine, out string) storestate.Facts {
	t.Helper()
	f, err := adapterFor(t, engine).Parse(out)
	if err != nil {
		t.Fatalf("Parse %s: %v", engine, err)
	}
	return f
}

func parseMongo(t *testing.T, out string) storestate.Facts {
	t.Helper()
	return parseAs(t, storestate.EngineMongoDB, out)
}

func TestMongoParsesHealthyFacts(t *testing.T) {
	f := parseMongo(t, healthyMongo)
	if f.Engine != storestate.EngineMongoDB {
		t.Fatalf("engine = %q", f.Engine)
	}
	if f.Writable == nil || !*f.Writable {
		t.Fatalf("writable = %v", f.Writable)
	}
	if f.Connections == nil || *f.Connections != (storestate.Gauge{Used: 4, Max: 838860}) {
		t.Fatalf("connections = %+v", f.Connections)
	}
	for _, want := range []storestate.Item{
		{Kind: storestate.KindRole, Name: "role", Value: "standalone"},
		{Kind: storestate.KindSetting, Name: "authorization", Value: "enabled"},
		{Kind: storestate.KindSetting, Name: "notablescan", Value: "false"},
		{Kind: storestate.KindSetting, Scope: "exampledb", Name: "profile", Value: "0"},
		{Kind: storestate.KindPrincipal, Scope: "exampledb", Name: "appuser", Value: "readWrite@exampledb"},
		{Kind: storestate.KindNamespace, Scope: "exampledb", Name: "items", Value: "12"},
		{Kind: storestate.KindIndex, Scope: "exampledb.items", Name: "sku_1", Value: "sku:1 unique"},
		{Kind: storestate.KindIndex, Scope: "exampledb.orders", Name: "placed_1", Value: "placed:-1 ttl=3600"},
		{Kind: storestate.KindValidator, Scope: "exampledb", Name: "orders", Value: "moderate/warn"},
	} {
		got, ok := item(f, want.Kind, want.Scope, want.Name)
		if !ok || got != want {
			t.Errorf("item %s/%s/%s = %+v, %v", want.Kind, want.Scope, want.Name, got, ok)
		}
	}
	if _, ok := item(f, storestate.KindValidator, "exampledb", "items"); ok {
		t.Error("a collection without a validator has a validator item")
	}
	for _, it := range f.Items {
		if it.Kind == storestate.KindWriteBlock || it.Kind == storestate.KindRefusal || it.Kind == storestate.KindLock {
			t.Errorf("healthy facts carry %+v", it)
		}
	}
	if len(f.Unread) != 0 {
		t.Errorf("unread = %v", f.Unread)
	}
}

// wantChange is one typed change a store-state fault must produce.
type wantChange struct {
	kind          storestate.ItemKind
	scope, name   string
	before, after string
}

func assertChanges(t *testing.T, baseline, current storestate.Facts, want []wantChange) {
	t.Helper()
	changes := storestate.Diff(baseline, current)
	for _, w := range want {
		found := false
		for _, c := range changes {
			k := c.Key()
			if k.Kind == w.kind && k.Scope == w.scope && k.Name == w.name && value(c.Before) == w.before && value(c.After) == w.after {
				found = true
			}
		}
		if !found {
			t.Errorf("no change %s %s/%s %q -> %q in %v", w.kind, w.scope, w.name, w.before, w.after, changes)
		}
	}
}

func TestMongoStoreFaultsAreTypedChanges(t *testing.T) {
	baseline := parseMongo(t, healthyMongo)
	for name, test := range map[string]struct {
		from, to string
		want     []wantChange
		blocked  bool
	}{
		"fsync lock": {"fsync\t0\nwaiting\t0", "fsync\t1\nwaiting\t2", []wantChange{
			{storestate.KindLock, "", "fsync", "", "held"},
			{storestate.KindLock, "", "waiting-for-lock", "", "active"},
			{storestate.KindWriteBlock, "", "fsync-lock", "", "active"},
		}, true},
		"validator rejecting writes": {"coll\texampledb\titems\t12\t-", "coll\texampledb\titems\t12\tstrict/error", []wantChange{
			{storestate.KindValidator, "exampledb", "items", "", "strict/error"},
		}, false},
		"validator action changed": {"moderate/warn", "moderate/error", []wantChange{
			{storestate.KindValidator, "exampledb", "orders", "moderate/warn", "moderate/error"},
		}, false},
		"unique index added": {"coll\texampledb\torders\t3", "index\texampledb\titems\tname_1_unique\tname:1 unique\ncoll\texampledb\torders\t3", []wantChange{
			{storestate.KindIndex, "exampledb.items", "name_1_unique", "", "name:1 unique"},
		}, false},
		"index dropped": {"index\texampledb\titems\tsku_1\tsku:1 unique\n", "", []wantChange{
			{storestate.KindIndex, "exampledb.items", "sku_1", "sku:1 unique", ""},
		}, false},
		"collection renamed": {"coll\texampledb\titems\t12\t-\nindex\texampledb\titems\t", "coll\texampledb\titems_old\t12\t-\nindex\texampledb\titems_old\t", []wantChange{
			{storestate.KindNamespace, "exampledb", "items", "12", ""},
			{storestate.KindNamespace, "exampledb", "items_old", "", "12"},
			{storestate.KindIndex, "exampledb.items", "sku_1", "sku:1 unique", ""},
		}, false},
		"bulk import": {"items\t12\t-", "items\t10012\t-", []wantChange{
			{storestate.KindNamespace, "exampledb", "items", "12", "10012"},
		}, false},
		"stepped down": {"role\tstandalone", "role\tsecondary", []wantChange{
			{storestate.KindRole, "", "role", "standalone", "secondary"},
			{storestate.KindWriteBlock, "", "not-primary", "", "active"},
		}, true},
		"role revoked": {"readWrite@exampledb", "read@exampledb", []wantChange{
			{storestate.KindPrincipal, "exampledb", "appuser", "readWrite@exampledb", "read@exampledb"},
		}, false},
		"profiling switched on": {"db\texampledb\t0", "db\texampledb\t2", []wantChange{
			{storestate.KindSetting, "exampledb", "profile", "0", "2"},
		}, false},
		"table scans refused": {"notablescan\tfalse", "notablescan\ttrue", []wantChange{
			{storestate.KindSetting, "", "notablescan", "false", "true"},
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(healthyMongo, test.from) {
				t.Fatalf("fixture lacks %q", test.from)
			}
			current := parseMongo(t, strings.Replace(healthyMongo, test.from, test.to, 1))
			assertChanges(t, baseline, current, test.want)
			if current.Writable == nil || *current.Writable == test.blocked {
				t.Fatalf("writable = %v, want %v", current.Writable, !test.blocked)
			}
		})
	}
}

func TestMongoRecordsRefusalsAsFacts(t *testing.T) {
	var lines []string
	for _, line := range strings.Split(healthyMongo, "\n") {
		if strings.HasPrefix(line, "== ") {
			lines = append(lines, line, "ERR Unauthorized command getCmdLineOpts requires authentication")
		}
	}
	f := parseMongo(t, strings.Join(lines, "\n"))
	if f.Writable != nil || f.Connections != nil {
		t.Fatalf("a refused read claims facts: %+v", f)
	}
	for _, section := range []string{"server", "params", "locks", "users", "catalog"} {
		if got, ok := item(f, storestate.KindRefusal, "", section); !ok || got.Value != "auth-required" {
			t.Errorf("refusal %s = %+v %v", section, got, ok)
		}
	}
	for _, c := range storestate.Diff(parseMongo(t, healthyMongo), f) {
		if c.After == nil {
			t.Errorf("a refused read reports %s as removed", c.Key())
		}
	}

	f = parseMongo(t, strings.Replace(healthyMongo, "== users\n", "== users\nERR Unauthorized not authorized on admin to execute command { usersInfo: 1 }\n", 1))
	if got, _ := item(f, storestate.KindRefusal, "", "users"); got.Value != "no-permission" {
		t.Fatalf("users refusal = %+v", got)
	}
	if f.Writable == nil || !*f.Writable {
		t.Fatalf("a refused users section hides writability: %v", f.Writable)
	}

	// A wrong password ends the shell before the script runs.
	f = parseMongo(t, "MongoServerError: Authentication failed.\n")
	if got, _ := item(f, storestate.KindRefusal, "", "server"); got.Value != "auth-failed" {
		t.Fatalf("auth failure = %+v", f.Items)
	}
}

func TestMongoTruncatedCatalogIsUnreadNotRemoved(t *testing.T) {
	cut := strings.Replace(healthyMongo, "coll\texampledb\torders\t3\tmoderate/warn\nindex\texampledb\torders\tplaced_1\tplaced:-1 ttl=3600\n", "more\n", 1)
	f := parseMongo(t, cut)
	for _, c := range storestate.Diff(parseMongo(t, healthyMongo), f) {
		t.Errorf("a truncated catalog reports %s", c)
	}
	f = parseMongo(t, strings.Replace(healthyMongo, "user\texampledb\tappuser\treadWrite@exampledb\n", "more\n", 1))
	if len(storestate.Diff(parseMongo(t, healthyMongo), f)) != 0 {
		t.Error("truncated users report a removal")
	}
}

func TestMongoUnavailableReadIsAnError(t *testing.T) {
	for name, out := range map[string]string{
		"empty":            "",
		"no shell":         "sh: 1: mongosh: not found\n",
		"refused":          "MongoNetworkError: connect ECONNREFUSED 127.0.0.1:27017\n",
		"legacy refused":   "Error: couldn't connect to server 127.0.0.1:27017, connection attempt failed: SocketException\nexception: connect failed\n",
		"missing sections": "== server\nrole\tstandalone\n",
		"network in eval":  strings.Replace(healthyMongo, "== params\n", "== params\nERR - connection 1 to 127.0.0.1:27017 timed out\n", 1),
		"malformed line":   strings.Replace(healthyMongo, "fsync\t0", "fsync", 1),
		"repeated section": healthyMongo + "== users\n",
	} {
		t.Run(name, func(t *testing.T) {
			if f, err := adapterFor(t, storestate.EngineMongoDB).Parse(out); err == nil {
				t.Fatalf("Parse accepted an unavailable read: %+v", f)
			}
		})
	}
}

// readCommandIsBounded checks a split read against the pod-read bounds and
// returns its text.
func readCommandIsBounded(t *testing.T, engine storestate.Engine, port int) string {
	t.Helper()
	argv := adapterFor(t, engine).ReadCommand(storestate.Endpoint{Port: port})
	// sh -c <credential prelude> $0 <engine script> $1...
	if len(argv) < 5 || argv[0] != "sh" || argv[1] != "-c" {
		t.Fatalf("argv = %q", argv)
	}
	for n := 10; n <= len(argv)-5; n++ {
		if !strings.Contains(argv[4], fmt.Sprintf("${%d}", n)) {
			t.Fatalf("the read passes %d positional arguments but its script never reads ${%d} (sh reads $10 as $1 then 0)", len(argv)-5, n)
		}
	}
	q := podread.Query{ID: "store-state-x", Target: podread.Target{Labels: map[string]string{"app": "x"}}, Args: argv, Interval: podread.MinInterval}
	if err := q.Validate(); err != nil {
		t.Fatalf("read command is not a valid pod read: %v", err)
	}
	return strings.Join(argv, "")
}

func TestMongoReadCommandIsBoundedAndReadOnly(t *testing.T) {
	text := readCommandIsBounded(t, storestate.EngineMongoDB, 27018)
	for _, want := range []string{"--port 27018", "fsyncLock", "usersInfo", "getCollectionInfos", "listIndexes", "mongosh", "command -v mongo"} {
		if !strings.Contains(text, want) {
			t.Errorf("read lacks %q", want)
		}
	}
	for _, forbidden := range regexp.MustCompile(`\.(insert|update|delete|remove|drop|find|aggregate|createIndex|dropIndex|fsyncLock|fsyncUnlock|shutdownServer)\w*\(|collMod|showCredentials|\$currentOp|echo "?\$p`).FindAllString(text, -1) {
		t.Errorf("read contains %q", forbidden)
	}
}

// TestParseNeverClaimsMoreThanItRead mutates healthy reads at random: a parse
// either fails or returns valid facts, and never calls a store writable while
// its read shows a write block.
func TestParseNeverClaimsMoreThanItRead(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	cases := map[storestate.Engine]string{storestate.EngineMongoDB: healthyMongo, storestate.EnginePostgres: healthyPostgres,
		storestate.EngineMySQL: healthyMySQL, storestate.EngineMemcached: healthyMemcached, storestate.EngineConsul: healthyConsul}
	for engine, healthy := range cases {
		a := adapterFor(t, engine)
		lines := strings.Split(healthy, "\n")
		for i := 0; i < 3000; i++ {
			mutated := append([]string(nil), lines...)
			for n := random.Intn(4) + 1; n > 0; n-- {
				j := random.Intn(len(mutated))
				switch random.Intn(4) {
				case 0:
					mutated = append(mutated[:j], mutated[j+1:]...)
				case 1:
					mutated = append(mutated[:j+1], mutated[j:]...)
				case 2:
					mutated[j] = strings.Replace(mutated[j], "\t", " ", 1)
				default:
					mutated[j] = "ERR Unauthorized not authorized"
				}
				if len(mutated) == 0 {
					break
				}
			}
			f, err := a.Parse(strings.Join(mutated, "\n"))
			if err != nil {
				continue
			}
			if err := f.Validate(); err != nil {
				t.Fatalf("%s parse returned invalid facts: %v", engine, err)
			}
			blocked := false
			for _, it := range f.Items {
				blocked = blocked || it.Kind == storestate.KindWriteBlock
			}
			if blocked && f.Writable != nil && *f.Writable {
				t.Fatalf("%s facts are writable with a write block: %+v", engine, f)
			}
		}
	}
}

func TestMongoDetectorReportsAnFsyncLock(t *testing.T) {
	c := config(t)
	c.Component, c.Engine = "orders-db", storestate.EngineMongoDB
	d := detector(t, c)
	findings, err := d.Detect(context.Background(), promotedSnapshot(t, d, healthyMongo, reading(healthyMongo)))
	if err != nil || len(findings) != 0 {
		t.Fatalf("healthy: %+v, %v", findings, err)
	}
	locked := strings.Replace(healthyMongo, "fsync\t0", "fsync\t1", 1)
	findings, err = d.Detect(context.Background(), promotedSnapshot(t, d, healthyMongo, reading(locked)))
	if err != nil {
		t.Fatal(err)
	}
	rules := map[string]bool{}
	for _, f := range findings {
		rules[f.RuleID] = true
	}
	if !rules[storestate.RuleWriteBlocked] || !rules[storestate.RuleCatalogChanged] {
		t.Fatalf("fsync lock findings = %+v", findings)
	}
}
