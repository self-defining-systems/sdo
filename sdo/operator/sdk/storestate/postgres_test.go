package storestate_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// healthyPostgres is synthetic output of the postgres read command on a
// primary with one application database.
const healthyPostgres = `== server
role	primary
conn	6	100
waiting	0
set	default_transaction_read_only	off
set	max_connections	100
set	statement_timeout	0
set	synchronous_standby_names
set	transaction_read_only	off
== dbsettings
exampledb	*	search_path	"$user", public
exampledb	*	app.tenant	set
== roles
appuser	login
admin	superuser login createdb createrole replication bypassrls
== databases
exampledb	t	-1
postgres	t	-1
== catalog exampledb
constraint	public.items	items_qty_check	c 064079ee
grant	appuser		connect=true select=2 insert=2 update=2 delete=2
index	public.items	items_pkey	primary unique fdbb6737
index	public.items	items_sku_key	unique 88d4a8dd
table	public.items		12
table	public.orders		3
== catalog postgres
grant	appuser		connect=true select=0 insert=0 update=0 delete=0
`

func parsePostgres(t *testing.T, out string) storestate.Facts {
	t.Helper()
	return parseAs(t, storestate.EnginePostgres, out)
}

func TestPostgresParsesHealthyFacts(t *testing.T) {
	f := parsePostgres(t, healthyPostgres)
	if f.Engine != storestate.EnginePostgres {
		t.Fatalf("engine = %q", f.Engine)
	}
	if f.Writable == nil || !*f.Writable {
		t.Fatalf("writable = %v", f.Writable)
	}
	if f.Connections == nil || *f.Connections != (storestate.Gauge{Used: 6, Max: 100}) {
		t.Fatalf("connections = %+v", f.Connections)
	}
	for _, want := range []storestate.Item{
		{Kind: storestate.KindRole, Name: "role", Value: "primary"},
		{Kind: storestate.KindSetting, Name: "statement_timeout", Value: "0"},
		{Kind: storestate.KindSetting, Name: "synchronous_standby_names", Value: ""},
		{Kind: storestate.KindSetting, Scope: "exampledb", Name: "app.tenant", Value: "set"},
		{Kind: storestate.KindSetting, Scope: "exampledb", Name: "allow-connections", Value: "t"},
		{Kind: storestate.KindPrincipal, Name: "appuser", Value: "login"},
		{Kind: storestate.KindPrincipal, Scope: "exampledb", Name: "appuser", Value: "connect=true select=2 insert=2 update=2 delete=2"},
		{Kind: storestate.KindNamespace, Scope: "exampledb", Name: "public.items", Value: "12"},
		{Kind: storestate.KindIndex, Scope: "exampledb.public.items", Name: "items_sku_key", Value: "unique 88d4a8dd"},
		{Kind: storestate.KindValidator, Scope: "exampledb.public.items", Name: "items_qty_check", Value: "c 064079ee"},
	} {
		got, ok := item(f, want.Kind, want.Scope, want.Name)
		if !ok || got != want {
			t.Errorf("item %s/%s/%s = %+v, %v", want.Kind, want.Scope, want.Name, got, ok)
		}
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

func TestPostgresStoreFaultsAreTypedChanges(t *testing.T) {
	baseline := parsePostgres(t, healthyPostgres)
	for name, test := range map[string]struct {
		from, to string
		want     []wantChange
		blocked  bool
	}{
		"database made read-only": {"== roles", "exampledb\t*\tdefault_transaction_read_only\ton\n== roles", []wantChange{
			{storestate.KindSetting, "exampledb", "default_transaction_read_only", "", "on"},
			{storestate.KindWriteBlock, "exampledb", "default-transaction-read-only", "", "active"},
		}, true},
		"server made read-only": {"default_transaction_read_only\toff", "default_transaction_read_only\ton", []wantChange{
			{storestate.KindSetting, "", "default_transaction_read_only", "off", "on"},
			{storestate.KindWriteBlock, "", "default-transaction-read-only", "", "active"},
		}, true},
		"role made read-only": {"== roles", "exampledb\tappuser\tdefault_transaction_read_only\ttrue\n== roles", []wantChange{
			{storestate.KindWriteBlock, "appuser@exampledb", "default-transaction-read-only", "", "active"},
		}, true},
		"in recovery": {"role\tprimary", "role\tstandby", []wantChange{
			{storestate.KindRole, "", "role", "primary", "standby"},
			{storestate.KindWriteBlock, "", "in-recovery", "", "active"},
		}, true},
		"password expired": {"appuser\tlogin", "appuser\tlogin expired", []wantChange{
			{storestate.KindPrincipal, "", "appuser", "login", "login expired"},
		}, false},
		"login revoked": {"appuser\tlogin", "appuser\tnologin", []wantChange{
			{storestate.KindPrincipal, "", "appuser", "login", "nologin"},
		}, false},
		"insert revoked": {"select=2 insert=2", "select=2 insert=1", []wantChange{
			{storestate.KindPrincipal, "exampledb", "appuser", "connect=true select=2 insert=2 update=2 delete=2", "connect=true select=2 insert=1 update=2 delete=2"},
		}, false},
		"connections closed to a database": {"exampledb\tt\t-1", "exampledb\tf\t-1", []wantChange{
			{storestate.KindSetting, "exampledb", "allow-connections", "t", "f"},
		}, false},
		"statement timeout": {"statement_timeout\t0", "statement_timeout\t10", []wantChange{
			{storestate.KindSetting, "", "statement_timeout", "0", "10"},
		}, false},
		"table locked": {"table\tpublic.items", "lock\tpublic.items\tAccessExclusiveLock\theld\ntable\tpublic.items", []wantChange{
			{storestate.KindLock, "exampledb.public.items", "AccessExclusiveLock", "", "held"},
		}, false},
		"writers waiting": {"waiting\t0", "waiting\t4", []wantChange{
			{storestate.KindLock, "", "waiting-for-lock", "", "active"},
		}, false},
		"rejecting trigger": {"index\tpublic.items\titems_pkey", "constraint\tpublic.items\treject_writes\ttrigger O 1a2b3c4d\nindex\tpublic.items\titems_pkey", []wantChange{
			{storestate.KindValidator, "exampledb.public.items", "reject_writes", "", "trigger O 1a2b3c4d"},
		}, false},
		"unique index dropped": {"index\tpublic.items\titems_sku_key\tunique 88d4a8dd\n", "", []wantChange{
			{storestate.KindIndex, "exampledb.public.items", "items_sku_key", "unique 88d4a8dd", ""},
		}, false},
		"table dropped": {"table\tpublic.orders\t\t3\n", "", []wantChange{
			{storestate.KindNamespace, "exampledb", "public.orders", "3", ""},
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(healthyPostgres, test.from) {
				t.Fatalf("fixture lacks %q", test.from)
			}
			current := parsePostgres(t, strings.Replace(healthyPostgres, test.from, test.to, 1))
			assertChanges(t, baseline, current, test.want)
			if current.Writable == nil || *current.Writable == test.blocked {
				t.Fatalf("writable = %v, want %v", current.Writable, !test.blocked)
			}
		})
	}
}

func TestPostgresConnectionFloodIsAGaugeOrARefusal(t *testing.T) {
	f := parsePostgres(t, strings.Replace(healthyPostgres, "conn\t6\t100", "conn\t97\t100", 1))
	if f.Connections == nil || !f.Connections.Near(storestate.NearLimit) {
		t.Fatalf("connections = %+v", f.Connections)
	}
	var full []string
	for _, section := range []string{"server", "dbsettings", "roles", "databases"} {
		full = append(full, "== "+section, `psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  sorry, too many clients already`)
	}
	f = parsePostgres(t, strings.Join(full, "\n"))
	if got, _ := item(f, storestate.KindRefusal, "", "server"); got.Value != "max-clients" {
		t.Fatalf("refusal = %+v", f.Items)
	}
	if f.Writable != nil || f.Connections != nil {
		t.Fatalf("a refused read claims facts: %+v", f)
	}
	for _, c := range storestate.Diff(parsePostgres(t, healthyPostgres), f) {
		if c.After == nil {
			t.Errorf("a refused read reports %s as removed", c.Key())
		}
	}
}

func TestPostgresRecordsRefusalsAsFacts(t *testing.T) {
	for name, test := range map[string]struct {
		from, to, scope, section, class string
	}{
		"catalog permission": {"== catalog postgres\ngrant", "== catalog postgres\nERROR:  permission denied for table secrets\ngrant", "postgres", "catalog", "no-permission"},
		"password":           {"== roles\n", "== roles\npsql: error: FATAL:  password authentication failed for user \"appuser\"\n", "", "roles", "auth-failed"},
		"starting":           {"== databases\n", "== databases\npsql: error: FATAL:  the database system is starting up\n", "", "databases", "loading"},
	} {
		t.Run(name, func(t *testing.T) {
			f := parsePostgres(t, strings.Replace(healthyPostgres, test.from, test.to, 1))
			if got, ok := item(f, storestate.KindRefusal, test.scope, test.section); !ok || got.Value != test.class {
				t.Fatalf("refusal = %+v %v in %+v", got, ok, f.Items)
			}
		})
	}
}

func TestPostgresCutSectionsAreUnreadNotRemoved(t *testing.T) {
	baseline := parsePostgres(t, healthyPostgres)
	cut := strings.Replace(healthyPostgres, "== catalog postgres\ngrant\tappuser\t\tconnect=true select=0 insert=0 update=0 delete=0\n", "== more\n", 1)
	for _, c := range storestate.Diff(baseline, parsePostgres(t, cut)) {
		t.Errorf("a cut catalog reports %s", c)
	}
	// A section at its row limit shows only part of its rows.
	var roles []string
	for i := 0; i < 25; i++ {
		roles = append(roles, fmt.Sprintf("role%02d\tlogin", i))
	}
	many := strings.Replace(healthyPostgres, "appuser\tlogin\nadmin\tsuperuser login createdb createrole replication bypassrls\n", strings.Join(roles, "\n")+"\n", 1)
	f := parsePostgres(t, many)
	if _, ok := item(f, storestate.KindPrincipal, "", "role24"); ok {
		t.Error("the row past the limit is kept")
	}
	for _, c := range storestate.Diff(baseline, f) {
		if c.After == nil && c.Key().Kind == storestate.KindPrincipal {
			t.Errorf("a cut role list reports %s as removed", c.Key())
		}
	}
}

func TestPostgresUnavailableReadIsAnError(t *testing.T) {
	var socket []string
	for _, section := range []string{"server", "dbsettings", "roles", "databases"} {
		socket = append(socket, "== "+section, `psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: No such file or directory`)
	}
	for name, out := range map[string]string{
		"empty":            "",
		"no client":        "== server\nsh: 1: psql: not found\n== dbsettings\nsh: 1: psql: not found\n== roles\nsh: 1: psql: not found\n== databases\nsh: 1: psql: not found\n",
		"no server":        strings.Join(socket, "\n"),
		"missing sections": "== server\nrole\tprimary\n",
		"empty server":     "== server\n" + healthyPostgres[strings.Index(healthyPostgres, "== dbsettings"):],
		"malformed line":   strings.Replace(healthyPostgres, "table\tpublic.orders\t\t3", "table\tpublic.orders", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if f, err := adapterFor(t, storestate.EnginePostgres).Parse(out); err == nil {
				t.Fatalf("Parse accepted an unavailable read: %+v", f)
			}
		})
	}
}

func TestPostgresReadCommandIsBoundedAndReadOnly(t *testing.T) {
	text := readCommandIsBounded(t, storestate.EnginePostgres, 5433)
	for _, want := range []string{"-p 5433", "pg_is_in_recovery", "pg_db_role_setting", "rolvaliduntil", "pg_locks", "has_table_privilege", "reltuples"} {
		if !strings.Contains(text, want) {
			t.Errorf("read lacks %q", want)
		}
	}
	for _, forbidden := range regexp.MustCompile(`(?i)\b(insert into|update \w+ set|delete from|alter |drop |create |truncate|grant \w+ on|copy |set role)|pg_terminate|pg_cancel|rolpassword|pg_authid|pg_shadow|echo "?\$PGPASSWORD`).FindAllString(text, -1) {
		t.Errorf("read contains %q", forbidden)
	}
}

func TestPostgresDetectorReportsAReadOnlyDatabase(t *testing.T) {
	c := config(t)
	c.Component, c.Engine = "orders-db", storestate.EnginePostgres
	d := detector(t, c)
	findings, err := d.Detect(context.Background(), promotedSnapshot(t, d, healthyPostgres, reading(healthyPostgres)))
	if err != nil || len(findings) != 0 {
		t.Fatalf("healthy: %+v, %v", findings, err)
	}
	readOnly := strings.Replace(healthyPostgres, "== roles", "exampledb\t*\tdefault_transaction_read_only\ton\n== roles", 1)
	findings, err = d.Detect(context.Background(), promotedSnapshot(t, d, healthyPostgres, reading(readOnly)))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) == 0 || findings[0].RuleID != storestate.RuleWriteBlocked || !strings.Contains(findings[0].Evidence, "default_transaction_read_only off") && !strings.Contains(findings[0].Evidence, "default_transaction_read_only (absent) -> on") {
		t.Fatalf("read-only findings = %+v", findings)
	}
}

// serverSettings replaces the healthy server section's two read-only settings
// with rows in the read's own format: name, effective value, source, and the
// server-scope value.
func serverSettings(defaultRow, txRow string) string {
	out := strings.Replace(healthyPostgres, "set\tdefault_transaction_read_only\toff", defaultRow, 1)
	return strings.Replace(out, "set\ttransaction_read_only\toff", txRow, 1)
}

func TestPostgresScopedSettingIsNotAServerChange(t *testing.T) {
	baseline := parsePostgres(t, healthyPostgres)
	for _, source := range []string{"database", "user", "database user", "client", "session"} {
		t.Run(source, func(t *testing.T) {
			out := serverSettings(
				"set\tdefault_transaction_read_only\ton\t"+source+"\toff",
				"set\ttransaction_read_only\ton\tdefault\ton")
			out = strings.Replace(out, "== roles", "exampledb\tappuser\tdefault_transaction_read_only\ton\n== roles", 1)
			current := parsePostgres(t, out)
			if got, ok := item(current, storestate.KindSetting, "", "default_transaction_read_only"); !ok || got.Value != "off" {
				t.Errorf("server default_transaction_read_only = %+v, %v", got, ok)
			}
			if got, ok := item(current, storestate.KindSetting, "", "transaction_read_only"); !ok || got.Value != "off" {
				t.Errorf("server transaction_read_only = %+v, %v", got, ok)
			}
			scoped := storestate.Key{Kind: storestate.KindSetting, Scope: "appuser@exampledb", Name: "default_transaction_read_only"}
			changes := storestate.Diff(baseline, current)
			var sawScoped bool
			for _, c := range changes {
				if c.Key().Scope == "" {
					t.Errorf("a scoped setting changes the server section: %s", c)
				}
				sawScoped = sawScoped || c.Key() == scoped
			}
			if !sawScoped {
				t.Errorf("the scoped item is not reported: %v", changes)
			}
			if current.Writable == nil || *current.Writable {
				t.Errorf("writable = %v, want false (the role's setting blocks writes)", current.Writable)
			}
		})
	}
}

func TestPostgresServerChangeStillShowsWithASource(t *testing.T) {
	baseline := parsePostgres(t, healthyPostgres)
	out := serverSettings(
		"set\tdefault_transaction_read_only\ton\tconfiguration file\ton",
		"set\ttransaction_read_only\ton\tdefault\ton")
	assertChanges(t, baseline, parsePostgres(t, out), []wantChange{
		{storestate.KindSetting, "", "default_transaction_read_only", "off", "on"},
		{storestate.KindSetting, "", "transaction_read_only", "off", "on"},
		{storestate.KindWriteBlock, "", "default-transaction-read-only", "", "active"},
	})
	// A scoped source that hides a server-wide change keeps the server value.
	out = serverSettings(
		"set\tdefault_transaction_read_only\toff\tdatabase user\ton",
		"set\ttransaction_read_only\toff\tdefault\ton")
	assertChanges(t, baseline, parsePostgres(t, out), []wantChange{
		{storestate.KindSetting, "", "default_transaction_read_only", "off", "on"},
	})
}

func TestPostgresScopedSettingNeedsItsServerValue(t *testing.T) {
	out := serverSettings("set\tdefault_transaction_read_only\ton\tuser", "set\ttransaction_read_only\toff")
	if f, err := adapterFor(t, storestate.EnginePostgres).Parse(out); err == nil {
		t.Fatalf("Parse accepted a scoped row without a server value: %+v", f)
	}
}

func TestPostgresReadAndRestoreVerifyTheServerScopeValue(t *testing.T) {
	a := adapterFor(t, storestate.EnginePostgres)
	text := strings.Join(a.ReadCommand(storestate.Endpoint{Port: 5432}), "\n")
	baseline := parsePostgres(t, healthyPostgres)
	current := parsePostgres(t, strings.Replace(healthyPostgres, "default_transaction_read_only\toff", "default_transaction_read_only\ton", 1))
	for _, c := range storestate.Diff(baseline, current) {
		if c.Key().Kind != storestate.KindSetting || c.Key().Name != "default_transaction_read_only" {
			continue
		}
		cmds, err := storestate.RestoreCommands(storestate.EnginePostgres, storestate.Endpoint{Port: 5432}, c, baseline, storestate.RestoreOptions{})
		if err != nil || len(cmds) != 1 {
			t.Fatalf("restore = %v, %v", cmds, err)
		}
		// The server restore judges the server's own value, never the
		// session's: scoped sources are told apart.
		restoreText := strings.Join(cmds[0], "\n")
		for _, want := range []string{"scoped:", "'database user'", "pg_show_all_file_settings"} {
			if !strings.Contains(restoreText, want) {
				t.Errorf("server restore lacks %q", want)
			}
		}
	}
	for _, want := range []string{"boot_val", "pg_show_all_file_settings", "'database user'"} {
		if !strings.Contains(text, want) {
			t.Errorf("read lacks %q", want)
		}
	}
}

// A baseline promoted before the read carried sources has set rows of three
// fields; it parses as server-scope values and compares equal to a healthy
// read in the current format.
func TestPostgresLegacyBaselineComparesWithSourcedReads(t *testing.T) {
	legacy := parsePostgres(t, healthyPostgres)
	sourced := healthyPostgres
	for _, row := range []string{"default_transaction_read_only\toff", "transaction_read_only\toff", "max_connections\t100", "statement_timeout\t0"} {
		sourced = strings.Replace(sourced, "set\t"+row, "set\t"+row+"\tdefault\t"+row[strings.Index(row, "\t")+1:], 1)
	}
	if changes := storestate.Diff(legacy, parsePostgres(t, sourced)); len(changes) != 0 {
		t.Fatalf("a legacy baseline differs from the same state read with sources: %v", changes)
	}
}

func TestPostgresRefusedSettingsStatementRefusesTheServerSection(t *testing.T) {
	out := strings.Replace(healthyPostgres, "set\tdefault_transaction_read_only\toff\n", "ERROR:  permission denied for view pg_settings\n", 1)
	f := parsePostgres(t, out)
	if got, ok := item(f, storestate.KindRefusal, "", "server"); !ok || got.Value != "no-permission" {
		t.Fatalf("refusal = %+v, %v", got, ok)
	}
}
