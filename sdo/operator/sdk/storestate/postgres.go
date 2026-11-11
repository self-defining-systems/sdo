package storestate

import (
	"fmt"
	"strings"
)

// postgresSections are the server-wide sections of the postgres read, in
// order; each header must be present for the read to count. "catalog <db>"
// sections follow, one per database that accepts connections (at most 8), and
// "more" marks a catalog cut at the output budget.
var postgresSections = []string{"server", "dbsettings", "roles", "databases"}

// postgresCatalogKinds are the item kinds a catalog section shows.
var postgresCatalogKinds = []ItemKind{KindNamespace, KindIndex, KindValidator, KindLock, KindPrincipal}

// postgresSectionKinds are the item kinds each server-wide section shows, so
// a refused or overflowing section leaves them unread.
var postgresSectionKinds = map[string][]ItemKind{
	"server":     {KindRole, KindSetting, KindLock, KindWriteBlock},
	"dbsettings": {KindSetting, KindWriteBlock},
	"roles":      {KindPrincipal},
	"databases":  {KindSetting},
}

// postgresRowLimits are the row limits of the bounded sections: a section
// that returns its limit has more rows than the read shows, and its kinds are
// unread.
var postgresRowLimits = map[string]int{"dbsettings": 33, "roles": 25, "databases": 17}

// postgresScript runs psql over the container's own socket as the user the
// credential prelude found ($U, else postgres), so local trust applies; the
// password it found ($P) reaches psql through the environment and is never
// printed. The queries arrive as positional arguments ($5 is the holders
// query, $6 the TCP fallback, $7 onwards the catalog query, split). Holders
// fall back to the TCP table when the server refuses the session list. The
// catalog is read in each database that accepts connections; its query bounds
// its own size (postgresCatalog), and the whole read is still cut at the
// output budget with a "== more" marker.
const postgresScript = `U=${U:-postgres};D=${POSTGRES_DB:-${POSTGRESQL_DATABASE:-${PGDATABASE:-$U}}};T=$(printf '\t');export PGPASSWORD="$P" PGCONNECT_TIMEOUT=2
p(){ psql -X -q -At -F "$T" -p %d -U "$U" -d "$1" -c "$2" 2>&1|tr -d '\r';};q(){ p "$@"|head -n 100;}
h=$(echo '== server';q "$D" "$1";echo '== dbsettings';q "$D" "$2";echo '== roles';q "$D" "$3";echo '== databases';q "$D" "$4";echo '== holders';o=$(q "$D" "$5");case "$o" in holder*)printf '%%s\n' "$o";;*)eval "$6";;esac)
printf '%%s\n' "$h";c="$7$8$9${10}${11}${12}";psql -X -q -At -p %d -U "$U" -d "$D" -c "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1 LIMIT 8" 2>/dev/null|while IFS= read -r d;do echo "== catalog $d";p "$d" "$c";done|awk -v b=$((3900-${#h})) '{n+=length($0)+1;if(n>b){print "== more";exit}print}'
`

// postgresQueries are the server-wide queries, one per section. Settings,
// role flags and catalog definitions are read; a custom (dotted) setting's
// value, a role's password and every row of every table are not.
var postgresQueries = []string{
	`SELECT 'role',CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'primary' END UNION ALL SELECT 'conn',count(*)||E'\t'||current_setting('max_connections') FROM pg_stat_activity WHERE backend_type='client backend' UNION ALL SELECT 'waiting',count(*)::text FROM pg_locks WHERE NOT granted UNION ALL (SELECT 'seq',schemaname||'.'||sequencename||E'\t'||last_value||E'\t'||max_value FROM pg_sequences WHERE last_value IS NOT NULL AND max_value>0 AND last_value>=max_value*0.9 ORDER BY last_value::numeric/max_value DESC LIMIT 5) UNION ALL (SELECT 'set',name||E'\t'||setting FROM pg_settings WHERE name IN('default_transaction_read_only','transaction_read_only','max_connections','superuser_reserved_connections','statement_timeout','lock_timeout','idle_in_transaction_session_timeout','idle_session_timeout','synchronous_commit','synchronous_standby_names','max_locks_per_transaction','max_prepared_transactions','fsync','work_mem','temp_file_limit','password_encryption','log_min_duration_statement','wal_level') ORDER BY 2)`,
	`SELECT coalesce(d.datname,'*'),coalesce(r.rolname,'*'),split_part(c,'=',1),CASE WHEN split_part(c,'=',1) LIKE '%.%' THEN 'set' ELSE substr(c,strpos(c,'=')+1) END FROM pg_db_role_setting s LEFT JOIN pg_database d ON d.oid=s.setdatabase LEFT JOIN pg_roles r ON r.oid=s.setrole,unnest(s.setconfig) c ORDER BY 1,2,3 LIMIT 33`,
	`SELECT rolname,concat_ws(' ',CASE WHEN rolsuper THEN 'superuser' END,CASE WHEN rolcanlogin THEN 'login' ELSE 'nologin' END,CASE WHEN rolcreatedb THEN 'createdb' END,CASE WHEN rolcreaterole THEN 'createrole' END,CASE WHEN rolreplication THEN 'replication' END,CASE WHEN rolbypassrls THEN 'bypassrls' END,CASE WHEN rolconnlimit>=0 THEN 'connlimit='||rolconnlimit END,CASE WHEN rolvaliduntil<now() THEN 'expired' WHEN rolvaliduntil<'infinity' THEN 'valid-until='||rolvaliduntil::date END,(SELECT 'member-of='||string_agg(b.rolname,',' ORDER BY b.rolname) FROM pg_auth_members m JOIN pg_roles b ON b.oid=m.roleid WHERE m.member=r.oid)) FROM pg_roles r WHERE rolname!~'^pg_' ORDER BY 1 LIMIT 25`,
	`SELECT datname,datallowconn,datconnlimit FROM pg_database WHERE NOT datistemplate ORDER BY 1 LIMIT 17`,
	postgresHolders,
}

// postgresHolders groups the client sessions other than the read's own by
// peer address (local for the Unix socket) and state.
const postgresHolders = `SELECT 'holder',coalesce(host(client_addr),'local'),s,count(*) FROM(SELECT client_addr,CASE WHEN wait_event_type='Lock' THEN 'blocked' WHEN state='idle' THEN 'idle' WHEN state LIKE 'idle in transaction%' THEN 'idle-in-transaction' ELSE 'active' END s FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid())x GROUP BY 2,3 ORDER BY 4 DESC,2,3 LIMIT 12`

// postgresCatalog is the per-database catalog query. Rows are (kind, object,
// name, value): locks that block writers first, then the tables with their
// planner row estimate and their indexes, constraints and triggers with a
// digest of each definition, and each login role's grant counts ("all" when
// the role holds the privilege on every table, so a new table the role owns
// or is granted by default changes nothing). When the
// itemized tables and definitions exceed postgresCatalogItemBytesSQL, the query
// prints one group digest per table group instead (catalog_group.go), so the
// output stays bounded and complete on a schema of any size. A constraint
// cloned onto a table from a partitioned parent (conparentid) is left out: the
// parent's own constraint carries its definition, and the clones change
// whenever the parent gains a partition.
const postgresCatalog = `WITH s AS(SELECT c.oid,n.nspname||'.'||c.relname o,c.reltuples r FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN('r','p') AND n.nspname!~'^(pg_|information_schema)'),` +
	`d AS(SELECT 'index' k,s.o,ic.relname m,concat_ws(' ',CASE WHEN i.indisprimary THEN 'primary' END,CASE WHEN i.indisunique THEN 'unique' END,CASE WHEN NOT i.indisvalid THEN 'invalid' END,left(md5(pg_get_indexdef(i.indexrelid)),8)) v FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid JOIN s ON s.oid=i.indrelid` +
	` UNION ALL SELECT 'constraint',s.o,o.conname,concat_ws(' ',o.contype::text,CASE WHEN NOT o.convalidated THEN 'not-valid' END,left(md5(pg_get_constraintdef(o.oid)),8)) FROM pg_constraint o JOIN s ON s.oid=o.conrelid WHERE o.contype IN('c','f','x') AND o.conparentid=0` +
	` UNION ALL SELECT 'trigger',s.o,g.tgname,'trigger '||g.tgenabled::text||' '||left(md5(pg_get_triggerdef(g.oid)),8) FROM pg_trigger g JOIN s ON s.oid=g.tgrelid WHERE NOT g.tgisinternal),` +
	`tb AS(SELECT 'table' k,o,'' m,CASE WHEN r<0 THEN '?' ELSE r::bigint::text END v FROM s),` +
	`z AS(SELECT coalesce(sum(length(k)+length(o)+length(m)+length(v)+4),0)<=` + postgresCatalogItemBytesSQL + ` f FROM(SELECT * FROM d UNION ALL SELECT * FROM tb)a)` +
	`SELECT k,o,m,v FROM(SELECT 0 p,'lock' k,l.relation::regclass::text o,l.mode m,'held' v FROM pg_locks l WHERE l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND l.granted AND l.mode IN('ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock') AND l.pid<>pg_backend_pid() GROUP BY 3,4` +
	` UNION ALL SELECT 1,k,o,m,v FROM d,z WHERE f UNION ALL SELECT 1,k,o,m,v FROM tb,z WHERE f` +
	` UNION ALL SELECT 1,'group',lpad(g::text,2,'0')||' '||left(md5(string_agg(o,',' ORDER BY o)),8),'',count(*)||' '||left(md5(string_agg(o||'='||coalesce(x,''),',' ORDER BY o)),8) FROM(SELECT s.o,get_byte(decode(md5(s.o),'hex'),0)%` + catalogGroups + ` g,y.x FROM s LEFT JOIN(SELECT o,string_agg(k||' '||m||' '||v,',' ORDER BY k,m) x FROM d GROUP BY o)y ON y.o=s.o)q,z WHERE NOT f GROUP BY g` +
	` UNION ALL SELECT 1,'grant',r.rolname,'',concat_ws(' ','connect='||has_database_privilege(r.oid,current_database(),'CONNECT'),'select='||CASE count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'SELECT')) WHEN count(t.oid) THEN 'all' ELSE (count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'SELECT')))::text END,'insert='||CASE count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'INSERT')) WHEN count(t.oid) THEN 'all' ELSE (count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'INSERT')))::text END,'update='||CASE count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'UPDATE')) WHEN count(t.oid) THEN 'all' ELSE (count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'UPDATE')))::text END,'delete='||CASE count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'DELETE')) WHEN count(t.oid) THEN 'all' ELSE (count(t.oid)FILTER(WHERE has_table_privilege(r.oid,t.oid,'DELETE')))::text END)` +
	` FROM pg_roles r LEFT JOIN pg_class t ON t.relkind IN('r','p') AND t.relnamespace IN(SELECT oid FROM pg_namespace WHERE nspname!~'^(pg_|information_schema)') WHERE r.rolcanlogin AND r.rolname!~'^pg_' GROUP BY r.oid,r.rolname)x ORDER BY p,k,o,m`

// postgresCatalogItemBytesSQL bounds a database's itemized catalog in bytes;
// above it the catalog is read as group digests.
const (
	postgresCatalogItemBytesSQL = "1500"
)

type postgresAdapter struct{}

func (postgresAdapter) Engine() Engine   { return EnginePostgres }
func (postgresAdapter) DefaultPort() int { return 5432 }

func (postgresAdapter) ReadCommand(e Endpoint) []string {
	args := append(append(append([]string{}, postgresQueries...), tcpHoldersSnippet(e.Port)), chunk(postgresCatalog, readChunkBytes)...)
	return readCommand([]string{"POSTGRES", "POSTGRESQL", "PG"}, fmt.Sprintf(postgresScript, e.Port, e.Port), args...)
}

// postgresRefusal classifies a section whose first line is psql's or the
// server's error. unreachable means psql reached no server.
func postgresRefusal(line string) (class string, refused bool) {
	if !strings.HasPrefix(line, "psql:") && !strings.HasPrefix(line, "ERROR:") && !strings.HasPrefix(line, "FATAL:") &&
		!strings.HasPrefix(line, "sh:") && !strings.Contains(line, ": not found") {
		return "", false
	}
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "too many clients"), strings.Contains(lower, "remaining connection slots"):
		return "max-clients", true
	case strings.Contains(lower, "password authentication failed"):
		return "auth-failed", true
	case strings.Contains(lower, "no password supplied"):
		return "auth-required", true
	case strings.Contains(lower, "permission denied"), strings.Contains(lower, "must be superuser"):
		return "no-permission", true
	case strings.Contains(lower, "starting up"), strings.Contains(lower, "recovery mode"):
		return "loading", true
	case strings.Contains(lower, "not found"), strings.Contains(lower, "no such file"), strings.Contains(lower, "connection refused"),
		strings.Contains(lower, "could not connect"), strings.Contains(lower, "shutting down"), strings.Contains(lower, "timeout expired"):
		return "unreachable", true
	}
	return "refused", true
}

func postgresTrue(v string) bool {
	switch strings.ToLower(v) {
	case "on", "true", "yes", "1":
		return true
	}
	return false
}

func (postgresAdapter) Parse(stdout string) (Facts, error) {
	sections, err := splitSections("postgres", stdout)
	if err != nil {
		return Facts{}, err
	}
	for _, name := range postgresSections {
		if _, ok := sections[name]; !ok {
			return Facts{}, fmt.Errorf("postgres read lacks section %q", name)
		}
	}
	var items []Item
	unread := map[ItemKind]bool{}
	answered := map[string]bool{}
	refuse := func(scope, name, class string, kinds []ItemKind) {
		items = append(items, Item{Kind: KindRefusal, Scope: scope, Name: name, Value: class})
		for _, k := range kinds {
			unread[k] = true
		}
	}
	for _, name := range postgresSections {
		lines := sections[name]
		if len(lines) > 0 {
			if class, refused := postgresRefusal(lines[0]); refused {
				if class == "unreachable" {
					return Facts{}, fmt.Errorf("postgres read section %s: %s", name, lines[0])
				}
				refuse("", name, class, postgresSectionKinds[name])
				continue
			}
		}
		if limit, ok := postgresRowLimits[name]; ok && len(lines) >= limit {
			sections[name] = lines[:limit-1]
			for _, k := range postgresSectionKinds[name] {
				unread[k] = true
			}
		}
		answered[name] = true
	}
	if answered["server"] && len(sections["server"]) == 0 {
		return Facts{}, fmt.Errorf("postgres read section server is empty")
	}
	if _, cut := sections["more"]; cut {
		for _, k := range postgresCatalogKinds {
			unread[k] = true
		}
	}

	f := Facts{Engine: EnginePostgres}
	holders, present := sections["holders"]
	if f.Holders, err = parseHolders(holders, present); err != nil {
		return Facts{}, fmt.Errorf("postgres read: %w", err)
	}
	var blocks []Item
	malformed := func(section, line string) error {
		return fmt.Errorf("postgres %s line is malformed: %q", section, line)
	}
	if answered["server"] {
		for _, line := range sections["server"] {
			fields := strings.Split(line, "\t")
			if fields[0] == "set" && len(fields) == 2 {
				fields = append(fields, "") // an empty setting value
			}
			switch {
			case fields[0] == "role" && len(fields) == 2:
				items = append(items, Item{Kind: KindRole, Name: "role", Value: fields[1]})
				if fields[1] == "standby" {
					blocks = append(blocks, Item{Kind: KindWriteBlock, Name: "in-recovery", Value: "active"})
				}
			case fields[0] == "seq" && len(fields) == 4:
				if g := gauge(fields[2], fields[3]); g != nil {
					if f.Sequences == nil {
						f.Sequences = map[string]Gauge{}
					}
					f.Sequences[fields[1]] = *g
				}
			case fields[0] == "conn" && len(fields) == 3:
				f.Connections = gauge(fields[1], fields[2])
			case fields[0] == "waiting" && len(fields) == 2:
				if fields[1] != "0" {
					items = append(items, Item{Kind: KindLock, Name: "waiting-for-lock", Value: "active"})
				}
			case fields[0] == "set" && len(fields) == 3:
				items = append(items, Item{Kind: KindSetting, Name: fields[1], Value: fields[2]})
				if fields[1] == "default_transaction_read_only" && postgresTrue(fields[2]) {
					blocks = append(blocks, Item{Kind: KindWriteBlock, Name: "default-transaction-read-only", Value: "active"})
				}
			default:
				return Facts{}, malformed("server", line)
			}
		}
	}
	if answered["dbsettings"] {
		for _, line := range sections["dbsettings"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 4 {
				return Facts{}, malformed("dbsettings", line)
			}
			scope := fields[0]
			if fields[1] != "*" {
				scope = fields[1] + "@" + fields[0]
			}
			items = append(items, Item{Kind: KindSetting, Scope: scope, Name: fields[2], Value: fields[3]})
			if fields[2] == "default_transaction_read_only" && postgresTrue(fields[3]) {
				blocks = append(blocks, Item{Kind: KindWriteBlock, Scope: scope, Name: "default-transaction-read-only", Value: "active"})
			}
		}
	}
	if answered["roles"] {
		for _, line := range sections["roles"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 2 {
				return Facts{}, malformed("roles", line)
			}
			items = append(items, Item{Kind: KindPrincipal, Name: fields[0], Value: fields[1]})
		}
	}
	if answered["databases"] {
		for _, line := range sections["databases"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 3 {
				return Facts{}, malformed("databases", line)
			}
			items = append(items,
				Item{Kind: KindSetting, Scope: fields[0], Name: "allow-connections", Value: fields[1]},
				Item{Kind: KindSetting, Scope: fields[0], Name: "connection-limit", Value: fields[2]})
		}
	}
	// Each database that accepts connections (the first 8) has a catalog
	// section unless the read was cut; one without leaves the catalog unread.
	if !answered["databases"] {
		for _, k := range postgresCatalogKinds {
			unread[k] = true
		}
	} else {
		expected := 0
		for _, line := range sections["databases"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 3 || fields[1] != "t" || expected == 8 {
				continue
			}
			expected++
			if _, ok := sections["catalog "+fields[0]]; !ok {
				for _, k := range postgresCatalogKinds {
					unread[k] = true
				}
			}
		}
	}
	for name, lines := range sections {
		db, ok := strings.CutPrefix(name, "catalog ")
		if !ok {
			continue
		}
		if len(lines) > 0 {
			if class, refused := postgresRefusal(lines[0]); refused {
				if class == "unreachable" {
					return Facts{}, fmt.Errorf("postgres read section %s: %s", name, lines[0])
				}
				refuse(db, "catalog", class, postgresCatalogKinds)
				continue
			}
		}
		for _, line := range lines {
			fields := strings.Split(line, "\t")
			if len(fields) != 4 {
				return Facts{}, malformed(name, line)
			}
			kind, object, item, value := fields[0], fields[1], fields[2], fields[3]
			switch kind {
			case "table":
				items = append(items, Item{Kind: KindNamespace, Scope: db, Name: object, Value: value})
			case "index":
				items = append(items, Item{Kind: KindIndex, Scope: db + "." + object, Name: item, Value: value})
			case "constraint", "trigger":
				items = append(items, Item{Kind: KindValidator, Scope: db + "." + object, Name: item, Value: value})
			case "lock":
				items = append(items, Item{Kind: KindLock, Scope: db + "." + object, Name: item, Value: value})
			case "grant":
				items = append(items, Item{Kind: KindPrincipal, Scope: db, Name: object, Value: value})
			case "group":
				// A catalog read as group digests lists no table, index or
				// trigger by itself; the digests stand for them.
				items = append(items, catalogGroupItem(KindValidator, db, object, value))
				unread[KindNamespace], unread[KindIndex] = true, true
			default:
				return Facts{}, malformed(name, line)
			}
		}
	}
	items = append(items, blocks...)
	if answered["server"] && answered["dbsettings"] || len(blocks) > 0 {
		writable := len(blocks) == 0
		f.Writable = &writable
	}
	f.Items = NewFacts(EnginePostgres, items).Items
	f.Unread = sortedKinds(unread)
	if err := f.Validate(); err != nil {
		return Facts{}, err
	}
	return f, nil
}
