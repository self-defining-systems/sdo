package storestate

import (
	"fmt"
	"strings"
)

// mysqlSections are the server-wide sections of the mysql read, in order;
// each header must be present. tables, indexes, locks and mdl follow, cut at
// the output budget with a "more" section.
var mysqlSections = []string{"vars", "status", "role", "users"}

// mysqlCatalogSections are the catalog sections and the kinds they show.
var mysqlCatalogSections = map[string][]ItemKind{
	"tables":  {KindNamespace},
	"indexes": {KindIndex},
	"locks":   {KindLock},
	"mdl":     {KindLock, KindWriteBlock},
}

var mysqlSectionKinds = map[string][]ItemKind{
	"vars":   {KindSetting, KindWriteBlock},
	"status": {},
	"role":   {KindRole},
	"users":  {KindPrincipal},
}

// mysqlRowLimits are the bounded sections: a section that returns its limit
// has more rows than the read shows, and its kinds are unread.
var mysqlRowLimits = map[string]int{"users": 25, "indexes": 129, "mdl": 16}

// mysqlScript runs the image's mysql (or mariadb) client against the store's
// own port as the user the credential prelude found ($U, else root), with
// the password ($P) in an option file on a here-document descriptor, never
// in argv or output. Queries arrive as positional arguments. The catalog is
// cut at the output budget with a "== more" marker.
const mysqlScript = `PORT=%d;DB=
U=${U:-root};W=$(printf '%%s' "$P"|sed 's/[\\"]/\\&/g')
c=mysql;command -v mysql >/dev/null 2>&1||c=mariadb
q(){ $c --defaults-extra-file=/dev/fd/3 --protocol=TCP -h 127.0.0.1 -P "$PORT" -u "$U" --connect-timeout=2 -N -B -e "$1" 3<<SDOEOF 2>&1|grep -v '^mysql: \[Warning\]'|head -n 140
[client]
password="$W"
SDOEOF
}
r(){ o=$(q 'SHOW REPLICA STATUS');case "$o" in ERROR*1064*|ERROR*syntax*)o=$(q 'SHOW SLAVE STATUS');;esac;case "$o" in ERROR*|*"not found"*)printf '%%s\n' "$o"|head -n1;;'')echo primary;;*)echo replica;;esac;}
h=$(echo '== vars';q "$1";echo '== status';q "$2";echo '== role';r;echo '== users';q "$3")
printf '%%s\n' "$h";{ echo '== tables';q "$4";echo '== indexes';q "$5";echo '== locks';q "$6";echo '== mdl';q "$7";}|awk -v b=$((3900-${#h})) '{n+=length($0)+1;if(n>b){print "== more";exit}print}'
`

// mysqlQueries are the read's queries: settings, connection status, users
// (lock and expiry flags and grant counts as the principal; the time of the
// last password change as a setting, so a rotation shows in the changes
// without holding health; never a credential), tables with exact row counts (an estimate marked "~" above
// 100000 rows), index digests, waiting lock counts and the metadata locks
// that block writers. No row of any table is read.
var mysqlQueries = []string{
	`SHOW GLOBAL VARIABLES WHERE Variable_name IN('read_only','super_read_only','offline_mode','innodb_read_only','max_connections','max_user_connections','max_connect_errors','wait_timeout','lock_wait_timeout','innodb_lock_wait_timeout','sql_mode','max_allowed_packet','autocommit','event_scheduler','require_secure_transport','default_storage_engine','log_bin','gtid_mode','transaction_isolation','innodb_buffer_pool_size','local_infile','max_execution_time')`,
	`SHOW GLOBAL STATUS WHERE Variable_name IN('Threads_connected','Threads_running')`,
	`SELECT u.user,u.host,IFNULL(DATE_FORMAT(u.password_last_changed,'%Y-%m-%dT%H:%i:%s'),'-'),CONCAT_WS(' ',IF(u.account_locked='Y','locked','unlocked'),IF(u.password_expired='Y','expired',NULL),CONCAT('global=',(SELECT COUNT(*) FROM information_schema.user_privileges p WHERE p.grantee=CONCAT('''',u.user,'''@''',u.host,''''))),CONCAT('schema=',(SELECT COUNT(*) FROM information_schema.schema_privileges p WHERE p.grantee=CONCAT('''',u.user,'''@''',u.host,''''))),CONCAT('table=',(SELECT COUNT(*) FROM information_schema.table_privileges p WHERE p.grantee=CONCAT('''',u.user,'''@''',u.host,'''')))) FROM mysql.user u WHERE u.user NOT LIKE 'mysql.%' ORDER BY 1,2 LIMIT 25`,
	"SET SESSION group_concat_max_len=65536; SELECT IFNULL(GROUP_CONCAT(CONCAT('SELECT ',QUOTE(table_schema),',',QUOTE(table_name),',',IF(IFNULL(table_rows,0)>100000,QUOTE(CONCAT('~',table_rows)),CONCAT('COUNT(*) FROM `',REPLACE(table_schema,'`','``'),'`.`',REPLACE(table_name,'`','``'),'`'))) SEPARATOR ' UNION ALL '),'SELECT 1,1,1 FROM DUAL WHERE FALSE') INTO @sdo_q FROM (SELECT table_schema,table_name,table_rows FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN('mysql','information_schema','performance_schema','sys') ORDER BY 1,2 LIMIT 64) t; PREPARE sdo_s FROM @sdo_q; EXECUTE sdo_s; SELECT 'more','','' FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN('mysql','information_schema','performance_schema','sys') HAVING COUNT(*)>64",
	`SELECT table_schema,table_name,index_name,CONCAT_WS(' ',IF(MAX(non_unique)=0,'unique',NULL),LEFT(MD5(GROUP_CONCAT(column_name ORDER BY seq_in_index)),8)) FROM information_schema.statistics WHERE table_schema NOT IN('mysql','information_schema','performance_schema','sys') GROUP BY 1,2,3 ORDER BY 1,2,3 LIMIT 129`,
	`SELECT 'waiting',COUNT(*) FROM information_schema.processlist WHERE state LIKE 'Waiting for%lock%' UNION ALL SELECT 'trx-waiting',COUNT(*) FROM information_schema.innodb_trx WHERE trx_state='LOCK WAIT'`,
	`SELECT object_type,IFNULL(object_schema,''),IFNULL(object_name,''),lock_type FROM performance_schema.metadata_locks WHERE lock_status='GRANTED' AND (object_type='GLOBAL' AND lock_type='SHARED' OR object_type='TABLE' AND lock_type IN('SHARED_NO_READ_WRITE','SHARED_READ_ONLY','SHARED_NO_WRITE','EXCLUSIVE')) GROUP BY 1,2,3,4 ORDER BY 1,2,3,4 LIMIT 16`,
}

type mysqlAdapter struct{}

func (mysqlAdapter) Engine() Engine   { return EngineMySQL }
func (mysqlAdapter) DefaultPort() int { return 3306 }

func (mysqlAdapter) ReadCommand(e Endpoint) []string {
	var args []string
	for _, q := range mysqlQueries {
		if len(q) > readChunkBytes {
			panic("mysql read query exceeds the pod-read argument bound")
		}
		args = append(args, q)
	}
	return readCommand([]string{"MYSQL", "MARIADB"}, fmt.Sprintf(mysqlScript, e.Port), args...)
}

// mysqlRefusal classifies a section whose first line is the client's or the
// server's error. unreachable means the client reached no server.
func mysqlRefusal(line string) (class string, refused bool) {
	lower := strings.ToLower(line)
	if !strings.HasPrefix(line, "ERROR") && !strings.Contains(lower, "not found") && !strings.HasPrefix(line, "sh:") {
		return "", false
	}
	switch {
	case strings.Contains(line, " 1040 "), strings.Contains(lower, "too many connections"):
		return "max-clients", true
	case strings.Contains(line, " 1045 "), strings.Contains(line, " 3118 "), strings.Contains(lower, "access denied for user"):
		return "auth-failed", true
	case strings.Contains(line, " 1142 "), strings.Contains(line, " 1227 "), strings.Contains(line, " 1044 "), strings.Contains(lower, "command denied"):
		return "no-permission", true
	case strings.Contains(line, " 1064 "), strings.Contains(line, " 1146 "), strings.Contains(line, " 1054 "), strings.Contains(line, " 1109 "):
		return "unsupported", true
	case strings.Contains(line, " 2002 "), strings.Contains(line, " 2003 "), strings.Contains(line, " 2013 "), strings.Contains(lower, "can't connect"),
		strings.Contains(lower, "not found"), strings.Contains(lower, "lost connection"):
		return "unreachable", true
	}
	return "refused", true
}

func mysqlOn(v string) bool {
	switch strings.ToUpper(v) {
	case "ON", "1", "TRUE", "YES":
		return true
	}
	return false
}

func (mysqlAdapter) Parse(stdout string) (Facts, error) {
	sections, err := splitSections("mysql", stdout)
	if err != nil {
		return Facts{}, err
	}
	for _, name := range mysqlSections {
		if _, ok := sections[name]; !ok {
			return Facts{}, fmt.Errorf("mysql read lacks section %q", name)
		}
	}
	var items []Item
	unread := map[ItemKind]bool{}
	answered := map[string]bool{}
	all := append(append([]string{}, mysqlSections...), "tables", "indexes", "locks", "mdl")
	kindsOf := func(name string) []ItemKind {
		if k, ok := mysqlSectionKinds[name]; ok {
			return k
		}
		return mysqlCatalogSections[name]
	}
	for _, name := range all {
		lines, ok := sections[name]
		if !ok {
			for _, k := range kindsOf(name) {
				unread[k] = true
			}
			continue
		}
		if len(lines) > 0 {
			if class, refused := mysqlRefusal(lines[0]); refused {
				if class == "unreachable" {
					return Facts{}, fmt.Errorf("mysql read section %s: %s", name, lines[0])
				}
				items = append(items, Item{Kind: KindRefusal, Name: name, Value: class})
				for _, k := range kindsOf(name) {
					unread[k] = true
				}
				continue
			}
		}
		if limit, ok := mysqlRowLimits[name]; ok && len(lines) >= limit {
			sections[name] = lines[:limit-1]
			for _, k := range kindsOf(name) {
				unread[k] = true
			}
		}
		answered[name] = true
	}
	if _, cut := sections["more"]; cut {
		for _, kinds := range mysqlCatalogSections {
			for _, k := range kinds {
				unread[k] = true
			}
		}
	}
	malformed := func(section, line string) error {
		return fmt.Errorf("mysql %s line is malformed: %q", section, line)
	}
	vars, status := map[string]string{}, map[string]string{}
	for _, pair := range []struct {
		section string
		into    map[string]string
	}{{"vars", vars}, {"status", status}} {
		if !answered[pair.section] {
			continue
		}
		for _, line := range sections[pair.section] {
			name, value, ok := strings.Cut(line, "\t")
			if !ok {
				return Facts{}, malformed(pair.section, line)
			}
			pair.into[name] = value
		}
	}
	if answered["vars"] && len(vars) == 0 {
		return Facts{}, fmt.Errorf("mysql read section vars is empty")
	}
	f := Facts{Engine: EngineMySQL}
	for name, value := range vars {
		items = append(items, Item{Kind: KindSetting, Name: name, Value: value})
	}
	if answered["status"] && answered["vars"] {
		f.Connections = gauge(status["Threads_connected"], vars["max_connections"])
	}
	var blocks []Item
	block := func(scope, name string) {
		blocks = append(blocks, Item{Kind: KindWriteBlock, Scope: scope, Name: name, Value: "active"})
	}
	for _, v := range []struct{ name, block string }{
		{"super_read_only", "super-read-only"}, {"read_only", "read-only"}, {"offline_mode", "offline-mode"}, {"innodb_read_only", "innodb-read-only"},
	} {
		if mysqlOn(vars[v.name]) {
			block("", v.block)
		}
	}
	if answered["role"] {
		if len(sections["role"]) != 1 {
			return Facts{}, malformed("role", strings.Join(sections["role"], " | "))
		}
		items = append(items, Item{Kind: KindRole, Name: "role", Value: sections["role"][0]})
	}
	if answered["users"] {
		for _, line := range sections["users"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 4 {
				return Facts{}, malformed("users", line)
			}
			account := fields[0] + "@" + fields[1]
			items = append(items, Item{Kind: KindPrincipal, Name: account, Value: fields[3]},
				Item{Kind: KindSetting, Scope: account, Name: "password-changed", Value: fields[2]})
		}
	}
	if answered["tables"] {
		for _, line := range sections["tables"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 3 {
				return Facts{}, malformed("tables", line)
			}
			if fields[0] == "more" {
				unread[KindNamespace] = true
				continue
			}
			items = append(items, Item{Kind: KindNamespace, Scope: fields[0], Name: fields[1], Value: fields[2]})
		}
	}
	if answered["indexes"] {
		for _, line := range sections["indexes"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 4 {
				return Facts{}, malformed("indexes", line)
			}
			items = append(items, Item{Kind: KindIndex, Scope: fields[0] + "." + fields[1], Name: fields[2], Value: fields[3]})
		}
	}
	if answered["locks"] {
		for _, line := range sections["locks"] {
			name, value, ok := strings.Cut(line, "\t")
			if !ok {
				return Facts{}, malformed("locks", line)
			}
			if value != "0" {
				items = append(items, Item{Kind: KindLock, Name: name, Value: "active"})
			}
		}
	}
	if answered["mdl"] {
		for _, line := range sections["mdl"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 4 {
				return Facts{}, malformed("mdl", line)
			}
			if fields[0] == "GLOBAL" {
				items = append(items, Item{Kind: KindLock, Name: "global-read-lock", Value: "held"})
				block("", "global-read-lock")
				continue
			}
			items = append(items, Item{Kind: KindLock, Scope: fields[1] + "." + fields[2], Name: strings.ToLower(fields[3]), Value: "held"})
		}
	}
	items = append(items, blocks...)
	if answered["vars"] || len(blocks) > 0 {
		writable := len(blocks) == 0
		f.Writable = &writable
	}
	f.Items = NewFacts(EngineMySQL, items).Items
	f.Unread = sortedKinds(unread)
	if err := f.Validate(); err != nil {
		return Facts{}, err
	}
	return f, nil
}
