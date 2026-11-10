package storestate

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// mysqlRestoreHead connects like the read: the image's mysql (or mariadb)
// client over TCP to the store's own port as the user the credential prelude
// found, the password in an option file on a here-document descriptor, never
// in argv or output. q runs one statement (or several, in one session) and
// prints at most five lines of the store's answer; it fails with the
// client's status.
const mysqlRestoreHead = `PORT=%d
U=${U:-root};W=$(printf '%%s' "$P"|sed 's/[\\"]/\\&/g')
c=mysql;command -v mysql >/dev/null 2>&1||c=mariadb
q(){ o=$($c --defaults-extra-file=/dev/fd/3 --protocol=TCP -h 127.0.0.1 -P "$PORT" -u "$U" --connect-timeout=2 -N -B -e "$1" 3<<SDOEOF 2>&1
[client]
password="$W"
SDOEOF
);r=$?;printf '%%s\n' "$o"|grep -v '^mysql: \[Warning\]'|head -n5;return $r;}
`

// mysqlStatementsScript runs its arguments in order, stopping at the first
// refusal. An argument that starts with "?" may fail.
const mysqlStatementsScript = mysqlRestoreHead + `for s;do case "$s" in \?*)q "${s#?}">/dev/null;;*)q "$s"||exit 1;;esac;done;echo '` + RestoredMarker + `'
`

// mysqlSettingScript sets global variable $1 to the SQL literal $2. A
// variable the server holds as persisted (mysqld-auto.cnf) is set with SET
// PERSIST, so a restart keeps the baseline value; any other with SET GLOBAL.
const mysqlSettingScript = mysqlRestoreHead + `n=$(q "SELECT COUNT(*) FROM performance_schema.persisted_variables WHERE variable_name='$1'" 2>/dev/null|tail -n1);k=GLOBAL;[ "$n" = 1 ]&&k=PERSIST
q "SET $k $1 = $2"||exit 1;echo '` + RestoredMarker + `'
`

// mysqlPayloadScript prints, for each account the read shows, its grant
// statements: "grants<TAB>user<TAB>host<TAB>fact value<TAB>text", the text's
// lines joined by the unit separator. $1 is the read's users query, so the
// fact value is the one the read computes. An account whose grants cannot be
// read whole is left out.
const mysqlPayloadScript = mysqlRestoreHead + `T=$(printf '\t');S=$(printf '\037')
e(){ printf '%%s' "$1"|sed "s/[\\\\']/\\\\&/g";}
echo '== payload grants'
q "$1"|while IFS=$T read -r u h x v;do
g=$(q "SHOW GRANTS FOR '$(e "$u")'@'$(e "$h")'") ||continue
printf 'grants\t%%s\t%%s\t%%s\t%%s\n' "$u" "$h" "$v" "$(printf '%%s' "$g"|tr '\n' "$S")"
done
echo '== end'
`

func mysqlStatements(e Endpoint, statements ...string) []string {
	return readCommand([]string{"MYSQL", "MARIADB"}, fmt.Sprintf(mysqlStatementsScript, e.Port), statements...)
}

// PayloadCommand implements PayloadReader: each account's grant statements,
// which the value-free read keeps only as counts.
func (mysqlAdapter) PayloadCommand(e Endpoint) []string {
	return readCommand([]string{"MYSQL", "MARIADB"}, fmt.Sprintf(mysqlPayloadScript, e.Port), mysqlQueries[2])
}

// ParsePayload implements PayloadReader. A read without its end marker is
// incomplete and an error; lines the client adds are skipped.
func (mysqlAdapter) ParsePayload(stdout string) (Payload, error) {
	started, ended := false, false
	var entries []PayloadEntry
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case line == "== payload grants":
			started = true
			continue
		case line == "== end":
			ended = true
			continue
		case !started || !strings.HasPrefix(line, "grants\t"):
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 5 {
			return Payload{}, fmt.Errorf("mysql payload line is malformed: %q", firstLine(line))
		}
		entries = append(entries, PayloadEntry{Key: Key{Kind: KindPrincipal, Name: f[1] + "@" + f[2]}, Value: f[3], Format: PayloadGrants, Text: f[4]})
	}
	if !started || !ended {
		return Payload{}, fmt.Errorf("mysql payload read is incomplete: %q", firstLine(stdout))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key.Less(entries[j].Key) })
	p := Payload{Entries: entries}
	return p, p.Validate()
}

func mysqlIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func mysqlString(s string) string {
	return `'` + strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(s) + `'`
}

var mysqlNumber = regexp.MustCompile(`^-?[0-9]+$`)

// mysqlLiteral is a variable value as a SQL literal: a number bare, anything
// else a string (the server converts it for booleans and enumerations).
func mysqlLiteral(v string) string {
	if mysqlNumber.MatchString(v) {
		return v
	}
	return mysqlString(v)
}

// mysqlAccount splits a principal name "user@host" at its last "@".
func mysqlAccount(name string) (user, host string, ok bool) {
	i := strings.LastIndex(name, "@")
	if i <= 0 {
		return "", "", false
	}
	return name[:i], name[i+1:], true
}

// mysqlRuntimeVariables are the read's variables the server accepts at
// runtime with a value of the same text. The others are startup options
// (log_bin, innodb_read_only) or change in steps (gtid_mode).
var mysqlRuntimeVariables = map[string]bool{
	"read_only": true, "super_read_only": true, "offline_mode": true, "max_connections": true, "max_user_connections": true,
	"max_connect_errors": true, "wait_timeout": true, "lock_wait_timeout": true, "innodb_lock_wait_timeout": true, "sql_mode": true,
	"max_allowed_packet": true, "autocommit": true, "event_scheduler": true, "require_secure_transport": true,
	"default_storage_engine": true, "transaction_isolation": true, "innodb_buffer_pool_size": true, "local_infile": true,
	"max_execution_time": true,
}

// mysqlWriteBlockVariables are the variables each write block follows from.
var mysqlWriteBlockVariables = map[string]string{"super-read-only": "super_read_only", "read-only": "read_only", "offline-mode": "offline_mode"}

// Restore implements Restorer: a global variable back to its baseline value
// (SET PERSIST when the server holds it persisted, else SET GLOBAL), a write
// block through the variable it follows from, an account's lock state, an
// account's grants replayed from the baseline's SHOW GRANTS text after
// revoking all (never its password), a replica reset to a primary, and an
// index added since the baseline dropped. What has no exact inverse is a
// *NotRestorableError with the reason.
func (mysqlAdapter) Restore(e Endpoint, c Change, baseline Facts, options RestoreOptions) ([][]string, error) {
	key := c.Key()
	for _, s := range []string{key.Scope, key.Name, value(c.Before), value(c.After)} {
		if strings.ContainsRune(s, 0) {
			return nil, notRestorable(key, "it holds a NUL byte")
		}
	}
	switch key.Kind {
	case KindSetting:
		if key.Scope != "" {
			return nil, notRestorable(key, "an account's password change time follows from its password, which is never kept")
		}
		item, ok := baseline.Lookup(key)
		if !ok {
			return nil, notRestorable(key, "the baseline does not show it")
		}
		return mysqlRestoreVariable(e, key, key.Name, item.Value)
	case KindWriteBlock:
		name, ok := mysqlWriteBlockVariables[key.Name]
		if !ok {
			if key.Name == "global-read-lock" {
				return nil, notRestorable(key, "the lock belongs to the session that took it; ending that session is not an inverse")
			}
			return nil, notRestorable(key, "%s is a startup option", key.Name)
		}
		item, ok := baseline.Lookup(Key{Kind: KindSetting, Name: name})
		if !ok {
			return nil, notRestorable(key, "the baseline does not show setting %s", name)
		}
		return mysqlRestoreVariable(e, key, name, item.Value)
	case KindRole:
		role, ok := baseline.Lookup(Key{Kind: KindRole, Name: "role"})
		if !ok || role.Value != "primary" {
			return nil, notRestorable(key, "the baseline role is not primary, so there is no primary to restore")
		}
		return [][]string{mysqlStatements(e, "?STOP REPLICA", "RESET REPLICA ALL")}, nil
	case KindPrincipal:
		return mysqlRestoreAccount(e, c, options)
	case KindIndex:
		return mysqlRestoreIndex(e, c)
	}
	return nil, notRestorable(key, "mysql %s items have no inverse", key.Kind)
}

func mysqlRestoreVariable(e Endpoint, key Key, name, baseline string) ([][]string, error) {
	if !mysqlRuntimeVariables[name] {
		return nil, notRestorable(key, "%s cannot be set at runtime to its baseline value", name)
	}
	if name == "event_scheduler" && strings.EqualFold(baseline, "DISABLED") {
		return nil, notRestorable(key, "event_scheduler DISABLED is a startup option")
	}
	return [][]string{readCommand([]string{"MYSQL", "MARIADB"}, fmt.Sprintf(mysqlSettingScript, e.Port), name, mysqlLiteral(baseline))}, nil
}

// mysqlGrantCounts are the grant counts of a principal value.
func mysqlGrantCounts(value string) string {
	var out []string
	for f := range fields(value) {
		if strings.HasPrefix(f, "global=") || strings.HasPrefix(f, "schema=") || strings.HasPrefix(f, "table=") {
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func mysqlRestoreAccount(e Endpoint, c Change, options RestoreOptions) ([][]string, error) {
	key := c.Key()
	user, host, ok := mysqlAccount(key.Name)
	if !ok || key.Scope != "" {
		return nil, notRestorable(key, "%q is not user@host", key.Name)
	}
	if c.Before == nil || c.After == nil {
		return nil, notRestorable(key, "an account added or removed is not a setting; its password is not kept")
	}
	account := mysqlString(user) + "@" + mysqlString(host)
	before, after := fields(c.Before.Value), fields(c.After.Value)
	if before["expired"] != after["expired"] {
		return nil, notRestorable(key, "an account's password expiry cannot be put back without its password")
	}
	var statements []string
	if mysqlGrantCounts(c.Before.Value) != mysqlGrantCounts(c.After.Value) {
		entry, err := payloadFor(options.Payload, *c.Before)
		if err != nil {
			return nil, err
		}
		grants, err := mysqlGrantStatements(key, entry, user, host)
		if err != nil {
			return nil, err
		}
		// One session: the account the restore runs as may be the one revoked,
		// and a session keeps the global privileges it connected with.
		statements = append(statements, "REVOKE ALL PRIVILEGES, GRANT OPTION FROM "+account+"; "+strings.Join(grants, "; "))
	}
	if before["locked"] != after["locked"] {
		clause := "ACCOUNT UNLOCK"
		if before["locked"] {
			clause = "ACCOUNT LOCK"
		}
		statements = append(statements, "ALTER USER "+account+" "+clause)
	}
	if len(statements) == 0 {
		return nil, notRestorable(key, "the change is not in the account's lock state or grants")
	}
	return [][]string{mysqlStatements(e, statements...)}, nil
}

// mysqlGrantStatements are the GRANT statements of an account's baseline
// SHOW GRANTS text. Anything else in the text (a partial revoke, a clause the
// restore does not replay, a statement separator) refuses the restore.
func mysqlGrantStatements(key Key, entry PayloadEntry, user, host string) ([]string, error) {
	if entry.Format == PayloadPlaceholder {
		return []string{"GRANT USAGE ON *.* TO " + mysqlString(user) + "@" + mysqlString(host)}, nil
	}
	if entry.Format != PayloadGrants {
		return nil, notRestorable(key, "the restore text has format %q, not grants", entry.Format)
	}
	target := mysqlIdent(user) + "@" + mysqlIdent(host)
	var out []string
	for _, line := range strings.Split(entry.Text, "\x1f") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		rest := strings.TrimSuffix(line, " WITH GRANT OPTION")
		if !strings.HasPrefix(line, "GRANT ") || !strings.HasSuffix(rest, " TO "+target) || strings.ContainsAny(line, ";\n\r") {
			return nil, notRestorable(key, "the baseline grant text holds a line the restore does not replay")
		}
		out = append(out, line)
	}
	if len(out) == 0 {
		return nil, notRestorable(key, "the baseline grant text is empty")
	}
	return out, nil
}

func mysqlRestoreIndex(e Endpoint, c Change) ([][]string, error) {
	key := c.Key()
	if c.Before != nil {
		return nil, notRestorable(key, "a changed or removed definition is kept only as a digest")
	}
	database, table, ok := strings.Cut(key.Scope, ".")
	if !ok || strings.Contains(table, ".") {
		return nil, notRestorable(key, "scope %q is not database.table", key.Scope)
	}
	if key.Name == "PRIMARY" {
		return nil, notRestorable(key, "a primary key added since the baseline is not dropped automatically")
	}
	return [][]string{mysqlStatements(e, fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s", mysqlIdent(database), mysqlIdent(table), mysqlIdent(key.Name)))}, nil
}
