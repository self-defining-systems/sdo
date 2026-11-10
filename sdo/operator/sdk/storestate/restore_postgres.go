package storestate

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// postgresRestoreHead connects like the read: psql over the container's own
// socket as the user the credential prelude found. Its session is never
// read-only, so it can undo a database made read-only. q runs one statement
// in database $d, printing at most five lines of the store's answer.
const postgresRestoreHead = `U=${U:-postgres};D=${POSTGRES_DB:-${POSTGRESQL_DATABASE:-${PGDATABASE:-$U}}};export PGPASSWORD="$P" PGCONNECT_TIMEOUT=5 PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c default_transaction_read_only=off"
q(){ o=$(psql -X -q -At -v ON_ERROR_STOP=1 -p %d -U "$U" -d "$d" -c "$1" 2>&1);r=$?;printf '%%s\n' "$o"|tr -d '\r'|head -n5;return $r;}
`

// postgresStatementsScript runs the statements $2.. in database $1 ("-" is
// the read's database) in order, stopping at the first refusal.
const postgresStatementsScript = postgresRestoreHead + `d=$1;shift;[ "$d" = - ]&&d=$D
for s;do q "$s"||exit 1;done;echo '` + RestoredMarker + `'
`

// postgresServerSettingScript restores a server setting: it first removes an
// ALTER SYSTEM override ($1) and reloads; only when the setting ($2 reads it)
// still differs from the baseline ($3) does it set it ($4) and reload again.
const postgresServerSettingScript = postgresRestoreHead + `d=$D;q "$1"||exit 1;q 'SELECT pg_reload_conf()'>/dev/null;sleep 1
if [ "$(q "$2")" != "$3" ];then q "$4"||exit 1;q 'SELECT pg_reload_conf()'>/dev/null;sleep 1;fi;echo '` + RestoredMarker + `'
`

var postgresTags = []string{"POSTGRES", "POSTGRESQL", "PG"}

func postgresStatements(e Endpoint, database string, statements ...string) []string {
	return readCommand(postgresTags, fmt.Sprintf(postgresStatementsScript, e.Port), append([]string{database}, statements...)...)
}

func pgIdent(s string) string  { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func pgString(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

var pgSettingName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// pgListSettings take a list; their stored text cannot be set back as one
// quoted value when it holds more than one element.
var pgListSettings = map[string]bool{"search_path": true, "temp_tablespaces": true, "session_preload_libraries": true, "local_preload_libraries": true, "shared_preload_libraries": true}

// Restore implements Restorer: a server setting by removing its ALTER SYSTEM
// override (or setting the baseline value), a per-database or per-role
// setting by ALTER DATABASE/ALTER ROLE SET or RESET, a database's connection
// settings, a role's flags, expiry, connection limit and memberships (never
// its password), pg_promote for a primary made a standby, a write block
// through the setting it follows from, and a constraint, trigger or index
// added since the baseline dropped (a trigger's enabled state put back).
func (postgresAdapter) Restore(e Endpoint, c Change, baseline Facts, options RestoreOptions) ([][]string, error) {
	key := c.Key()
	for _, s := range []string{key.Scope, key.Name, value(c.Before), value(c.After)} {
		if strings.ContainsRune(s, 0) {
			return nil, notRestorable(key, "it holds a NUL byte")
		}
	}
	switch key.Kind {
	case KindSetting:
		return postgresRestoreSetting(e, c, options)
	case KindWriteBlock:
		switch key.Name {
		case "default-transaction-read-only":
			settingKey := Key{Kind: KindSetting, Scope: key.Scope, Name: "default_transaction_read_only"}
			setting := Change{After: &Item{Kind: KindSetting, Scope: key.Scope, Name: settingKey.Name, Value: "on"}}
			if item, ok := baseline.Lookup(settingKey); ok {
				setting.Before = &item
			}
			return postgresRestoreSetting(e, setting, options)
		case "in-recovery":
			return postgresPromote(e, key, baseline)
		}
	case KindRole:
		return postgresPromote(e, key, baseline)
	case KindPrincipal:
		if key.Scope == "" {
			return postgresRestoreRole(e, c)
		}
		return nil, notRestorable(key, "grant counts do not say which tables a grant covered")
	case KindValidator, KindIndex:
		return postgresRestoreCatalog(e, c)
	}
	return nil, notRestorable(key, "postgres %s items have no inverse", key.Kind)
}

func value(i *Item) string {
	if i == nil {
		return ""
	}
	return i.Value
}

func postgresPromote(e Endpoint, key Key, baseline Facts) ([][]string, error) {
	role, ok := baseline.Lookup(Key{Kind: KindRole, Name: "role"})
	if !ok || role.Value != "primary" {
		return nil, notRestorable(key, "the baseline role is not primary, so there is no primary to promote back")
	}
	return [][]string{postgresStatements(e, "-", "SELECT pg_promote()")}, nil
}

func postgresRestoreSetting(e Endpoint, c Change, options RestoreOptions) ([][]string, error) {
	key := c.Key()
	before := c.Before
	if key.Scope != "" && (key.Name == "allow-connections" || key.Name == "connection-limit") {
		if before == nil {
			return nil, notRestorable(key, "the baseline does not show the database")
		}
		if key.Name == "allow-connections" {
			return [][]string{postgresStatements(e, "-", fmt.Sprintf("ALTER DATABASE %s ALLOW_CONNECTIONS %t", pgIdent(key.Scope), postgresTrue(before.Value) || before.Value == "t"))}, nil
		}
		limit, err := strconv.Atoi(before.Value)
		if err != nil {
			return nil, notRestorable(key, "baseline connection limit %q is not a number", before.Value)
		}
		return [][]string{postgresStatements(e, "-", fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT %d", pgIdent(key.Scope), limit))}, nil
	}
	if !pgSettingName.MatchString(key.Name) {
		return nil, notRestorable(key, "%q is not a setting name", key.Name)
	}
	set := ""
	if before != nil {
		switch {
		case strings.Contains(key.Name, ".") && key.Scope != "":
			return nil, notRestorable(key, "the read keeps a custom setting's value hidden")
		case pgListSettings[key.Name] && strings.Contains(before.Value, ","):
			return nil, notRestorable(key, "a list setting's stored text cannot be set back as one value")
		}
		set = fmt.Sprintf("SET %s TO %s", key.Name, pgString(before.Value))
	}
	if key.Scope == "" {
		if before == nil {
			return nil, notRestorable(key, "the baseline does not show it")
		}
		return [][]string{readCommand(postgresTags, fmt.Sprintf(postgresServerSettingScript, e.Port),
			"ALTER SYSTEM RESET "+key.Name,
			"SELECT setting FROM pg_settings WHERE name="+pgString(key.Name),
			before.Value,
			"ALTER SYSTEM "+set)}, nil
	}
	clause := set
	if before == nil {
		clause = "RESET " + key.Name
	}
	var target, end string
	if role, database, ok := strings.Cut(key.Scope, "@"); ok {
		target = "ROLE " + pgIdent(role)
		end = "usename=" + pgString(role)
		if database != "*" {
			target += " IN DATABASE " + pgIdent(database)
			end += " AND datname=" + pgString(database)
		}
	} else {
		target = "DATABASE " + pgIdent(key.Scope)
		end = "datname=" + pgString(key.Scope)
	}
	statements := []string{"ALTER " + target + " " + clause}
	if options.EndSessions {
		statements = append(statements, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid() AND "+end)
	}
	return [][]string{postgresStatements(e, "-", statements...)}, nil
}

// postgresRoleFlags maps the read's role flags to their ALTER ROLE options.
var postgresRoleFlags = [][3]string{
	{"superuser", "SUPERUSER", "NOSUPERUSER"},
	{"login", "LOGIN", "NOLOGIN"},
	{"createdb", "CREATEDB", "NOCREATEDB"},
	{"createrole", "CREATEROLE", "NOCREATEROLE"},
	{"replication", "REPLICATION", "NOREPLICATION"},
	{"bypassrls", "BYPASSRLS", "NOBYPASSRLS"},
}

func postgresRestoreRole(e Endpoint, c Change) ([][]string, error) {
	key := c.Key()
	if c.Before == nil || c.After == nil {
		return nil, notRestorable(key, "a role added or removed is not a setting; its password is not kept")
	}
	before, after := fields(c.Before.Value), fields(c.After.Value)
	if before["expired"] {
		return nil, notRestorable(key, "the baseline role was already expired")
	}
	role := pgIdent(key.Name)
	options := []string{}
	for _, flag := range postgresRoleFlags {
		if before[flag[0]] || (flag[0] == "login" && !before["nologin"]) {
			options = append(options, flag[1])
		} else {
			options = append(options, flag[2])
		}
	}
	limit, until, members := "-1", "infinity", map[string]bool{}
	for f := range before {
		switch {
		case strings.HasPrefix(f, "connlimit="):
			limit = strings.TrimPrefix(f, "connlimit=")
		case strings.HasPrefix(f, "valid-until="):
			until = strings.TrimPrefix(f, "valid-until=")
		case strings.HasPrefix(f, "member-of="):
			for _, m := range strings.Split(strings.TrimPrefix(f, "member-of="), ",") {
				members[m] = true
			}
		}
	}
	if _, err := strconv.Atoi(limit); err != nil {
		return nil, notRestorable(key, "baseline connection limit %q is not a number", limit)
	}
	options = append(options, "CONNECTION LIMIT "+limit, "VALID UNTIL "+pgString(until))
	statements := []string{"ALTER ROLE " + role + " " + strings.Join(options, " ")}
	current := map[string]bool{}
	for f := range after {
		if strings.HasPrefix(f, "member-of=") {
			for _, m := range strings.Split(strings.TrimPrefix(f, "member-of="), ",") {
				current[m] = true
			}
		}
	}
	for _, m := range sortedSet(members) {
		if !current[m] {
			statements = append(statements, "GRANT "+pgIdent(m)+" TO "+role)
		}
	}
	for _, m := range sortedSet(current) {
		if !members[m] {
			statements = append(statements, "REVOKE "+pgIdent(m)+" FROM "+role)
		}
	}
	return [][]string{postgresStatements(e, "-", statements...)}, nil
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		if k != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// postgresTriggerStates maps pg_trigger.tgenabled to its ALTER TABLE clause.
var postgresTriggerStates = map[string]string{"O": "ENABLE TRIGGER", "D": "DISABLE TRIGGER", "R": "ENABLE REPLICA TRIGGER", "A": "ENABLE ALWAYS TRIGGER"}

func postgresRestoreCatalog(e Endpoint, c Change) ([][]string, error) {
	key := c.Key()
	parts := strings.Split(key.Scope, ".")
	if len(parts) != 3 {
		return nil, notRestorable(key, "scope %q is not database.schema.table", key.Scope)
	}
	database, table := parts[0], pgIdent(parts[1])+"."+pgIdent(parts[2])
	before, after := strings.Fields(value(c.Before)), strings.Fields(value(c.After))
	isTrigger := func(f []string) bool { return len(f) == 3 && f[0] == "trigger" }
	switch {
	case c.Before != nil && c.After != nil && isTrigger(before) && isTrigger(after) && before[2] == after[2]:
		clause, ok := postgresTriggerStates[before[1]]
		if !ok {
			return nil, notRestorable(key, "unknown trigger state %q", before[1])
		}
		return [][]string{postgresStatements(e, database, fmt.Sprintf("ALTER TABLE %s %s %s", table, clause, pgIdent(key.Name)))}, nil
	case c.Before != nil:
		return nil, notRestorable(key, "a changed or removed definition is kept only as a digest")
	case key.Kind == KindIndex:
		return [][]string{postgresStatements(e, database, fmt.Sprintf("DROP INDEX %s.%s", pgIdent(parts[1]), pgIdent(key.Name)))}, nil
	case isTrigger(after):
		return [][]string{postgresStatements(e, database, fmt.Sprintf("DROP TRIGGER %s ON %s", pgIdent(key.Name), table))}, nil
	}
	return [][]string{postgresStatements(e, database, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", table, pgIdent(key.Name)))}, nil
}
