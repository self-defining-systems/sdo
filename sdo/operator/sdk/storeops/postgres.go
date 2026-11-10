package storeops

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// postgresPrelude sets the role and password from the container's environment
// and defines q, psql against the store's own port and database over the local
// socket. Positional arguments: $1 port, $2 database ("" is the container's
// default); they are shifted away.
const postgresPrelude = `export PGUSER="${PGUSER:-${POSTGRES_USER:-postgres}}"
if [ -z "${PGPASSWORD:-}" ] && [ -n "${POSTGRES_PASSWORD:-}" ]; then export PGPASSWORD="$POSTGRES_PASSWORD"; fi
P="$1";D="$2";shift 2;[ -n "$D" ] || D="${POSTGRES_DB:-$PGUSER}"
q(){ psql -X -q -At -v ON_ERROR_STOP=1 -p "$P" -d "$D" "$@";}
`

var postgresClause = regexp.MustCompile(`(?i)\b(where|returning)\b`)

var postgresDatabase = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$`)

type postgres struct{}

func (postgres) Name() string     { return "postgres" }
func (postgres) DefaultPort() int { return 5432 }
func (postgres) Image() string    { return `(^|/)(postgres|postgis|pgvector)[^/]*([:@]|$)` }

func (postgres) Normalize(s Scope) Scope {
	if strings.TrimSpace(s.Selector) == "" {
		s.Selector = "TRUE"
	}
	return s
}

func (e postgres) ValidateScope(s Scope) error {
	s = e.Normalize(s)
	if s.Database != "" && !postgresDatabase.MatchString(s.Database) {
		return fmt.Errorf("postgres scope database %q is not a plain name", s.Database)
	}
	if _, err := postgresTable(s.Collection); err != nil {
		return err
	}
	if err := sqlText("postgres selector", s.Selector); err != nil {
		return err
	}
	return validateLimit(s)
}

// postgresTable quotes a table or schema.table name.
func postgresTable(name string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("postgres scope collection %q is not a table or schema.table", name)
	}
	for i, part := range parts {
		if !identifier.MatchString(part) {
			return "", fmt.Errorf("postgres scope collection %q is not a table or schema.table", name)
		}
		parts[i] = `"` + part + `"`
	}
	return strings.Join(parts, "."), nil
}

// ValidateStatement accepts "delete" or a SET clause ("SET status = 'open'"),
// which SDO applies only to the rows the selector matches.
func (postgres) ValidateStatement(statement string) error {
	if statement == "delete" {
		return nil
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "SET ") {
		return fmt.Errorf("postgres repair statement must be \"delete\" or a SET clause")
	}
	if err := sqlText("postgres repair statement", statement); err != nil {
		return err
	}
	if postgresClause.MatchString(topLevelSQL(statement)) {
		return fmt.Errorf("postgres repair statement may not carry its own top-level WHERE or RETURNING; the scope selects the rows (a subquery in parentheses may use them)")
	}
	return nil
}

// topLevelSQL blanks the text inside parentheses and quotes, so a check sees
// only the clause's own keywords: a correlated subquery such as
// "SET total = (SELECT sum(n) FROM lines WHERE lines.id = orders.id)" keeps its
// WHERE. An unbalanced statement is returned whole and fails at the server.
func topLevelSQL(statement string) string {
	var out strings.Builder
	depth, quote := 0, byte(0)
	for i := 0; i < len(statement); i++ {
		c := statement[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
			out.WriteByte(' ')
			continue
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			if depth == 0 {
				return statement
			}
			depth--
			out.WriteByte(' ')
			continue
		}
		if depth > 0 || quote != 0 {
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(c)
	}
	return out.String()
}

func (e postgres) run(port int, s Scope, script string, args ...string) Command {
	return Command{Args: append([]string{"sh", "-c", postgresPrelude + script, "sh", strconv.Itoa(port), s.Database}, args...)}
}

func (e postgres) Count(port int, s Scope) Command {
	s = e.Normalize(s)
	table, _ := postgresTable(s.Collection)
	return e.run(port, s, `q -c "$1"`, "SELECT 'sdo-count ' || count(*) FROM "+table+" WHERE ("+s.Selector+")")
}

func (e postgres) Backup(port int, s Scope) Command {
	s = e.Normalize(s)
	table, _ := postgresTable(s.Collection)
	return e.run(port, s, `L="$2"
`+dumpWrapper(`q -c "$1"`), "COPY (SELECT * FROM "+table+" WHERE ("+s.Selector+")) TO STDOUT", strconv.Itoa(s.limit()))
}

// Write changes the rows in one transaction and commits only when the number
// changed is within [min, max].
func (e postgres) Write(port int, s Scope, statement string, min, max int) Command {
	s = e.Normalize(s)
	table, _ := postgresTable(s.Collection)
	change := "UPDATE " + table + " " + statement + " WHERE (" + s.Selector + ") RETURNING 1"
	if statement == "delete" {
		change = "DELETE FROM " + table + " WHERE (" + s.Selector + ") RETURNING 1"
	}
	script := fmt.Sprintf(`BEGIN;
WITH sdo_changed AS (%s) SELECT count(*) AS sdo_n, count(*) BETWEEN %d AND %d AS sdo_ok FROM sdo_changed \gset
\if :sdo_ok
COMMIT;
\echo sdo-affected :sdo_n
\else
ROLLBACK;
\echo sdo-expect-failed :sdo_n
\endif
`, change, min, max)
	c := e.run(port, s, `q`)
	c.Stdin = script
	return c
}

// Restore replaces, in one transaction, every row whose primary key is in the
// backup with its backed-up version. A table without a primary key is refused.
func (e postgres) Restore(port int, s Scope, dump string) (Command, error) {
	s = e.Normalize(s)
	table, err := postgresTable(s.Collection)
	if err != nil {
		return Command{}, err
	}
	replace := `DO $sdo$ DECLARE cond text; BEGIN
SELECT string_agg(format('t.%I = r.%I', a.attname, a.attname), ' AND ') INTO cond FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '` + table + `'::regclass AND i.indisprimary;
IF cond IS NULL THEN RAISE EXCEPTION 'restore needs a primary key on %', '` + table + `'; END IF;
EXECUTE format('DELETE FROM %s t USING sdo_restore r WHERE %s', '` + table + `'::regclass, cond);
END $sdo$`
	c := e.run(port, s, `sed '$d' | q -1 -c "$1" -c "$2" -c "$3" -c "$4" && echo "sdo-restored $5"`,
		"CREATE TEMP TABLE sdo_restore (LIKE "+table+")",
		"COPY sdo_restore FROM STDIN",
		replace,
		"INSERT INTO "+table+" OVERRIDING SYSTEM VALUE SELECT * FROM sdo_restore",
		strconv.Itoa(dumpItems(dump)))
	c.Stdin = dump
	return c, nil
}

func (postgres) Invalidate(int, Scope) (Command, error) {
	return Command{}, fmt.Errorf("postgres has no derived-store invalidation; rebuild the copy by hand")
}

// Copy inserts, in one transaction, every row of from that into lacks (ON
// CONFLICT DO NOTHING keeps the target's row), only when from matches [min,
// max] rows. Both tables are in one database and have the same columns in
// the same order.
func (e postgres) Copy(port int, from, into Scope, min, max int) (Command, error) {
	from, into = e.Normalize(from), e.Normalize(into)
	if from.Database != into.Database {
		return Command{}, fmt.Errorf("postgres copy stays in one database")
	}
	if into.Selector != "TRUE" {
		return Command{}, fmt.Errorf("postgres copy target selects a whole table; leave its selector empty")
	}
	source, err := postgresTable(from.Collection)
	if err != nil {
		return Command{}, err
	}
	target, err := postgresTable(into.Collection)
	if err != nil {
		return Command{}, err
	}
	script := fmt.Sprintf(`BEGIN;
SELECT count(*) AS sdo_n, count(*) BETWEEN %d AND %d AS sdo_ok FROM %s WHERE (%s) \gset
\if :sdo_ok
WITH sdo_ins AS (INSERT INTO %s OVERRIDING SYSTEM VALUE SELECT * FROM %s WHERE (%s) ON CONFLICT DO NOTHING RETURNING 1) SELECT count(*) AS sdo_c FROM sdo_ins \gset
COMMIT;
\echo sdo-affected :sdo_c
\else
ROLLBACK;
\echo sdo-expect-failed :sdo_n
\endif
`, min, max, source, from.Selector, target, source, from.Selector)
	c := e.run(port, from, `q`)
	c.Stdin = script
	return c, nil
}

// RemoveMembers is refused: a postgres repair removes array elements with a
// SET clause (array_remove).
func (postgres) RemoveMembers(int, Scope, MemberPredicate, int, int) (Command, error) {
	return Command{}, fmt.Errorf("postgres removes array members with a SET clause such as array_remove")
}
