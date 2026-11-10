package storeops

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"sdo.dev/operator/sdk/storestate"
)

// mysqlTags are the environment-variable tags under which a MySQL or MariaDB
// image keeps its credentials.
var mysqlTags = []string{"MYSQL", "MARIADB"}

// MySQLClient is a shell fragment, run after the store-state credential
// prelude (P password, U user), that defines q: the image's mysql or mariadb
// client against 127.0.0.1:$PORT as U (default root), in database $DB, with
// batch output and no column names. The password reaches the client through
// an option file on a here-document descriptor, never argv or output.
const MySQLClient = `U=${U:-root};W=$(printf '%s' "$P"|sed 's/[\\"]/\\&/g')
c=mysql;command -v mysql >/dev/null 2>&1||c=mariadb
q(){ $c --defaults-extra-file=/dev/fd/3 --protocol=TCP -h 127.0.0.1 -P "$PORT" -u "$U" --connect-timeout=2 -N -B ${DB:+-D "$DB"} "$@" 3<<SDOEOF
[client]
password="$W"
SDOEOF
}
`

// mysqlPrelude defines q for a record operation. Positional arguments: $1
// port, $2 database ("" is the container's MYSQL_DATABASE or
// MARIADB_DATABASE); they are shifted away.
const mysqlPrelude = `PORT="$1";DB="$2";shift 2;[ -n "$DB" ]||DB="${MYSQL_DATABASE:-${MARIADB_DATABASE:-}}"
[ -n "$DB" ]||{ echo "no database named and none in the container's environment" >&2;exit 1;}
` + MySQLClient

// mysqlColumns is the query that renders, for the table named by the second
// argument in the current database, one SQL expression per stored (not
// generated) column that prints the value as a literal: NULL, a hex literal
// for binary types, or the hex of its text introduced as utf8mb4, so a row
// is one line whatever its values hold. Joined with
// CONCAT_WS(',', ...) a row becomes the VALUES list of its own REPLACE.
const mysqlColumns = "SET SESSION group_concat_max_len=1048576; SELECT GROUP_CONCAT(CONCAT('IF(`',REPLACE(column_name,'`','``'),'` IS NULL,''NULL'',CONCAT(',IF(data_type IN('binary','varbinary','tinyblob','blob','mediumblob','longblob','geometry','point','linestring','polygon','bit'),'''X''''''','''_utf8mb4 X'''''''),',HEX(',IF(data_type IN('binary','varbinary','tinyblob','blob','mediumblob','longblob','geometry','point','linestring','polygon','bit'),CONCAT('`',REPLACE(column_name,'`','``'),'`'),CONCAT('CAST(`',REPLACE(column_name,'`','``'),'` AS CHAR)')),'),''''''''))') ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s AND generation_expression=''"

// mysqlColumnNames lists the stored columns, quoted and in order, for the
// REPLACE of a restore.
const mysqlColumnNames = "SET SESSION group_concat_max_len=1048576; SELECT GROUP_CONCAT(CONCAT('`',REPLACE(column_name,'`','``'),'`') ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s AND generation_expression=''"

var mysqlDatabase = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9_$-]{0,63}$`)

type mysql struct{}

func (mysql) Name() string     { return "mysql" }
func (mysql) DefaultPort() int { return 3306 }
func (mysql) Image() string    { return `(^|/)(mysql|mariadb|percona)[^/]*([:@]|$)` }

func (mysql) Normalize(s Scope) Scope {
	if strings.TrimSpace(s.Selector) == "" {
		s.Selector = "TRUE"
	}
	return s
}

// ValidateScope accepts a database (or the container's default), a table and
// the body of a WHERE clause.
func (e mysql) ValidateScope(s Scope) error {
	s = e.Normalize(s)
	if s.Database != "" && !mysqlDatabase.MatchString(s.Database) {
		return fmt.Errorf("mysql scope database %q is not a plain name", s.Database)
	}
	if !identifier.MatchString(s.Collection) {
		return fmt.Errorf("mysql scope collection %q is not a table name", s.Collection)
	}
	if err := mysqlText("mysql selector", s.Selector); err != nil {
		return err
	}
	return validateLimit(s)
}

// mysqlText refuses SQL text that could leave its clause; MySQL also reads
// "#" as a comment.
func mysqlText(what, text string) error {
	if err := sqlText(what, text); err != nil {
		return err
	}
	if strings.Contains(text, "#") {
		return fmt.Errorf("%s may not contain %q", what, "#")
	}
	return nil
}

// ValidateStatement accepts "delete" or a SET clause, applied to the rows the
// selector matches.
func (mysql) ValidateStatement(statement string) error {
	if statement == "delete" {
		return nil
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "SET ") {
		return fmt.Errorf("mysql repair statement must be \"delete\" or a SET clause")
	}
	if err := mysqlText("mysql repair statement", statement); err != nil {
		return err
	}
	if postgresClause.MatchString(topLevelSQL(statement)) {
		return fmt.Errorf("mysql repair statement may not carry its own top-level WHERE or RETURNING; the scope selects the rows (a subquery in parentheses may use them)")
	}
	return nil
}

func mysqlTable(name string) string { return "`" + name + "`" }

func mysqlString(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

func (e mysql) run(port int, s Scope, script string, args ...string) Command {
	return Command{Args: storestate.CredentialCommand(mysqlTags, mysqlPrelude+script, append([]string{strconv.Itoa(port), s.Database}, args...)...)}
}

func (e mysql) Count(port int, s Scope) Command {
	s = e.Normalize(s)
	return e.run(port, s, `q -e "$1"`, "SELECT CONCAT('sdo-count ',COUNT(*)) FROM "+mysqlTable(s.Collection)+" WHERE ("+s.Selector+")")
}

// Backup prints one VALUES list per row: each stored column as NULL or a hex
// literal.
func (e mysql) Backup(port int, s Scope) Command {
	s = e.Normalize(s)
	return e.run(port, s, `L="$4";cols=$(q -e "$1")||exit 1;case "$cols" in ''|NULL) echo "table $2 not found" >&2;exit 1;;esac
`+dumpWrapper(`q -e "SELECT CONCAT_WS(',',$cols) FROM $2 WHERE ($3)"`),
		fmt.Sprintf(mysqlColumns, mysqlString(s.Collection)), mysqlTable(s.Collection), s.Selector, strconv.Itoa(s.limit()))
}

// Write counts the rows under a lock and changes them in one transaction,
// only when their number is within [min, max].
func (e mysql) Write(port int, s Scope, statement string, min, max int) Command {
	s = e.Normalize(s)
	table := mysqlTable(s.Collection)
	inBounds := fmt.Sprintf("@sdo_n BETWEEN %d AND %d", min, max)
	change := "UPDATE " + table + " " + statement + " WHERE (" + s.Selector + ") AND " + inBounds
	if statement == "delete" {
		change = "DELETE FROM " + table + " WHERE (" + s.Selector + ") AND " + inBounds
	}
	c := e.run(port, s, `q`)
	c.Stdin = "START TRANSACTION;\nSELECT COUNT(*) INTO @sdo_n FROM " + table + " WHERE (" + s.Selector + ") FOR UPDATE;\n" + change + ";\n" +
		"SELECT IF(" + inBounds + ",CONCAT('sdo-affected ',ROW_COUNT()),CONCAT('sdo-expect-failed ',@sdo_n));\nCOMMIT;\n"
	return c
}

// Restore replaces, in one transaction, every backed-up row: REPLACE deletes
// the row with the same primary or unique key and inserts the backed-up one.
func (e mysql) Restore(port int, s Scope, dump string) (Command, error) {
	s = e.Normalize(s)
	c := e.run(port, s, `cols=$(q -e "$1")||exit 1;case "$cols" in ''|NULL) echo "table $2 not found" >&2;exit 1;;esac
T="$2";C="$cols";export T C
{ echo 'START TRANSACTION;';sed '$d'|awk '{print "REPLACE INTO " ENVIRON["T"] " (" ENVIRON["C"] ") VALUES (" $0 ");"}';echo 'COMMIT;';}|q&&echo "sdo-restored $3"`,
		fmt.Sprintf(mysqlColumnNames, mysqlString(s.Collection)), mysqlTable(s.Collection), strconv.Itoa(dumpItems(dump)))
	c.Stdin = dump
	return c, nil
}

func (mysql) Invalidate(int, Scope) (Command, error) {
	return Command{}, fmt.Errorf("mysql has no derived-store invalidation; rebuild the copy by hand")
}

// Copy inserts, in one transaction, every row of from that into lacks
// (INSERT IGNORE keeps a row whose primary or unique key into already
// holds), only when from matches [min, max] rows. The stored columns of into
// are copied by name, so from must have them all.
func (e mysql) Copy(port int, from, into Scope, min, max int) (Command, error) {
	from, into = e.Normalize(from), e.Normalize(into)
	if into.Selector != "TRUE" {
		return Command{}, fmt.Errorf("mysql copy target selects a whole table; leave its selector empty")
	}
	db := func(s Scope) string {
		if s.Database == "" {
			return ""
		}
		return mysqlTable(s.Database) + "."
	}
	source, target := db(from)+mysqlTable(from.Collection), db(into)+mysqlTable(into.Collection)
	inBounds := fmt.Sprintf("@sdo_n BETWEEN %d AND %d", min, max)
	intoDB := "DATABASE()"
	if into.Database != "" {
		intoDB = mysqlString(into.Database)
	}
	c := e.run(port, Scope{Database: from.Database}, `q`)
	c.Stdin = "SET SESSION group_concat_max_len=1048576;\nSTART TRANSACTION;\nSELECT COUNT(*) INTO @sdo_n FROM " + source + " WHERE (" + from.Selector + ") FOR UPDATE;\n" +
		"SELECT GROUP_CONCAT(CONCAT('`',REPLACE(column_name,'`','``'),'`') ORDER BY ordinal_position SEPARATOR ',') INTO @sdo_c FROM information_schema.columns WHERE table_schema=" + intoDB + " AND table_name=" + mysqlString(into.Collection) + " AND generation_expression='';\n" +
		"SET @sdo_q=CONCAT('INSERT IGNORE INTO " + strings.ReplaceAll(target, "'", "''") + " (',@sdo_c,') SELECT ',@sdo_c,' FROM " + strings.ReplaceAll(source, "'", "''") + " WHERE (', " + mysqlString(from.Selector) + ", ') AND " + inBounds + "');\n" +
		"PREPARE sdo_s FROM @sdo_q;\nEXECUTE sdo_s;\n" +
		"SELECT IF(" + inBounds + ",CONCAT('sdo-affected ',ROW_COUNT()),CONCAT('sdo-expect-failed ',@sdo_n));\nDEALLOCATE PREPARE sdo_s;\nCOMMIT;\n"
	return c, nil
}

func (mysql) RemoveMembers(int, Scope, MemberPredicate, int, int) (Command, error) {
	return Command{}, fmt.Errorf("mysql rows have no members; use a SET or delete statement")
}
