package storeops

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Bounds of one Read.
const (
	// MaxReadRows is the most rows one Read returns.
	MaxReadRows = 32
	// MaxReadFields is the most fields one Read projects.
	MaxReadFields = 16
	// MaxReadValueBytes is the longest string value one row may hold.
	MaxReadValueBytes = 256
	// ReadTimeoutMillis bounds one read statement where the engine has a
	// statement timeout.
	ReadTimeoutMillis = 3000
)

// CredentialName reports whether a column, field, or JSON-pointer segment is
// named like a credential: a password, hash, digest, token, key or the like.
// Read refuses such names in projections and selectors, and a password hash
// counts as a Secret value. The filter errs toward refusing.
func CredentialName(name string) bool {
	lower := strings.ToLower(name)
	var folded strings.Builder
	for _, r := range lower {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			folded.WriteRune(r)
		}
	}
	for _, word := range []string{"authorization", "cookie", "apikey", "token", "secret", "password", "passwd", "credential", "digest", "encrypted", "authenticationstring"} {
		if strings.Contains(folded.String(), word) {
			return true
		}
	}
	for _, segment := range nameSegments(name) {
		for _, prefix := range []string{"pass", "pwd", "hash", "salt", "otp"} {
			if strings.HasPrefix(segment, prefix) {
				return true
			}
		}
	}
	return strings.HasSuffix(lower, "_key") || strings.HasPrefix(lower, "key_") || strings.HasSuffix(lower, ".key") || strings.HasSuffix(name, "Key") && len(name) > 3
}

// nameSegments splits a name at non-alphanumerics and at lower-to-upper case
// changes, lowercased.
func nameSegments(name string) []string {
	var segments []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			segments = append(segments, strings.ToLower(string(current)))
			current = nil
		}
	}
	var previous rune
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			current = append(current, r)
		case r >= 'A' && r <= 'Z':
			if previous >= 'a' && previous <= 'z' {
				flush()
			}
			current = append(current, r)
		default:
			flush()
		}
		previous = r
	}
	flush()
	return segments
}

// ValidateRead checks one Read: the engine reads rows, the scope is valid and
// names no credential catalog, the projection is a bounded list of plain
// non-credential fields, and the selector is a column, operator and literal
// expression with no function call.
func ValidateRead(e Engine, s Scope, fields []string) error {
	if err := e.ValidateScope(s); err != nil {
		return err
	}
	s = e.Normalize(s)
	name := e.Name()
	if err := credentialCatalog(name, s); err != nil {
		return err
	}
	if len(fields) == 0 || len(fields) > MaxReadFields {
		return fmt.Errorf("a read projects 1..%d fields, got %d", MaxReadFields, len(fields))
	}
	seen := map[string]bool{}
	for _, field := range fields {
		if seen[field] {
			return fmt.Errorf("read field %q is listed twice", field)
		}
		seen[field] = true
		if err := readField(name, field); err != nil {
			return err
		}
	}
	switch name {
	case "postgres", "mysql":
		return validateSQLSelector(name, s.Selector)
	case "mongodb":
		return validateMongoSelector(s.Selector)
	}
	return fmt.Errorf("%s records are read by Count only: a read returns rows of named fields, and %s has none", name, name)
}

var (
	sqlField   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
	mongoField = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,62}(\.[A-Za-z_][A-Za-z0-9_-]{0,62}){0,7}$`)
)

func readField(engine, field string) error {
	pattern := sqlField
	if engine == "mongodb" {
		pattern = mongoField
	}
	if !pattern.MatchString(field) {
		return fmt.Errorf("read field %q is not a plain column or field path", field)
	}
	if CredentialName(field) {
		return fmt.Errorf("read field %q is named like a credential and is never readable", field)
	}
	return nil
}

// credentialCatalog refuses the scopes that hold an engine's credentials.
func credentialCatalog(engine string, s Scope) error {
	refuse := func(what string) error {
		return fmt.Errorf("%s holds credentials and is never readable", what)
	}
	switch engine {
	case "postgres":
		table := strings.ToLower(s.Collection)
		if i := strings.LastIndex(table, "."); i >= 0 {
			table = table[i+1:]
		}
		switch table {
		case "pg_authid", "pg_shadow", "pg_user_mapping", "pg_auth_members", "pg_db_role_setting":
			return refuse("postgres " + table)
		}
	case "mysql":
		if strings.EqualFold(s.Database, "mysql") {
			return refuse("the mysql system database")
		}
	case "mongodb":
		if strings.HasPrefix(s.Collection, "system.") || strings.EqualFold(s.Database, "admin") && strings.Contains(s.Collection, "system.") {
			return refuse("mongodb " + s.Database + "." + s.Collection)
		}
	}
	return nil
}

var sqlReservedWords = map[string]bool{
	"select": true, "from": true, "union": true, "case": true, "when": true, "then": true, "else": true, "end": true,
	"exists": true, "any": true, "all": true, "some": true, "with": true, "table": true, "values": true, "into": true,
	"cast": true, "interval": true, "user": true, "session_user": true, "system_user": true, "array": true,
	"row": true, "collate": true, "escape": true, "similar": true, "overlaps": true, "distinct": true, "group": true,
	"order": true, "limit": true, "offset": true, "having": true, "join": true, "for": true, "returning": true,
}

var sqlKeywords = map[string]bool{
	"and": true, "or": true, "not": true, "is": true, "in": true, "like": true, "ilike": true, "between": true,
}

var sqlLiteralWords = map[string]bool{"null": true, "true": true, "false": true}

// validateSQLSelector accepts only column names, comparison and arithmetic
// operators, AND/OR/NOT, IS [NOT] NULL, [NOT] IN (literals), LIKE, BETWEEN and
// literals. Anything that could call a function, run a subquery, cast, quote
// an identifier or read a variable is refused, so a selector cannot reach a
// side-effecting function such as a sleep, a lock or a file read.
func validateSQLSelector(engine, selector string) error {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%s read selector %q: "+format, append([]any{engine, selector}, args...)...)
	}
	type kind int
	const (
		start kind = iota
		operand
		operator
		open
		closeParen
	)
	previous, depth := start, 0
	text := selector
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case c == ' ' || c == '\t':
			i++
		case c == '\'':
			j := i + 1
			for {
				if j >= len(text) {
					return bad("unterminated string")
				}
				if text[j] == '\'' {
					if j+1 < len(text) && text[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if previous == operand || previous == closeParen {
				return bad("two values in a row")
			}
			previous = operand
			i = j + 1
		case c >= '0' && c <= '9':
			j := i
			for j < len(text) && (text[j] >= '0' && text[j] <= '9' || text[j] == '.') {
				j++
			}
			if previous == operand || previous == closeParen {
				return bad("two values in a row")
			}
			if j < len(text) && (text[j] == '_' || text[j] >= 'A' && text[j] <= 'Z' || text[j] >= 'a' && text[j] <= 'z') {
				return bad("malformed number")
			}
			previous = operand
			i = j
		case c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
			j := i
			for j < len(text) && (text[j] == '_' || text[j] >= 'A' && text[j] <= 'Z' || text[j] >= 'a' && text[j] <= 'z' || text[j] >= '0' && text[j] <= '9') {
				j++
			}
			word := strings.ToLower(text[i:j])
			i = j
			switch {
			case sqlReservedWords[word] || strings.HasPrefix(word, "current_"):
				return bad("%q is not allowed", word)
			case sqlKeywords[word]:
				previous = operator
			case sqlLiteralWords[word]:
				if previous == operand || previous == closeParen {
					return bad("two values in a row")
				}
				previous = operand
			default:
				if previous == operand || previous == closeParen {
					return bad("two values in a row")
				}
				if CredentialName(word) {
					return bad("column %q is named like a credential", word)
				}
				previous = operand
			}
		case c == '(':
			if previous == operand || previous == closeParen {
				return bad("a call or a subquery is not allowed")
			}
			depth++
			previous = open
			i++
		case c == ')':
			if depth == 0 || previous == operator {
				return bad("unbalanced parentheses")
			}
			depth--
			previous = closeParen
			i++
		case c == ',':
			if depth == 0 {
				return bad("a comma outside an IN list")
			}
			previous = operator
			i++
		case strings.IndexByte("=<>!+-*/%", c) >= 0:
			previous = operator
			i++
		default:
			return bad("character %q is not allowed", string(c))
		}
	}
	if depth != 0 || previous == operator {
		return bad("incomplete expression")
	}
	return nil
}

var mongoComparison = map[string]bool{
	"$eq": true, "$ne": true, "$gt": true, "$gte": true, "$lt": true, "$lte": true, "$in": true, "$nin": true,
	"$and": true, "$or": true, "$nor": true, "$not": true, "$exists": true,
}

// validateMongoSelector accepts only comparison and logical operators over
// plain field names: no $where, $function, $expr or $regex.
func validateMongoSelector(selector string) error {
	var filter map[string]any
	if err := json.Unmarshal([]byte(selector), &filter); err != nil {
		return fmt.Errorf("mongodb read selector must be a JSON filter document")
	}
	return walkMongoFilter(filter, 0)
}

func walkMongoFilter(value any, depth int) error {
	if depth > 8 {
		return fmt.Errorf("mongodb read selector is nested too deeply")
	}
	switch v := value.(type) {
	case map[string]any:
		for key, inner := range v {
			if strings.HasPrefix(key, "$") {
				if !mongoComparison[key] {
					return fmt.Errorf("mongodb read selector operator %q is not a comparison or logical operator", key)
				}
			} else if !mongoField.MatchString(key) || CredentialName(key) {
				return fmt.Errorf("mongodb read selector field %q is not a plain non-credential field", key)
			}
			if err := walkMongoFilter(inner, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, inner := range v {
			if err := walkMongoFilter(inner, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// readColumns renders the validated projection as quoted SQL columns.
func readColumns(fields []string, quote func(string) string) string {
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = quote(field)
	}
	return strings.Join(columns, ", ")
}

// Read prints "sdo-count <n>", then one JSON object per row (at most
// MaxReadRows, the fields named, in storage order), then "#sdo-end <rows>".
// It changes nothing: the selector passed ValidateRead.
func (e postgres) Read(port int, s Scope, fields []string) (Command, error) {
	if err := ValidateRead(e, s, fields); err != nil {
		return Command{}, err
	}
	s = e.Normalize(s)
	table, _ := postgresTable(s.Collection)
	columns := readColumns(fields, func(f string) string { return `"` + f + `"` })
	count := "SELECT 'sdo-count ' || count(*) FROM " + table + " WHERE (" + s.Selector + ")"
	rows := "SELECT row_to_json(r) FROM (SELECT " + columns + " FROM " + table + " WHERE (" + s.Selector + ") LIMIT " + strconv.Itoa(MaxReadRows) + ") r"
	return e.run(port, s, `export PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=`+strconv.Itoa(ReadTimeoutMillis)+`"
q -c "$1" || exit 1
L=`+strconv.Itoa(MaxReadRows)+`
`+dumpWrapper(`q -c "$2"`), count, rows), nil
}

func (e mysql) Read(port int, s Scope, fields []string) (Command, error) {
	if err := ValidateRead(e, s, fields); err != nil {
		return Command{}, err
	}
	s = e.Normalize(s)
	table := mysqlTable(s.Collection)
	pairs := make([]string, 0, 2*len(fields))
	for _, f := range fields {
		pairs = append(pairs, mysqlString(f), mysqlTable(f))
	}
	readOnly := "SET SESSION TRANSACTION READ ONLY; "
	count := readOnly + "SELECT CONCAT('sdo-count ',COUNT(*)) FROM " + table + " WHERE (" + s.Selector + ")"
	rows := readOnly + "SELECT JSON_OBJECT(" + strings.Join(pairs, ", ") + ") FROM " + table + " WHERE (" + s.Selector + ") LIMIT " + strconv.Itoa(MaxReadRows)
	return e.run(port, s, `q -r -e "$1" || exit 1
L=`+strconv.Itoa(MaxReadRows)+`
`+dumpWrapper(`q -r -e "$2"`), count, rows), nil
}

func (e mongodb) Read(port int, s Scope, fields []string) (Command, error) {
	if err := ValidateRead(e, s, fields); err != nil {
		return Command{}, err
	}
	s = e.Normalize(s)
	quoted := make([]string, len(fields))
	for i, f := range fields {
		quoted[i] = jsString(f)
	}
	js := e.collection(s) + " var fields = [" + strings.Join(quoted, ", ") + "]; print('sdo-count ' + c.countDocuments(f, {maxTimeMS: " + strconv.Itoa(ReadTimeoutMillis) + "})); var n = 0;" +
		" c.find(f).maxTimeMS(" + strconv.Itoa(ReadTimeoutMillis) + ").limit(" + strconv.Itoa(MaxReadRows) + ").forEach(function (d) { var o = {}; fields.forEach(function (p) { var v = d; p.split('.').forEach(function (k) { v = (v === null || v === undefined) ? undefined : v[k]; }); if (v !== undefined) { o[p] = v; } }); print(JSON.stringify(o)); n++; });" +
		" print('#sdo-end ' + n);"
	return Command{Args: []string{"sh", "-c", mongoPrelude + `m "$sh_" --quiet --eval "$1"`, "sh", strconv.Itoa(port), js}}, nil
}

func (redis) Read(int, Scope, []string) (Command, error) {
	return Command{}, fmt.Errorf("redis records are read by Count only")
}

func (memcached) Read(int, Scope, []string) (Command, error) {
	return Command{}, fmt.Errorf("memcached records are read by Count only")
}

// ParseRead reads a Read's output: the matched count, and the rows with every
// value a scalar (int64, float64, bool, string of at most MaxReadValueBytes,
// or nil). Output without the end marker, with a row count that differs from
// it, with a key outside fields, or with an array or object value is an
// error, never an empty answer.
func ParseRead(stdout string, fields []string) (int, []map[string]any, error) {
	allowed := map[string]bool{}
	for _, f := range fields {
		allowed[f] = true
	}
	count, haveCount, ended := 0, false, false
	var rows []map[string]any
	end := 0
	for _, line := range strings.Split(strings.TrimRight(stdout, "\n"), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
			continue
		case ended:
			return 0, nil, fmt.Errorf("read output continues after its end marker")
		case strings.HasPrefix(line, MarkerCount+" "):
			n, err := strconv.Atoi(strings.TrimPrefix(line, MarkerCount+" "))
			if err != nil || n < 0 || haveCount {
				return 0, nil, fmt.Errorf("read output has a malformed count line")
			}
			count, haveCount = n, true
		case strings.HasPrefix(line, MarkerEnd+" "):
			n, err := strconv.Atoi(strings.TrimPrefix(line, MarkerEnd+" "))
			if err != nil {
				return 0, nil, fmt.Errorf("read output has a malformed end marker")
			}
			end, ended = n, true
		default:
			if len(rows) >= MaxReadRows {
				return 0, nil, fmt.Errorf("read output holds more than %d rows", MaxReadRows)
			}
			row, err := parseReadRow(line, allowed)
			if err != nil {
				return 0, nil, err
			}
			rows = append(rows, row)
		}
	}
	switch {
	case !ended:
		return 0, nil, fmt.Errorf("read output has no end marker: the command stopped early")
	case !haveCount:
		return 0, nil, fmt.Errorf("read output has no count line")
	case end != len(rows):
		return 0, nil, fmt.Errorf("read output ended after %d rows but holds %d", end, len(rows))
	}
	return count, rows, nil
}

func parseReadRow(line string, allowed map[string]bool) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil || raw == nil {
		return nil, fmt.Errorf("read output holds a line that is not a row")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("read output holds a line that is not a row")
	}
	row := make(map[string]any, len(raw))
	for key, value := range raw {
		if !allowed[key] {
			return nil, fmt.Errorf("read output holds the field %q, which was not requested", key)
		}
		switch v := value.(type) {
		case nil, bool:
			row[key] = v
		case string:
			if len(v) > MaxReadValueBytes {
				return nil, fmt.Errorf("read field %q holds a string of more than %d bytes", key, MaxReadValueBytes)
			}
			row[key] = v
		case json.Number:
			if i, err := v.Int64(); err == nil {
				row[key] = i
			} else if f, err := v.Float64(); err == nil {
				row[key] = f
			} else {
				return nil, fmt.Errorf("read field %q holds a number that is not representable", key)
			}
		default:
			return nil, fmt.Errorf("read field %q is an array or object; a read returns scalars", key)
		}
	}
	return row, nil
}
