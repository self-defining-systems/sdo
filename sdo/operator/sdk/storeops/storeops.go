// Package storeops builds the engine-native commands SDO runs inside a data
// store's own container to count, back up, change, restore or invalidate the
// records of one scope. It is the record-level counterpart of storestate, which
// reads only value-free facts.
//
// Every command is a fixed shell template run with the store's own CLI. The
// scope and statement are validated per engine and reach the template as
// positional arguments or as escaped literals, never as unchecked program text.
// Credentials come from the container's environment and are never printed.
// Each command ends its output with a marker line that Parse reads, so a
// command that stopped early is never read as an answer.
package storeops

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Bounds of one scope.
const (
	// DefaultLimit is the item bound of a scope that sets none.
	DefaultLimit = 10000
	// MaxLimit is the largest item bound a scope may set.
	MaxLimit = 100000
)

// Markers of command output.
const (
	MarkerCount    = "sdo-count"
	MarkerAffected = "sdo-affected"
	MarkerExpect   = "sdo-expect-failed"
	MarkerRestored = "sdo-restored"
	// MarkerEnd is the last line of a backup dump: "#sdo-end <items>".
	MarkerEnd = "#sdo-end"
)

// Exit codes the templates use beyond the tool's own.
const (
	// ExitLimit means the scope holds more items than its limit.
	ExitLimit = 4
)

// Scope selects the records of one namespace with an engine-native, read-only
// selector.
//
//   - redis: Database is the logical database number ("" is 0), Collection is
//     empty and Selector is a key glob ("" is every key).
//   - mongodb: Database and Collection name the collection and Selector is a
//     JSON filter document ("" is {}).
//   - postgres: Database names the database ("" is the container's default),
//     Collection is a table or schema.table and Selector is the body of a WHERE
//     clause ("" is TRUE).
//
// Limit bounds how many items the scope may hold (0 is DefaultLimit).
type Scope struct {
	Database   string `json:"database,omitempty"`
	Collection string `json:"collection,omitempty"`
	Selector   string `json:"selector,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// Command is one argv run in the store's container, with its standard input.
type Command struct {
	Args  []string
	Stdin string
}

// Engine is one store engine's table of record operations. Every method takes
// a scope that passed ValidateScope.
type Engine interface {
	Name() string
	DefaultPort() int
	// Image is an RE2 pattern over a container image that selects the store's
	// own container in a Pod.
	Image() string
	// Normalize fills the engine's defaults into a scope.
	Normalize(Scope) Scope
	ValidateScope(Scope) error
	// ValidateStatement checks a write statement for RepairData.
	ValidateStatement(string) error
	// Count prints "sdo-count <n>": the items the scope selects.
	Count(port int, s Scope) Command
	// Backup prints one line per item, then "#sdo-end <n>". It exits
	// ExitLimit when the scope holds more than its limit.
	Backup(port int, s Scope) Command
	// Write applies statement to the items of the scope only if their number
	// is within [min, max]; it prints "sdo-affected <n>", or
	// "sdo-expect-failed <n>" having changed nothing where the engine can
	// make the check atomic.
	Write(port int, s Scope, statement string, min, max int) Command
	// Restore reads a Backup dump on stdin (its last line included), puts
	// every item back and prints "sdo-restored <n>". It does not delete items
	// created since the backup.
	Restore(port int, s Scope, dump string) (Command, error)
	// Invalidate drops the items of a derived store's scope so they are read
	// again from their source, and prints "sdo-affected <n>".
	Invalidate(port int, s Scope) (Command, error)
}

var engines = map[string]Engine{"redis": redis{}, "mongodb": mongodb{}, "postgres": postgres{}}

// For returns the engine's table, or false when SDO has no record operations
// for the engine.
func For(engine string) (Engine, bool) {
	e, ok := engines[engine]
	return e, ok
}

// Engines lists the engines with record operations.
func Engines() []string { return []string{"mongodb", "postgres", "redis"} }

// Covers reports whether a backup of scope backup holds every item of scope
// target: the same namespace, and the same selector or the whole namespace.
func Covers(e Engine, backup, target Scope) bool {
	b, t := e.Normalize(backup), e.Normalize(target)
	whole := e.Normalize(Scope{Database: b.Database, Collection: b.Collection}).Selector
	return b.Database == t.Database && b.Collection == t.Collection && (b.Selector == t.Selector || b.Selector == whole)
}

// Limit returns the scope's item bound.
func (s Scope) limit() int {
	if s.Limit == 0 {
		return DefaultLimit
	}
	return s.Limit
}

func validateLimit(s Scope) error {
	if s.Limit < 0 || s.Limit > MaxLimit {
		return fmt.Errorf("scope limit %d is outside 0..%d", s.Limit, MaxLimit)
	}
	return nil
}

// Parse reads the value of the last line "<marker> <n>" of stdout.
func Parse(stdout, marker string) (int, bool) {
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		rest, ok := strings.CutPrefix(strings.TrimSpace(lines[i]), marker+" ")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.Fields(rest + " x")[0])
		return n, err == nil && n >= 0
	}
	return 0, false
}

// dumpWrapper runs tool (a shell fragment whose stdout lines are the dump),
// bounds the line count by the scope limit, and appends "#sdo-end <n>" only
// when the tool succeeded. "$L" holds the limit.
func dumpWrapper(tool string) string {
	return `{ ` + tool + `; echo "#sdo-rc $?"; } | awk -v limit="$L" '
/^#sdo-rc / { rc = $2; next }
{ if (rc != "") bad = 1; n++; if (n > limit) { over = 1; exit } print }
END { if (over) exit ` + strconv.Itoa(ExitLimit) + `; if (bad || rc != "0") exit 3; print "#sdo-end " n }'`
}

// jsString renders s as a JavaScript string literal.
func jsString(s string) string {
	return strconv.Quote(s)
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// sqlText refuses text that could leave the clause it fills: a statement
// separator, a comment, a psql meta-command or a line break.
func sqlText(what, text string) error {
	for _, bad := range []string{";", "--", "/*", "\\", "\n", "\r", "\x00"} {
		if strings.Contains(text, bad) {
			return fmt.Errorf("%s may not contain %q", what, bad)
		}
	}
	return nil
}
