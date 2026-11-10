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
//   - mysql: Database names the database ("" is the container's
//     MYSQL_DATABASE), Collection is a table and Selector is the body of a
//     WHERE clause ("" is TRUE).
//   - memcached: Database and Collection are empty and Selector is a key glob
//     ("" is every key); keys are listed with lru_crawler metadump.
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
	// Copy puts every item of from that into lacks into into's namespace,
	// keeping items into already holds (a set or sorted set is merged
	// member by member on redis), only when from matches [min, max] items;
	// it prints "sdo-affected <n>" (items written) or "sdo-expect-failed
	// <n>". from is left as it is.
	Copy(port int, from, into Scope, min, max int) (Command, error)
	// RemoveMembers removes, from every set, sorted set or list value the
	// scope selects, the members the predicate selects, only when the scope
	// matches [min, max] keys; it prints "sdo-affected <members removed>
	// failed <keys of another type>" or "sdo-expect-failed <n>".
	RemoveMembers(port int, s Scope, p MemberPredicate, min, max int) (Command, error)
}

// MemberPredicate selects the members of a collection value to remove. A
// member is removed when any condition set holds:
//   - NotDigits: the member is not a decimal integer;
//   - MissingKey: the key this pattern names, with {member} replaced by the
//     member, does not exist in the same database (a dangling reference);
//   - In: the member is one of these;
//   - NotIn: the member is none of these (set NotIn to the valid members).
type MemberPredicate struct {
	NotDigits  bool     `json:"not_digits,omitempty"`
	MissingKey string   `json:"missing_key,omitempty"`
	In         []string `json:"in,omitempty"`
	NotIn      []string `json:"not_in,omitempty"`
}

// MaxPredicateMembers bounds the members In and NotIn may each list.
const MaxPredicateMembers = 1000

// Validate requires at least one condition and bounded member lists.
func (p MemberPredicate) Validate() error {
	if !p.NotDigits && p.MissingKey == "" && len(p.In) == 0 && len(p.NotIn) == 0 {
		return fmt.Errorf("member predicate sets no condition (not_digits, missing_key, in, not_in)")
	}
	if p.MissingKey != "" && (strings.Count(p.MissingKey, "{member}") != 1 || len(p.MissingKey) > 512 || strings.ContainsAny(p.MissingKey, "\n\r\x00")) {
		return fmt.Errorf("member predicate missing_key %q must hold {member} once, on one line of at most 512 bytes", p.MissingKey)
	}
	for _, list := range [][]string{p.In, p.NotIn} {
		if len(list) > MaxPredicateMembers {
			return fmt.Errorf("member predicate lists %d members, more than %d", len(list), MaxPredicateMembers)
		}
		for _, m := range list {
			if len(m) > 512 || strings.ContainsAny(m, "\x00") {
				return fmt.Errorf("member predicate member must be at most 512 bytes without NUL")
			}
		}
	}
	return nil
}

var engines = map[string]Engine{"redis": redis{}, "mongodb": mongodb{}, "postgres": postgres{}, "mysql": mysql{}, "memcached": memcached{}}

// For returns the engine's table, or false when SDO has no record operations
// for the engine.
func For(engine string) (Engine, bool) {
	e, ok := engines[engine]
	return e, ok
}

// Engines lists the engines with record operations.
func Engines() []string { return []string{"memcached", "mongodb", "mysql", "postgres", "redis"} }

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

// ParseFailed reads the items a write rejected, from "sdo-affected <n> failed
// <m>"; engines that apply a write atomically report none.
func ParseFailed(stdout string) int {
	for _, line := range strings.Split(stdout, "\n") {
		if !strings.HasPrefix(line, MarkerAffected+" ") {
			continue
		}
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "failed" {
				n, _ := strconv.Atoi(fields[i+1])
				return n
			}
		}
	}
	return 0
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
END { if (over || rc == "` + strconv.Itoa(ExitLimit) + `") exit ` + strconv.Itoa(ExitLimit) + `; if (bad || rc != "0") exit 3; print "#sdo-end " (n + 0) }'`
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
