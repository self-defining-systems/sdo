package storeops

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// mongoPrelude defines m, which runs a MongoDB client against the store's own
// port with the root credentials of the container's environment when it has
// them, and sh_, the installed shell. Positional arguments: $1 port; it is
// shifted away.
const mongoPrelude = `P="$1";shift
m(){ if [ -n "${MONGO_INITDB_ROOT_USERNAME:-}" ]; then "$@" --host 127.0.0.1 --port "$P" --username "$MONGO_INITDB_ROOT_USERNAME" --password "${MONGO_INITDB_ROOT_PASSWORD:-}" --authenticationDatabase admin; else "$@" --host 127.0.0.1 --port "$P"; fi;}
sh_=$(command -v mongosh || command -v mongo) || exit 1
`

// mongoParse renders a JS expression that parses the JSON text s, as Extended
// JSON where the shell supports it.
func mongoParse(s string) string {
	return "(typeof EJSON !== 'undefined' ? EJSON.parse(" + jsString(s) + ") : JSON.parse(" + jsString(s) + "))"
}

type mongodb struct{}

func (mongodb) Name() string     { return "mongodb" }
func (mongodb) DefaultPort() int { return 27017 }
func (mongodb) Image() string    { return `(^|/)(mongo|mongodb)[^/]*([:@]|$)` }

func (mongodb) Normalize(s Scope) Scope {
	if strings.TrimSpace(s.Selector) == "" {
		s.Selector = "{}"
	}
	return s
}

func mongoName(what, name string) error {
	if name == "" || len(name) > 120 || strings.ContainsAny(name, "/\\. \"$\x00\n\r") {
		return fmt.Errorf("mongodb scope %s %q is not a plain name", what, name)
	}
	return nil
}

func (e mongodb) ValidateScope(s Scope) error {
	s = e.Normalize(s)
	if err := mongoName("database", s.Database); err != nil {
		return err
	}
	if s.Collection == "" || len(s.Collection) > 120 || strings.ContainsAny(s.Collection, "$\x00\n\r") || strings.HasPrefix(s.Collection, "system.") {
		return fmt.Errorf("mongodb scope collection %q is not a user collection name", s.Collection)
	}
	var filter map[string]any
	if err := json.Unmarshal([]byte(s.Selector), &filter); err != nil || filter == nil {
		return fmt.Errorf("mongodb selector must be a JSON filter document")
	}
	return validateLimit(s)
}

// ValidateStatement accepts "delete", a JSON update document whose every
// top-level key is an update operator ({"$rename": {"addr": "address"}}), or an
// update pipeline (a JSON array of stage documents).
func (mongodb) ValidateStatement(statement string) error {
	if statement == "delete" {
		return nil
	}
	var value any
	if err := json.Unmarshal([]byte(statement), &value); err != nil {
		return fmt.Errorf("mongodb repair statement must be \"delete\" or a JSON update document or pipeline")
	}
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 {
			return fmt.Errorf("mongodb update document is empty")
		}
		for key := range v {
			if !strings.HasPrefix(key, "$") {
				return fmt.Errorf("mongodb update document key %q is not an update operator; a replacement document is refused", key)
			}
		}
	case []any:
		if len(v) == 0 {
			return fmt.Errorf("mongodb update pipeline is empty")
		}
		for _, stage := range v {
			if _, ok := stage.(map[string]any); !ok {
				return fmt.Errorf("mongodb update pipeline stages must be documents")
			}
		}
	default:
		return fmt.Errorf("mongodb repair statement must be \"delete\" or a JSON update document or pipeline")
	}
	return nil
}

func (mongodb) collection(s Scope) string {
	return "var c = db.getSiblingDB(" + jsString(s.Database) + ").getCollection(" + jsString(s.Collection) + "); var f = " + mongoParse(s.Selector) + ";"
}

func (e mongodb) Count(port int, s Scope) Command {
	s = e.Normalize(s)
	js := e.collection(s) + " print('sdo-count ' + c.countDocuments(f));"
	return Command{Args: []string{"sh", "-c", mongoPrelude + `m "$sh_" --quiet --eval "$1"`, "sh", strconv.Itoa(port), js}}
}

func (e mongodb) Backup(port int, s Scope) Command {
	s = e.Normalize(s)
	return Command{Args: []string{"sh", "-c", mongoPrelude + `L="$4"
` + dumpWrapper(`m mongoexport --quiet --db "$1" --collection "$2" --query "$3" --jsonFormat=canonical`), "sh", strconv.Itoa(port), s.Database, s.Collection, s.Selector, strconv.Itoa(s.limit())}}
}

// Write counts and writes in one shell session. MongoDB gives no atomic
// count-and-update across documents, so a write racing the count can change a
// different number than counted; the executor counts again afterwards.
func (e mongodb) Write(port int, s Scope, statement string, min, max int) Command {
	s = e.Normalize(s)
	write := "var r = c.updateMany(f, " + mongoParse(statement) + "); print('sdo-affected ' + r.modifiedCount);"
	if statement == "delete" {
		write = "var r = c.deleteMany(f); print('sdo-affected ' + r.deletedCount);"
	}
	js := e.collection(s) + fmt.Sprintf(" var n = c.countDocuments(f); if (n < %d || n > %d) { print('sdo-expect-failed ' + n); } else { %s }", min, max, write)
	return Command{Args: []string{"sh", "-c", mongoPrelude + `m "$sh_" --quiet --eval "$1"`, "sh", strconv.Itoa(port), js}}
}

// Restore upserts every backed-up document by _id, replacing its current
// version.
func (e mongodb) Restore(port int, s Scope, dump string) (Command, error) {
	s = e.Normalize(s)
	return Command{Args: []string{"sh", "-c", mongoPrelude + `sed '$d' | m mongoimport --quiet --db "$1" --collection "$2" --mode=upsert && echo "sdo-restored $3"`, "sh", strconv.Itoa(port), s.Database, s.Collection, strconv.Itoa(dumpItems(dump))}, Stdin: dump}, nil
}

func (mongodb) Invalidate(int, Scope) (Command, error) {
	return Command{}, fmt.Errorf("mongodb has no derived-store invalidation; rebuild the copy by hand")
}

// dumpItems counts the item lines of a Backup dump.
func dumpItems(dump string) int {
	n, _ := Parse(dump, MarkerEnd)
	return n
}
