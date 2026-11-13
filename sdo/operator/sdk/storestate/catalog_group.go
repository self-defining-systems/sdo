package storestate

import (
	"fmt"
	"strconv"
	"strings"
)

// A postgres group value carries, after the table count and definition digest,
// the group's index, constraint and trigger counts ("137 89f0cb69 i412 c88
// t14"), so a changed group says what kind of object was added or dropped.
// Other engines' values carry the first two fields only.
var groupKinds = []struct{ tag, one, many string }{{"i", "index", "indexes"}, {"c", "constraint", "constraints"}, {"t", "trigger", "triggers"}}

// groupCounts parses a group value into its table count and the per-kind
// counts, which are ok only when the value carries them.
func groupCounts(value string) (tables int, kinds [3]int, ok bool) {
	fields := strings.Fields(value)
	if len(fields) != 5 {
		return 0, kinds, false
	}
	tables, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, kinds, false
	}
	for i, kind := range groupKinds {
		n, err := strconv.Atoi(strings.TrimPrefix(fields[2+i], kind.tag))
		if err != nil || !strings.HasPrefix(fields[2+i], kind.tag) {
			return 0, kinds, false
		}
		kinds[i] = n
	}
	return tables, kinds, true
}

func signedCount(n int, one, many string) string {
	if n == 1 || n == -1 {
		return fmt.Sprintf("%+d %s", n, one)
	}
	return fmt.Sprintf("%+d %s", n, many)
}

// describeGroup renders a changed group digest in readable terms: the database,
// the group, and which kind of object was added or dropped. ok is false when
// the change is not a group digest.
func describeGroup(c Change) (key, before, after string, ok bool) {
	k := c.Key()
	db, object, scoped := strings.Cut(k.Scope, ".")
	if !scoped || !isCatalogGroup(object) || k.Name != catalogGroupName {
		return "", "", "", false
	}
	group, _, _ := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(object, catalogGroupPrefix), "}"), " ")
	key = fmt.Sprintf("%s definitions (table group %s of %s)", db, group, catalogGroups)
	if c.Before == nil || c.After == nil {
		if c.After == nil {
			return key + " removed", "", "", true
		}
		return key + " added", "", "", true
	}
	bt, bk, bok := groupCounts(c.Before.Value)
	at, ak, aok := groupCounts(c.After.Value)
	if !bok || !aok {
		return key + ": a definition changed", "", "", true
	}
	var deltas []string
	if at != bt {
		deltas = append(deltas, signedCount(at-bt, "table", "tables"))
	}
	for i, kind := range groupKinds {
		if ak[i] != bk[i] {
			deltas = append(deltas, signedCount(ak[i]-bk[i], kind.one, kind.many))
		}
	}
	if len(deltas) == 0 {
		deltas = []string{"a definition changed in place (same object counts)"}
	}
	counts := func(tables int, kinds [3]int) string {
		return fmt.Sprintf("%d tables, %d indexes, %d constraints, %d triggers", tables, kinds[0], kinds[1], kinds[2])
	}
	return key + ": " + strings.Join(deltas, ", "), counts(bt, bk), counts(at, ak), true
}

// A catalog too large for the read's output budget (hundreds of tables, a
// thousand constraints and indexes) is not cut: the read digests it by group
// instead. Every table (or collection) falls into one of catalogGroups (16) groups
// by a hash of its name, and the read prints one line per group: the group
// number with a digest of its table names, and the table count with a digest
// of the definitions of their constraints, indexes and triggers. The read is
// complete and bounded whatever the schema's size, and stays value-free.
//
// A group is one item whose scope names the group and its table digest
// ("<db>.{table group 05 1a2b3c4d}"). A definition changed on tables that all
// still exist keeps the scope and changes the value, which is judged like a
// constraint changed on a live table; a table that appeared or disappeared (a
// new time partition) moves the group to a new scope, which is the table's own
// lifecycle and not judged.
const catalogGroups = "16"

// catalogGroupPrefix starts the object name of a group digest.
const catalogGroupPrefix = "{table group "

// catalogGroupName is the item name of a group's definition digest.
const catalogGroupName = "definitions"

// catalogGroupItem returns the digest item of one group: group is "<group>
// <table digest>" and value "<tables> <definition digest>".
func catalogGroupItem(kind ItemKind, db, group, value string) Item {
	return Item{Kind: kind, Scope: db + "." + catalogGroupPrefix + group + "}", Name: catalogGroupName, Value: value}
}

// isCatalogGroup reports whether a catalog item's object is a group digest.
func isCatalogGroup(object string) bool {
	return strings.HasPrefix(object, catalogGroupPrefix)
}
