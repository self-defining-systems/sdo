package storestate

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// A postgres group value carries, after the table count, the count and digest
// of each kind of object the group's tables have ("137 i412:89f0 c88:1b2c
// t14:99aa n1800:0c0d": indexes, constraints, triggers, columns), so a changed
// group names the kind. Other engines' values carry a table count and one
// digest, which name no kind. Triggers and constraints act on writes, so they
// lead.
var groupKinds = []struct{ tag, one, many string }{{"t", "trigger", "triggers"}, {"c", "constraint", "constraints"}, {"i", "index", "indexes"}, {"n", "column", "columns"}}

var groupField = regexp.MustCompile(`^([a-z])(\d+)(?::([0-9a-f]+))?$`)

// kindState is one kind's object count and definition digest.
type kindState struct {
	count  int
	digest string
}

// groupCounts parses a group value into its table count (-1 when unreadable)
// and the state of each kind the value carries, by tag.
func groupCounts(value string) (int, map[string]kindState) {
	fields, kinds := strings.Fields(value), map[string]kindState{}
	if len(fields) == 0 {
		return -1, kinds
	}
	tables, err := strconv.Atoi(fields[0])
	if err != nil {
		tables = -1
	}
	for _, field := range fields[1:] {
		if m := groupField.FindStringSubmatch(field); m != nil {
			if n, err := strconv.Atoi(m[2]); err == nil {
				kinds[m[1]] = kindState{n, m[3]}
			}
		}
	}
	return tables, kinds
}

// judgedGroupValue is a group value without its columns, which are listed as a
// change but never judged.
func judgedGroupValue(value string) string {
	before, _, _ := strings.Cut(value, " n")
	return before
}

func signedCount(n int, one, many string) string {
	if n == 1 || n == -1 {
		return fmt.Sprintf("%+d %s", n, one)
	}
	return fmt.Sprintf("%+d %s", n, many)
}

func plainCount(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// groupOf names the database and table group of a catalog group digest.
func groupOf(k Key) (db, group string, ok bool) {
	db, object, scoped := strings.Cut(k.Scope, ".")
	if !scoped || !isCatalogGroup(object) || k.Name != catalogGroupName {
		return "", "", false
	}
	group, _, _ = strings.Cut(strings.TrimSuffix(strings.TrimPrefix(object, catalogGroupPrefix), "}"), " ")
	return db, group, true
}

// groupClue renders the two sides of one table group. A table that appeared or
// disappeared moves a group to a new scope, so the sides may come from an added
// and a removed item; either way the group's own counts say what changed.
func groupClue(db, group string, before, after *Item) Clue {
	place := fmt.Sprintf("in %s table group %s of %s", db, group, catalogGroups)
	if before == nil || after == nil {
		how := "appeared"
		if after == nil {
			how = "disappeared"
		}
		return Clue{Kinds: []string{KindUnknown}, Key: "table group " + how + " " + place, aspects: []string{place + " " + how}}
	}
	bt, bk := groupCounts(before.Value)
	at, ak := groupCounts(after.Value)
	clue, deltas, was, is := Clue{}, []string(nil), []string(nil), []string(nil)
	add := func(kind, delta, beforeText, afterText, aspect string) {
		clue.Kinds, deltas = append(clue.Kinds, kind), append(deltas, delta)
		was, is = append(was, beforeText), append(is, afterText)
		clue.aspects = append(clue.aspects, place+" "+kind+" "+aspect)
	}
	ownScope := before.Scope == after.Scope
	for _, kind := range groupKinds {
		b, hadKind := bk[kind.tag]
		a, hasKind := ak[kind.tag]
		if !hadKind || !hasKind {
			continue
		}
		switch {
		case a.count != b.count:
			add(kind.one, signedCount(a.count-b.count, kind.one, kind.many), plainCount(b.count, kind.one, kind.many), plainCount(a.count, kind.one, kind.many), strconv.Itoa(a.count))
		case ownScope && a.digest != b.digest && a.digest != "" && b.digest != "":
			add(kind.one, kind.one+" changed in place", plainCount(b.count, kind.one, kind.many), plainCount(a.count, kind.one, kind.many), a.digest)
		}
	}
	tablesMoved := !ownScope
	if at != bt && at >= 0 && bt >= 0 {
		add("table", signedCount(at-bt, "table", "tables"), plainCount(bt, "table", "tables"), plainCount(at, "table", "tables"), strconv.Itoa(at))
		tablesMoved = true
	} else if tablesMoved {
		add("table", "tables replaced or renamed (same count)", "", "", after.Scope)
	}
	if len(deltas) == 0 {
		return Clue{Kinds: []string{KindUnknown}, Key: "definition changed (kind not recorded) " + place, aspects: []string{place + " definition " + after.Value}}
	}
	clue.withTable = tablesMoved
	clue.Key = strings.Join(deltas, ", ") + " " + place
	clue.Before, clue.After = joinCounts(was), joinCounts(is)
	// A new table brings its own columns, indexes and constraints.
	clue.Routine = tablesMoved && at > bt && bt >= 0 && !slices.Contains(clue.Kinds, "trigger")
	for _, tag := range []string{"i", "c", "n"} {
		if ak[tag].count < bk[tag].count {
			clue.Routine = false
		}
	}
	return clue
}

func joinCounts(counts []string) string {
	return strings.Join(slices.DeleteFunc(slices.Clone(counts), func(s string) bool { return s == "" }), ", ")
}

// A catalog too large for the read's output budget (hundreds of tables, a
// thousand constraints and indexes) is not cut: the read digests it by group
// instead. Every table (or collection) falls into one of catalogGroups (16) groups
// by a hash of its name, and the read prints one line per group: the group
// number with a digest of its table names, and the table count with digests
// of the definitions of their constraints, indexes, triggers and columns. The read is
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

// catalogDigestItem is the count and digest of one kind of object over a whole
// database ("columns" or "functions"). Its scope names no table, so it is
// listed as a change and never judged as one.
func catalogDigestItem(db, kind, value string) Item {
	return Item{Kind: KindValidator, Scope: db + ".{" + kind + "}", Name: kind, Value: value}
}
