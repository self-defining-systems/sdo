package storestate

import "strings"

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
