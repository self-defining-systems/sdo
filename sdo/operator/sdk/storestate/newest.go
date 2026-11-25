package storestate

import (
	"crypto/md5"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// maxNamedText bounds one clue's names; a name that does not fit is left out whole.
const maxNamedText = 200

// NewObject is one schema object by the transaction that last wrote its
// definition. Table is empty for a function.
type NewObject struct {
	DB    string `json:"db"`
	Kind  string `json:"kind"`
	Table string `json:"table,omitempty"`
	Name  string `json:"name"`
	XID   uint32 `json:"xid"`
}

// parseNewObject reads a "<db>\t<kind> <xid>\t<table>\t<name>" line.
func parseNewObject(line string) (NewObject, bool) {
	f := strings.Split(line, "\t")
	if len(f) != 4 || f[3] == "" {
		return NewObject{}, false
	}
	kind, xid, _ := strings.Cut(f[1], " ")
	n, err := strconv.ParseUint(xid, 10, 32)
	return NewObject{DB: f[0], Kind: kind, Table: f[2], Name: f[3], XID: uint32(n)}, err == nil
}

// writtenSince lists current's newest objects that baseline's list lacks and
// that were written after the oldest it lists: an older object is only listed
// now because a newer one was dropped. A baseline whose list was cut or unread is
// no reference: what it lacks may be old, so nothing is named.
func writtenSince(baseline, current Facts) []NewObject {
	if !baseline.NewestComplete {
		return nil
	}
	type list struct{ db, kind string }
	oldest := map[list]uint32{}
	for _, o := range baseline.Newest {
		at, seen := oldest[list{o.DB, o.Kind}]
		// Transaction ids wrap, so they compare by signed difference.
		if !seen || int32(o.XID-at) < 0 {
			oldest[list{o.DB, o.Kind}] = o.XID
		}
	}
	var out []NewObject
	for _, o := range current.Newest {
		at, seen := oldest[list{o.DB, o.Kind}]
		if !slices.Contains(baseline.Newest, o) && (!seen || int32(o.XID-at) > 0) {
			out = append(out, o)
		}
	}
	return out
}

// nameObjects names the written objects a clue stands for: those of its kinds
// in its table group, or in its database. Routine and unplaced clues name none.
func nameObjects(clue Clue, written []NewObject) string {
	if clue.Routine || clue.place == "" {
		return ""
	}
	var names []string
	used, left := 0, 0
	written = slices.Clone(written)
	slices.SortStableFunc(written, func(a, b NewObject) int {
		return slices.Index(clue.Kinds, a.Kind) - slices.Index(clue.Kinds, b.Kind)
	})
	for _, o := range written {
		place := o.DB
		if o.Table != "" && strings.Contains(clue.place, " ") {
			sum := md5.Sum([]byte(o.Table))
			groups, _ := strconv.Atoi(catalogGroups)
			place = fmt.Sprintf("%s %02d", o.DB, int(sum[0])%groups)
		}
		if place != clue.place || !slices.Contains(clue.Kinds, o.Kind) {
			continue
		}
		name := o.Kind + " " + o.Name
		if o.Table != "" {
			name += " on " + o.Table
		}
		if used+len(name) > maxNamedText {
			left++
			continue
		}
		names, used = append(names, name), used+len(name)+2
	}
	if left > 0 {
		names = append(names, fmt.Sprintf("%d with longer names not shown", left))
	}
	return strings.Join(names, ", ")
}
