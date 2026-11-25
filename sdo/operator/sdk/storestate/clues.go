package storestate

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk/podread"
)

// KindUnknown is the kind of a change whose read does not say what changed.
const KindUnknown = "unknown"

// Bounds of one store Pod's rendered clues. A clue is dropped whole, never cut:
// the last line says how many were dropped and which kinds they were.
const (
	MaxClues     = 24
	MaxClueBytes = 2048
	// maxClueText bounds one rendered key or value.
	maxClueText = 160
	// maxSummaryText bounds each of the two lines that stand for several clues.
	maxSummaryText = 220
)

// Clue is one rendered change of a store: the kinds of object that changed,
// where, the values that differ and when the change was first read. The
// changes it stands for stay in StoreChanges.Changes.
type Clue struct {
	Kinds              []string
	Key, Before, After string
	// Named are the schema objects the change wrote, each whole ("trigger cut
	// on public.orders"); empty when routine or not recorded.
	Named string
	Noisy bool
	// Routine is churn an application makes by itself: a new table with its
	// own columns, indexes and constraints, or a count that moved a little.
	Routine bool
	// FirstObserved is the first reading that had the change and LastWithout
	// the reading before it; zero when no earlier reading was kept.
	FirstObserved, LastWithout time.Time

	place     string // "<db> <group>" or "<db>", for naming
	withTable bool
	aspects   []string
}

// changeClues renders changes as clues, unranked: a table group's added and
// removed halves are one clue per time it was first observed, so every part of
// a clue shares its date.
func changeClues(engine Engine, changes []Change, observed map[string]podread.Observed) []Clue {
	type side struct{ before, after *Item }
	groups, order := map[[2]string]*side{}, [][2]string(nil)
	newTables := map[string]bool{}
	for _, c := range changes {
		if k := c.Key(); k.Kind == KindNamespace && c.Before == nil && c.After != nil {
			newTables[k.Scope+"."+k.Name] = true
		}
	}
	var out []Clue
	for _, c := range changes {
		key := c.Key()
		if db, group, ok := groupOf(key); ok {
			id := [2]string{db, group}
			s := groups[id]
			if s == nil {
				s = &side{}
				groups[id], order = s, append(order, id)
			}
			if c.Before != nil {
				s.before = c.Before
			}
			if c.After != nil {
				s.after = c.After
			}
			continue
		}
		out = append(out, itemClue(engine, c, newTables))
	}
	for _, id := range order {
		out = append(out, groupClues(id[0], id[1], groups[id].before, groups[id].after, observed)...)
	}
	return out
}

func itemClue(engine Engine, c Change, newTables map[string]bool) Clue {
	key := c.Key()
	item := c.After
	if item == nil {
		item = c.Before
	}
	kind, clue := itemKind(engine, *item), Clue{Noisy: c.Noisy}
	clue.Kinds = []string{kind}
	clue.Key = strings.Replace(key.String(), string(key.Kind)+" ", kind+" ", 1)
	if c.Before != nil {
		clue.Before = c.Before.Value
	}
	if c.After != nil {
		clue.After = c.After.Value
	}
	if db, object, ok := strings.Cut(key.Scope, "."); ok && object == "{"+key.Name+"}" {
		// A count and digest over the whole database (catalogDigestItem).
		clue.Key, clue.place = kind+" definitions of "+db, db
		b, _, _ := strings.Cut(clue.Before, ":")
		a, _, _ := strings.Cut(clue.After, ":")
		if a == b {
			clue.Key += " changed in place"
		}
		clue.Before, clue.After = b, a
	}
	clue.aspects = []string{key.String()}
	switch key.Kind {
	case KindNamespace, KindKeyPattern, KindRegistryEntry:
		// A count moves with every read; it is dated by when it left its baseline.
	default:
		if c.After == nil {
			clue.aspects[0] += " absent"
		} else {
			clue.aspects[0] += "=" + c.After.Value
		}
	}
	clue.withTable = newTables[key.Scope]
	before, hasBefore := count(c.Before)
	after, hasAfter := count(c.After)
	switch key.Kind {
	case KindNamespace, KindKeyPattern, KindRegistryEntry:
		// A new table, a first estimate, and a count within twice its baseline
		// either way are growth; a larger move is listed by itself.
		clue.Routine = c.Before == nil && kind == "table" || c.Before != nil && !hasBefore && hasAfter ||
			hasBefore && hasAfter && after*2 >= before && after <= before*2
	case KindIndex, KindValidator:
		clue.Routine = clue.withTable && kind != "trigger"
	}
	return clue
}

// itemKind names the kind of object an item is.
func itemKind(engine Engine, item Item) string {
	switch {
	case item.Kind == KindNamespace && item.Scope != "":
		return "table"
	case item.Kind != KindValidator:
		if !knownKinds[item.Kind] {
			return KindUnknown
		}
		return string(item.Kind)
	case item.Name == "columns" && strings.HasSuffix(item.Scope, ".{columns}"):
		return "column"
	case item.Name == "functions" && strings.HasSuffix(item.Scope, ".{functions}"):
		return "function"
	case strings.HasPrefix(item.Value, "trigger "):
		return "trigger"
	case engine == EnginePostgres || engine == EngineMySQL:
		return "constraint"
	}
	return string(item.Kind)
}

// Clues are the store's changes for a reader, rarest kind first: a change of a
// kind few other changes share leads, one on tables that all still exist goes
// before one that came with a new table, and routine growth is one line with a
// count. The list is bounded by MaxClues and MaxClueBytes; what does not fit is
// one last line that says so.
func (s StoreChanges) Clues() []Clue {
	var clues, routine []Clue
	for _, clue := range changeClues(s.Engine, s.Changes, s.observed) {
		if seen, ok := s.observed[clue.aspects[0]]; ok {
			clue.FirstObserved, clue.LastWithout = seen.First, seen.LastWithout
		}
		if clue.Named = nameObjects(clue, s.written); clue.Routine && !clue.Noisy {
			routine = append(routine, clue)
			continue
		}
		clues = append(clues, clue)
	}
	// What came with a new table is counted apart, so it never makes the same
	// kind on an existing table common.
	shared := map[bool]map[string]int{false: {}, true: {}}
	for _, clue := range clues {
		for _, kind := range clue.Kinds {
			shared[clue.withTable][kind]++
		}
	}
	rarity := func(clue Clue) int {
		least := len(clues)
		for _, kind := range clue.Kinds {
			least = min(least, shared[clue.withTable][kind])
		}
		return least
	}
	sort.SliceStable(clues, func(i, j int) bool {
		a, b := clues[i], clues[j]
		switch {
		case a.Noisy != b.Noisy:
			return b.Noisy
		case a.withTable != b.withTable:
			return b.withTable
		case rarity(a) != rarity(b):
			return rarity(a) < rarity(b)
		case !a.FirstObserved.Equal(b.FirstObserved):
			return a.FirstObserved.After(b.FirstObserved)
		}
		return a.Key < b.Key
	})
	tail := []Clue(nil)
	if len(routine) > 0 {
		tail = append(tail, routineClue(routine))
	}
	return boundClues(clues, tail)
}

// routineClue is one line for all routine growth. Its time is the newest.
func routineClue(routine []Clue) Clue {
	out := Clue{Kinds: countKinds(routine), Routine: true}
	for _, clue := range routine {
		if clue.FirstObserved.After(out.FirstObserved) {
			out.FirstObserved, out.LastWithout = clue.FirstObserved, clue.LastWithout
		}
	}
	noun := "changes"
	if len(routine) == 1 {
		noun = "change"
	}
	out.Key = boundText(fmt.Sprintf("routine growth: %d %s (new tables with their own columns, indexes and constraints, or counts within twice their baseline): %s", len(routine), noun, strings.Join(out.Kinds, ", ")), maxSummaryText)
	return out
}

// boundClues keeps whole clues within the bounds and names what it dropped.
func boundClues(clues, tail []Clue) []Clue {
	var out []Clue
	used := 0
	for _, clue := range clues {
		clue.Key, clue.Before, clue.After = boundText(clue.Key, maxClueText), boundText(clue.Before, maxClueText), boundText(clue.After, maxClueText)
		size := len(clue.Key) + len(clue.Named) + len(clue.Before) + len(clue.After)
		if len(out) == MaxClues-2 || used+size > MaxClueBytes-2*(maxSummaryText) {
			break
		}
		out, used = append(out, clue), used+size
	}
	if dropped := clues[len(out):]; len(dropped) > 0 {
		kinds := countKinds(dropped)
		out = append(out, Clue{Kinds: kinds, Key: boundText(fmt.Sprintf("+%d more store changes not listed (%d-byte limit): %s", len(dropped), MaxClueBytes, strings.Join(kinds, ", ")), maxSummaryText)})
	}
	return append(out, tail...)
}

// countKinds lists the kinds of clues, each once, sorted.
func countKinds(clues []Clue) []string {
	var kinds []string
	for _, clue := range clues {
		kinds = append(kinds, clue.Kinds...)
	}
	slices.Sort(kinds)
	return slices.Compact(kinds)
}

// boundText shortens text to at most limit bytes at a word boundary and says
// how much is left out; a first word longer than the limit is replaced.
func boundText(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := strings.LastIndexByte(text[:limit-16], ' ')
	if cut <= 0 {
		return fmt.Sprintf("(a %d-byte value)", len(text))
	}
	return fmt.Sprintf("%s (+%d bytes)", strings.TrimRight(text[:cut], ", "), len(text)-cut)
}

// ObserveChanges returns when each change of a store-state result's readings
// from their baseline was first read, carrying previous (of the result's
// earlier readings). read is, by Pod, when that Pod last gave an answer before
// this result; it is updated. The result is empty for a result that is not a store-state read.
func ObserveChanges(id string, result podread.Result, previous map[string]map[string]podread.Observed, read map[string]time.Time) map[string]map[string]podread.Observed {
	engine, component, ok := ParseQueryID(id)
	if !ok || result.Error != "" {
		return previous
	}
	adapter, _ := AdapterFor(engine)
	baselines := promotedBaselines(adapter, component, result)
	out := map[string]map[string]podread.Observed{}
	for _, reading := range result.Readings {
		current, err := parseReading(adapter, component, reading)
		baseline, has := pick(baselines, reading.Pod)
		if err != nil || !has {
			// A read without an answer says nothing about what changed.
			if kept := previous[reading.Pod]; kept != nil {
				out[reading.Pod] = kept
			}
			continue
		}
		without := read[reading.Pod]
		if without.IsZero() || without.Before(baseline.At) {
			without = baseline.At
		}
		seen := map[string]podread.Observed{}
		for _, clue := range changeClues(engine, Diff(baseline, current), nil) {
			for _, aspect := range clue.aspects {
				if earlier, ok := previous[reading.Pod][aspect]; ok {
					seen[aspect] = earlier
				} else {
					seen[aspect] = podread.Observed{First: reading.ObservedAt, LastWithout: without}
				}
			}
		}
		out[reading.Pod], read[reading.Pod] = seen, reading.ObservedAt
	}
	return out
}
