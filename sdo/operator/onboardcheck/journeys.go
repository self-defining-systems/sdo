package onboardcheck

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
)

// Items behind the journey table's probe and map columns.
const (
	// ItemProbe is red for an operation no probe exercises with a result check
	// (a write needs a probe that reads it back), and for a probe the healthy
	// window found failing or unable to judge.
	ItemProbe = "probe-checks-result"
	// ItemPointers is red for an operation whose path lists a component with no
	// metrics or logs pointer.
	ItemPointers = appmap.ItemMapPointers
	// ItemMapAccuracy compares, per broken component, the operations that
	// failed with the operations whose path lists it: a missing edge is red, a
	// spurious one a note.
	ItemMapAccuracy = "map-accuracy"
)

// Cell is one journey's result on one item of the standard.
type Cell struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	// NotRun says the cell could not be judged in this run (no --break); a
	// journey with a cell not run is not complete.
	NotRun bool `json:"not_run,omitempty"`
}

// Journey is one declared operation's row of the checklist: (a) mapping,
// (b) errors, (c) latency, (d) result-checking probe, (e) detection, (f) map,
// (g) traces. A through F must be complete for the report to be clean; G is a
// note.
type Journey struct {
	Operation string `json:"operation"`
	// Given is the human-given journey the operation serves, if any.
	Given     string `json:"journey,omitempty"`
	Mapping   Cell   `json:"mapping"`
	Errors    Cell   `json:"errors"`
	Latency   Cell   `json:"latency"`
	Probe     Cell   `json:"probe"`
	Detection Cell   `json:"detection"`
	Map       Cell   `json:"map"`
	Traces    Cell   `json:"traces"`
}

func (j Journey) cells() []Cell {
	return []Cell{j.Mapping, j.Errors, j.Latency, j.Probe, j.Detection, j.Map}
}

// Complete is true when (a) through (f) are judged and none is red.
func (j Journey) Complete() bool {
	for _, c := range j.cells() {
		if c.Status == Red || c.NotRun {
			return false
		}
	}
	return true
}

// opAccuracy is what breaking components showed about one journey's path.
type opAccuracy struct {
	confirmed []string
	spurious  []string
	missing   []string
}

func onPath(op appmap.Operation, component string) bool {
	for _, n := range op.Path {
		if n == component {
			return true
		}
	}
	return false
}

// judgeMapAccuracy compares, for every component that was broken, the
// operations that failed with the operations whose path lists it.
func judgeMapAccuracy(m appmap.Map, order []string, outcomes map[string]*breakOutcome, report *Report) {
	if report.accuracy == nil {
		report.accuracy = map[string]*opAccuracy{}
	}
	acc := func(id string) *opAccuracy {
		if report.accuracy[id] == nil {
			report.accuracy[id] = &opAccuracy{}
		}
		return report.accuracy[id]
	}
	for _, name := range order {
		out := outcomes[name]
		if out == nil || out.how == "no workload" || out.err != "" {
			continue
		}
		var confirmed, spurious, missing []string
		for _, op := range m.Operations {
			if !onPath(op, name) {
				continue
			}
			if out.opFailure[op.ID] != nil {
				confirmed = append(confirmed, op.ID)
				acc(op.ID).confirmed = append(acc(op.ID).confirmed, name)
			} else {
				spurious = append(spurious, op.ID)
				acc(op.ID).spurious = append(acc(op.ID).spurious, name)
			}
		}
		for id := range out.stray {
			missing = append(missing, id)
			acc(id).missing = append(acc(id).missing, strings.Join(out.wave, "+"))
		}
		sort.Strings(missing)
		switch {
		case len(missing) > 0:
			report.add(ItemMapAccuracy, name, Red, fmt.Sprintf("missing edges: %s failed (a failing request or probe step named it) while %s was broken, but its path does not list %s; add %s to the path of %s so detection and localization follow it", strings.Join(missing, ", "), strings.Join(out.wave, " and "), name, name, strings.Join(missing, ", ")))
		case len(spurious) > 0:
			report.add(ItemMapAccuracy, name, Note, fmt.Sprintf("spurious edges: %s lists %s on its path, but no failure of it was seen while %s was broken (the path may be wrong, or no probe or request metric catches it); confirmed: %s", strings.Join(spurious, ", "), name, name, none(confirmed)))
		default:
			report.add(ItemMapAccuracy, name, Green, fmt.Sprintf("operations that failed match the paths that list it: %s", none(confirmed)))
		}
	}
}

func none(s []string) string {
	if len(s) == 0 {
		return "none"
	}
	return strings.Join(s, ", ")
}

// judgeProbesAndPointers adds the per-operation probe and pointer results.
func judgeProbesAndPointers(cfg Config, report *Report) {
	m := cfg.Map
	byName := map[string]appmap.Component{}
	for _, c := range m.Components {
		byName[c.Name] = c
	}
	for _, op := range m.Operations {
		var scenarios []string
		reads := false
		needsRead := op.Writes || len(op.Triggers) > 0
		for _, p := range m.Probes {
			for _, id := range p.Operations {
				if id == op.ID {
					scenarios = append(scenarios, p.Scenario)
					reads = reads || p.Kind == appmap.KindWriteRead || p.Kind == appmap.KindReadLater
				}
			}
		}
		switch {
		case len(scenarios) == 0:
			report.add(ItemProbe, op.ID, Red, "no probe exercises this operation: add a probe whose step calls it and checks the result (data written reads back intact, background work completes, existing records still right)")
		case needsRead && !reads:
			report.add(ItemProbe, op.ID, Red, fmt.Sprintf("writes data or triggers background work (%s) but only call probes exercise it (%s): add a write-read or read-later probe that waits for the triggered work to finish, reads its effect as a user would and cleans up", none(op.Triggers), strings.Join(scenarios, ", ")))
		default:
			if why := probeTrouble(*report, op, scenarios); why != "" {
				report.add(ItemProbe, op.ID, Red, why)
			} else {
				report.add(ItemProbe, op.ID, Green, "probes: "+strings.Join(scenarios, ", "))
			}
		}
		var missing []string
		for _, n := range op.Path {
			if o := byName[n].Observe; o == nil || o.Metrics == "" || o.Logs == "" {
				missing = append(missing, n)
			}
		}
		if len(op.Path) == 0 {
			report.add(ItemPointers, op.ID, Red, "the operation has no component path")
		} else if len(missing) > 0 {
			report.add(ItemPointers, op.ID, Red, pointersDetail(missing))
		} else {
			report.add(ItemPointers, op.ID, Green, "every component on the path has metrics and logs pointers")
		}
	}
}

// probeTrouble says why the healthy-window items found a probe of the
// operation failing, pending or not exercised ("" when they did not).
func probeTrouble(r Report, op appmap.Operation, scenarios []string) string {
	for _, x := range r.Results {
		if x.Status != Red || (x.Item != ItemQuiet && x.Item != ItemHarm) {
			continue
		}
		text := strings.ToLower(x.Subject + " " + x.Detail)
		for _, s := range append([]string{op.ID}, scenarios...) {
			if named(text, s) {
				return fmt.Sprintf("%s: %s", x.Item, clip(x.Detail, 200))
			}
		}
	}
	return ""
}

var statusClasses = map[string][]string{"client": {"400", "404", "429"}, "server": {"500", "502", "503"}}

// errorClasses says what is wrong with an error pattern over HTTP status
// values: it must count server errors and leave client errors out ("" when
// right or when the status values are not HTTP codes).
func errorClasses(errors string, observed []string) string {
	re, err := regexp.Compile("^(?:" + errors + ")$")
	if err != nil {
		return ""
	}
	http := regexp.MustCompile(`^\d{3}$`)
	for _, v := range observed {
		if !http.MatchString(v) {
			return ""
		}
	}
	var miss, over []string
	for _, v := range statusClasses["server"] {
		if !re.MatchString(v) {
			miss = append(miss, v)
		}
	}
	for _, v := range append(append([]string{}, statusClasses["client"]...), observed...) {
		if v[0] == '4' && re.MatchString(v) {
			over = append(over, v)
		}
	}
	switch {
	case len(miss) > 0:
		return fmt.Sprintf("the errors pattern %q does not count server errors (%s)", errors, strings.Join(miss, ", "))
	case len(over) > 0:
		return fmt.Sprintf("the errors pattern %q counts client errors (%s) as errors; separate server errors (5xx) from client errors", errors, strings.Join(over[:1], ", "))
	}
	return ""
}

// finalize adds the probe, pointer and map results and builds the journey
// table from every item's results.
func finalize(cfg Config, r *Report) {
	judgeProbesAndPointers(cfg, r)
	applyReasons(cfg, r)
	for _, op := range cfg.Map.Operations {
		j := Journey{Operation: op.ID, Given: op.Journey}
		j.Mapping = cellOf(*r, op.ID, ItemJourneys, ItemObserved)
		j.Errors = cellOf(*r, op.ID, ItemErrorsSeparable)
		j.Latency = cellOf(*r, op.ID, ItemLatency)
		j.Probe = cellOf(*r, op.ID, ItemProbe)
		j.Detection = detectionCell(cfg, *r, op)
		j.Map = mapCell(cfg, *r, op)
		j.Traces = tracesCell(cfg.Map, op)
		r.Journeys = append(r.Journeys, j)
	}
}

// cellOf is the worst result of the items for the operation.
func cellOf(r Report, op string, items ...string) Cell {
	best := Cell{Status: Note, Reason: "not judged"}
	judged := false
	for _, x := range r.Results {
		if x.Subject != op {
			continue
		}
		for _, item := range items {
			if x.Item != item {
				continue
			}
			rank := map[string]int{Green: 1, Note: 2, Red: 3}
			if !judged || rank[x.Status] > rank[best.Status] {
				best = Cell{Status: x.Status, Reason: x.Item + ": " + clip(x.Detail, 160)}
			}
			judged = true
		}
	}
	return best
}

func detectionCell(cfg Config, r Report, op appmap.Operation) Cell {
	for _, x := range r.Results {
		if x.Item == ItemQuiet && x.Status == Red && namesOperationOrScenario(cfg.Map, op, x.Subject+" "+x.Detail) {
			return Cell{Status: Red, Reason: "raised while healthy: " + clip(x.Detail, 160)}
		}
	}
	if cfg.Break == nil {
		return Cell{Status: Note, NotRun: true, Reason: "not run: add --break (full check) to show a break names this operation"}
	}
	return cellOf(r, op.ID, ItemBreak)
}

func namesOperationOrScenario(m appmap.Map, op appmap.Operation, text string) bool {
	return namesOperation(strings.ToLower(text), m, op)
}

func mapCell(cfg Config, r Report, op appmap.Operation) Cell {
	if c := cellOf(r, op.ID, ItemPointers); c.Status == Red {
		return c
	}
	acc := r.accuracy[op.ID]
	switch {
	case acc != nil && len(acc.missing) > 0:
		return Cell{Status: Red, Reason: fmt.Sprintf("missing edges: failed while %s was broken but the path does not list it", strings.Join(acc.missing, ", "))}
	case cfg.Break == nil:
		return Cell{Status: Note, NotRun: true, Reason: "pointers present; path not confirmed (no --break)"}
	case acc != nil && len(acc.spurious) > 0:
		return Cell{Status: Note, Reason: fmt.Sprintf("path confirmed for %s; spurious or undetected: %s", none(acc.confirmed), strings.Join(acc.spurious, ", "))}
	case acc == nil || len(acc.confirmed) == 0:
		return Cell{Status: Note, Reason: "pointers present; no component on the path could be broken to confirm it"}
	}
	return Cell{Status: Green, Reason: "path confirmed by breaking " + strings.Join(acc.confirmed, ", ")}
}

func tracesCell(m appmap.Map, op appmap.Operation) Cell {
	byName := map[string]appmap.Component{}
	for _, c := range m.Components {
		byName[c.Name] = c
	}
	var missing []string
	for _, n := range op.Path {
		if o := byName[n].Observe; o == nil || o.TraceService == "" {
			missing = append(missing, n)
		}
	}
	if len(missing) == 0 && len(op.Path) > 0 {
		return Cell{Status: Green, Reason: "every component on the path names its trace service"}
	}
	return Cell{Status: Note, Reason: "no trace_service for " + strings.Join(missing, ", ") + " (fine where the runtime cannot trace)"}
}

// journeyTable renders the checklist, one row per operation.
func journeyTable(js []Journey) string {
	var b strings.Builder
	b.WriteString("journeys (a mapping, b errors, c latency, d probe, e detection, f map, g traces):\n")
	label := func(j Journey) string {
		if j.Given != "" {
			return j.Operation + " (" + j.Given + ")"
		}
		return j.Operation
	}
	width := len("journey")
	for _, j := range js {
		width = max(width, len(label(j)))
	}
	mark := func(c Cell) string {
		switch {
		case c.Status == Red:
			return "RED"
		case c.NotRun:
			return "n/r"
		case c.Status == Note:
			return "note"
		}
		return "ok"
	}
	fmt.Fprintf(&b, "  %-*s  %-4s %-4s %-4s %-4s %-4s %-4s %-4s\n", width, "journey", "a", "b", "c", "d", "e", "f", "g")
	for _, j := range js {
		fmt.Fprintf(&b, "  %-*s  %-4s %-4s %-4s %-4s %-4s %-4s %-4s\n", width, label(j), mark(j.Mapping), mark(j.Errors), mark(j.Latency), mark(j.Probe), mark(j.Detection), mark(j.Map), mark(j.Traces))
	}
	for _, j := range js {
		for i, c := range []Cell{j.Mapping, j.Errors, j.Latency, j.Probe, j.Detection, j.Map, j.Traces} {
			if c.Status == Green || c.Reason == "" {
				continue
			}
			fmt.Fprintf(&b, "  %s %s (%s): %s\n", j.Operation, "abcdefg"[i:i+1], mark(c), c.Reason)
		}
	}
	return b.String()
}

func pointersDetail(missing []string) string {
	return fmt.Sprintf("components on the path without observe metrics and logs pointers: %s; give each component a line such as "+
		"`observe: {metrics: \"<where its metrics are>\", logs: \"container <name>: <what it logs>\"}`, or add under `reasons:` "+
		"`- {item: %s, subject: <operation>, reason: <why>}` (%s)", strings.Join(missing, ", "), ItemPointers, mapDoc)
}

// excusable are the map items a recorded reason may turn into a note
// wherever they are raised. Items about what the live detectors saw (an
// operation the metric does not count, its latency) stay red whatever the map says.
var excusable = map[string]bool{ItemPointers: true, ItemJourneys: true}

// applyReasons turns each red result of an excusable item the map gives a
// reason for (an item and subject under `reasons:`) into a note carrying the
// reason, so a recorded reason takes effect whichever check raised the item.
func applyReasons(cfg Config, r *Report) {
	reasons := map[[2]string]string{}
	for _, reason := range cfg.Map.Reasons {
		reasons[[2]string{reason.Item, reason.Subject}] = reason.Reason
	}
	for i, result := range r.Results {
		if why, ok := reasons[[2]string{result.Item, result.Subject}]; ok && result.Status == Red && excusable[result.Item] {
			r.Results[i].Status, r.Results[i].Detail = Note, result.Detail+" (reason: "+why+")"
		}
	}
}
