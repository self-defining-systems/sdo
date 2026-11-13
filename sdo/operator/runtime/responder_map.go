package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/program"
)

const (
	mapListCap  = 30 // components and operations listed by `sdo map`
	mapNearCap  = 12 // neighbours listed per direction by `sdo map near`
	mapNamesCap = 40 // names listed in an unknown-name error
)

// architectureReader is a read view that can hand over the accepted
// architecture document the application map lives in.
type architectureReader interface {
	ArchitectureText() (string, error)
}

// mapGraph is the accepted application map with its possible edges: calls only
// a Secret reference suggests, never confirmed.
type mapGraph struct {
	m        appmap.Map
	possible [][2]string
	comps    map[string]appmap.Component
}

func newMapGraph(doc string) (mapGraph, error) {
	region, found, err := appmap.Find(doc)
	if err != nil {
		return mapGraph{}, err
	}
	if !found {
		return mapGraph{}, errors.New("no application map: the accepted architecture has no application-map region")
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil {
		return mapGraph{}, fmt.Errorf("application map unreadable: %w", err)
	}
	g := mapGraph{m: m, possible: appmap.ParseInventory(doc).Possible, comps: map[string]appmap.Component{}}
	for _, c := range m.Components {
		g.comps[c.Name] = c
	}
	return g, nil
}

func (g mapGraph) hasEdge(from, to string) bool {
	for _, e := range g.m.Edges {
		if e.From == from && e.To == to {
			return true
		}
	}
	return false
}

func (g mapGraph) operation(id string) (appmap.Operation, bool) {
	for _, op := range g.m.Operations {
		if op.ID == id {
			return op, true
		}
	}
	return appmap.Operation{}, false
}

// probesOf are the probes whose steps exercise the operation.
func (g mapGraph) probesOf(id string) []appmap.Probe {
	var out []appmap.Probe
	for _, p := range g.m.Probes {
		for _, op := range p.Operations {
			if op == id {
				out = append(out, p)
				break
			}
		}
	}
	return out
}

// edgesWithin are the mapped edges whose two ends are both in the set, in map order.
func (g mapGraph) edgesWithin(set map[string]bool) []string {
	var out []string
	for _, e := range g.m.Edges {
		if set[e.From] && set[e.To] {
			out = append(out, e.From+"->"+e.To)
		}
	}
	return out
}

func (g mapGraph) label(name string) string {
	c, ok := g.comps[name]
	if !ok || (!c.HoldsData && c.Data == nil) {
		return name
	}
	return name + " [data" + dataSuffix(c) + "]"
}

func dataSuffix(c appmap.Component) string {
	if c.Data == nil {
		return ""
	}
	text := ": " + c.Data.Engine + " " + c.Data.Role
	if len(c.Data.DerivedFrom) > 0 {
		text += " from " + strings.Join(c.Data.DerivedFrom, ",")
	}
	return text
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unknownName(kind, name string, valid []string) string {
	sort.Strings(valid)
	more := ""
	if len(valid) > mapNamesCap {
		more = fmt.Sprintf(", +%d more", len(valid)-mapNamesCap)
		valid = valid[:mapNamesCap]
	}
	return fmt.Sprintf("unknown %s %q; %ss: %s%s", kind, name, kind, strings.Join(valid, ", "), more)
}

// applicationMap answers the read-only `sdo map` questions from the accepted
// application map and the live user-test verdicts. It writes nothing.
func (l *ResponderLoop) applicationMap(ctx context.Context, request ToolRequest, response *ToolResponse) {
	switch request.MapView {
	case "", "path", "diff", "near":
	default:
		response.Error = fmt.Sprintf("unknown map view %q; views: path, diff, near", request.MapView)
		return
	}
	if (request.MapView == "path" || request.MapView == "near") && request.Name == "" {
		response.Error = fmt.Sprintf("sdo map %s needs a name", request.MapView)
		return
	}
	if l.Provider == nil {
		response.Error = "fresh snapshot provider is unavailable"
		return
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	readView := program.ReadFrom(snapshot)
	if l.ReadView != nil {
		readView = l.ReadView(snapshot)
	}
	reader, ok := readView.(architectureReader)
	if !ok {
		response.Error = "no application map: the accepted architecture is unavailable"
		return
	}
	doc, err := reader.ArchitectureText()
	if err != nil {
		response.Error = "no application map: " + err.Error()
		return
	}
	g, err := newMapGraph(doc)
	if err != nil {
		response.Error = err.Error()
		return
	}
	switch request.MapView {
	case "":
		response.Text = g.renderOverview()
	case "path":
		op, found := g.operation(request.Name)
		if !found {
			ids := make([]string, len(g.m.Operations))
			for i, o := range g.m.Operations {
				ids[i] = o.ID
			}
			response.Error = unknownName("operation", request.Name, ids)
			return
		}
		response.Text = g.renderPath(op, l.userTestVerdicts(readView))
	case "near":
		if _, found := g.comps[request.Name]; !found {
			names := make([]string, 0, len(g.comps))
			for n := range g.comps {
				names = append(names, n)
			}
			response.Error = unknownName("component", request.Name, names)
			return
		}
		response.Text = g.renderNear(request.Name)
	case "diff":
		response.Text = g.renderDiff(l.userTestJudgement(readView))
	}
	response.Summary = strings.TrimSpace("sdo map " + request.MapView)
}

// userJudgement is the failing and passing user tests `sdo detect` names, and
// the components the detect summary puts on each failing test's path.
type userJudgement struct {
	known       bool
	failing     []string
	failingPath map[string][]string
	passing     []string
	unevaluated int
}

// userTestJudgement reads the same sets the first message and `sdo detect`
// use: buildSuspectSet over the fresh snapshot.
func (l *ResponderLoop) userTestJudgement(view program.ReadView) userJudgement {
	reads, ok := any(NewCycleReadView(view)).(detectReads)
	if !ok {
		return userJudgement{}
	}
	in := l.pageInput(reads, l.Request.Findings, l.clock().UTC())
	set := buildSuspectSet(detectInput{Request: in.Request, Findings: in.Findings, View: in.View, Now: in.Now})
	out := userJudgement{known: true, passing: append([]string(nil), set.passing...), failingPath: map[string][]string{}, unevaluated: len(set.unevaluated)}
	for _, test := range set.failing {
		out.failing = append(out.failing, test.id)
		out.failingPath[test.id] = test.path
	}
	return out
}

// userTestVerdicts maps scenario ID to FAIL or PASS for the judged tests.
func (l *ResponderLoop) userTestVerdicts(view program.ReadView) map[string]string {
	j := l.userTestJudgement(view)
	out := map[string]string{}
	for _, id := range j.passing {
		out[id] = "PASS"
	}
	for _, id := range j.failing {
		out[id] = "FAIL"
	}
	return out
}

func opKind(op appmap.Operation) string {
	if op.Writes {
		return "write"
	}
	return "read"
}

func (g mapGraph) renderOverview() string {
	var out strings.Builder
	fmt.Fprintf(&out, "components (%d):\n", len(g.m.Components))
	for i, c := range g.m.Components {
		if i == mapListCap {
			fmt.Fprintf(&out, "  +%d more\n", len(g.m.Components)-mapListCap)
			break
		}
		data := "no data"
		if c.HoldsData || c.Data != nil {
			data = "holds data" + dataSuffix(c)
		}
		fmt.Fprintf(&out, "  %s  %s  services: %s  %s\n", c.Name, c.Workload, strings.Join(c.Services, ","), data)
	}
	fmt.Fprintf(&out, "operations (%d):\n", len(g.m.Operations))
	for i, op := range g.m.Operations {
		if i == mapListCap {
			fmt.Fprintf(&out, "  +%d more\n", len(g.m.Operations)-mapListCap)
			break
		}
		fmt.Fprintf(&out, "  %s  %s  %d components\n", op.ID, opKind(op), len(op.Path))
	}
	fmt.Fprintf(&out, "edges: %d", len(g.m.Edges))
	if n := g.unadoptedPossible(); n > 0 {
		fmt.Fprintf(&out, " (+%d possible, unconfirmed)", n)
	}
	out.WriteString("\nsdo map path OPERATION | diff | near COMPONENT\n")
	return out.String()
}

func (g mapGraph) unadoptedPossible() int {
	n := 0
	for _, e := range g.possible {
		if !g.hasEdge(e[0], e[1]) {
			n++
		}
	}
	return n
}

func (g mapGraph) renderPath(op appmap.Operation, verdicts map[string]string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "operation %s (%s) enters %s:%d %s %s\n", op.ID, opKind(op), op.Entry.Service, op.Entry.Port, op.Entry.Method, op.Entry.Path)
	labels := make([]string, len(op.Path))
	in := map[string]bool{}
	for i, name := range op.Path {
		labels[i] = g.label(name)
		in[name] = true
	}
	fmt.Fprintf(&out, "path: %s\n", strings.Join(labels, " -> "))
	if edges := g.edgesWithin(in); len(edges) > 0 {
		fmt.Fprintf(&out, "edges: %s\n", strings.Join(edges, ", "))
	}
	probes := g.probesOf(op.ID)
	if len(probes) == 0 {
		out.WriteString("probes: none cover this operation\n")
		return out.String()
	}
	for _, p := range probes {
		status := verdicts[p.Scenario]
		if status == "" {
			status = "unknown"
		}
		fmt.Fprintf(&out, "probe %s (%s): %s\n", p.Scenario, p.Kind, status)
	}
	return out.String()
}

// testComponents is the components a user test exercises: the operation paths
// its probe covers, else the services the detect summary resolves.
func (g mapGraph) testComponents(id string, fallback []string) map[string]bool {
	set := map[string]bool{}
	for _, p := range g.m.Probes {
		if p.Scenario != id {
			continue
		}
		for _, opID := range p.Operations {
			if op, ok := g.operation(opID); ok {
				for _, name := range op.Path {
					set[name] = true
				}
			}
		}
	}
	if len(set) == 0 {
		for _, name := range fallback {
			set[name] = true
		}
	}
	return set
}

func (g mapGraph) renderDiff(j userJudgement) string {
	if !j.known {
		return "user tests are unavailable: the snapshot cannot be judged\n"
	}
	if len(j.failing) == 0 {
		return fmt.Sprintf("no failing user tests (%d passing); nothing to diff\n", len(j.passing))
	}
	failComps, passComps := map[string]bool{}, map[string]bool{}
	for _, id := range j.failing {
		for n := range g.testComponents(id, j.failingPath[id]) {
			failComps[n] = true
		}
	}
	for _, id := range j.passing {
		for n := range g.testComponents(id, nil) {
			passComps[n] = true
		}
	}
	var out strings.Builder
	fmt.Fprintf(&out, "failing: %s\npassing: %s\n", strings.Join(j.failing, ", "), joinOrNone(j.passing))
	if j.unevaluated > 0 {
		fmt.Fprintf(&out, "not yet judged: %d tests\n", j.unevaluated)
	}
	only, both := map[string]bool{}, map[string]bool{}
	for n := range failComps {
		if passComps[n] {
			both[n] = true
		} else {
			only[n] = true
		}
	}
	passEdgeSet := map[string]bool{}
	for _, e := range g.edgesWithin(passComps) {
		passEdgeSet[e] = true
	}
	var onlyEdges, bothEdges []string
	for _, e := range g.edgesWithin(failComps) {
		if passEdgeSet[e] {
			bothEdges = append(bothEdges, e)
		} else {
			onlyEdges = append(onlyEdges, e)
		}
	}
	out.WriteString("only on failing paths:\n")
	g.writeSet(&out, only, onlyEdges)
	out.WriteString("shared with passing paths:\n")
	g.writeSet(&out, both, bothEdges)
	return out.String()
}

func (g mapGraph) writeSet(out *strings.Builder, comps map[string]bool, edges []string) {
	names := sortedKeys(comps)
	labels := make([]string, len(names))
	for i, n := range names {
		labels[i] = g.label(n)
	}
	fmt.Fprintf(out, "  components: %s\n  edges: %s\n", joinOrNone(labels), joinOrNone(edges))
}

func joinOrNone(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return strings.Join(items, ", ")
}

// neighbours walks the edges up to two hops from start. depends follows
// From->To (what start depends on); otherwise To->From (what depends on
// start). Possible edges are marked.
func (g mapGraph) neighbours(start string, depends bool) []string {
	seen := map[string]bool{start: true}
	frontier := []string{start}
	var out []string
	for depth := 1; depth <= 2; depth++ {
		var next []string
		visit := func(cur string, pair [2]string, possible bool) {
			from, to := pair[0], pair[1]
			if !depends {
				from, to = to, from
			}
			if from != cur || seen[to] {
				return
			}
			if _, known := g.comps[to]; !known {
				return
			}
			seen[to] = true
			next = append(next, to)
			text := g.label(to)
			if depth == 2 {
				text += " (via " + cur + ")"
			}
			if possible {
				text += " (possible, unconfirmed)"
			}
			out = append(out, text)
		}
		for _, cur := range frontier {
			for _, e := range g.m.Edges {
				visit(cur, [2]string{e.From, e.To}, false)
			}
			for _, e := range g.possible {
				if !g.hasEdge(e[0], e[1]) {
					visit(cur, e, true)
				}
			}
		}
		sort.Strings(next)
		frontier = next
	}
	return out
}

func (g mapGraph) renderNear(name string) string {
	var out strings.Builder
	c := g.comps[name]
	fmt.Fprintf(&out, "component %s  %s  services: %s\n", g.label(name), c.Workload, strings.Join(c.Services, ","))
	write := func(title string, items []string) {
		more := ""
		if len(items) > mapNearCap {
			more = fmt.Sprintf(", +%d more", len(items)-mapNearCap)
			items = items[:mapNearCap]
		}
		fmt.Fprintf(&out, "%s: %s%s\n", title, joinOrNone(items), more)
	}
	write("upstream (it depends on, 2 hops)", g.neighbours(name, true))
	write("downstream (depend on it, 2 hops)", g.neighbours(name, false))
	var ops []string
	for _, op := range g.m.Operations {
		for _, n := range op.Path {
			if n == name {
				ops = append(ops, op.ID)
				break
			}
		}
	}
	write("operations through it", ops)
	return out.String()
}
