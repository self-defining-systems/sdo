package runtime

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/telemetry"
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
	case "", "path", "near":
	default:
		response.Error = fmt.Sprintf("unknown map view %q; views: path, near", request.MapView)
		return
	}
	if request.MapView != "" && request.Name == "" {
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
		response.Text = g.renderPath(op, l.userTestVerdicts(readView), l.observability(ctx, g, snapshot))
	case "near":
		if _, found := g.comps[request.Name]; !found {
			names := make([]string, 0, len(g.comps))
			for n := range g.comps {
				names = append(names, n)
			}
			response.Error = unknownName("component", request.Name, names)
			return
		}
		response.Text = g.renderNear(request.Name, l.observability(ctx, g, snapshot))
	}
	response.Summary = strings.TrimSpace("sdo map " + request.MapView)
}

// userTestVerdicts maps scenario ID to FAIL or PASS for the tests `sdo detect`
// judges: buildSuspectSet over the fresh snapshot.
func (l *ResponderLoop) userTestVerdicts(view program.ReadView) map[string]string {
	out := map[string]string{}
	reads, ok := any(NewCycleReadView(view)).(detectReads)
	if !ok {
		return out
	}
	in := l.pageInput(reads, l.Request.Findings, l.clock().UTC())
	set := buildSuspectSet(detectInput{Request: in.Request, Findings: in.Findings, View: in.View, Now: in.Now})
	for _, id := range set.passing {
		out[id] = "PASS"
	}
	for _, test := range set.failing {
		out[test.id] = "FAIL"
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
	out.WriteString("\nsdo map path OPERATION | near COMPONENT\n")
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

func (g mapGraph) renderPath(op appmap.Operation, verdicts map[string]string, observe func(string) string) string {
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
	for _, name := range op.Path {
		fmt.Fprintf(&out, "observe %s: %s\n", name, observe(name))
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

func (g mapGraph) renderNear(name string, observe func(string) string) string {
	var out strings.Builder
	c := g.comps[name]
	fmt.Fprintf(&out, "component %s  %s  services: %s\n", g.label(name), c.Workload, strings.Join(c.Services, ","))
	write := func(title string, items []string) {
		more := ""
		if len(items) > mapNearCap {
			more = fmt.Sprintf(", +%d more", len(items)-mapNearCap)
			items = items[:mapNearCap]
		}
		if len(items) == 0 {
			items = []string{"none"}
		}
		fmt.Fprintf(&out, "%s: %s%s\n", title, strings.Join(items, ", "), more)
	}
	hopTexts := func(hops []mapHop) []string {
		var texts []string
		for _, hop := range hops {
			text := g.label(hop.Name)
			if hop.Depth > 1 {
				text += " (via " + hop.Via + ")"
			}
			if hop.Possible {
				text += " (possible, unconfirmed)"
			}
			texts = append(texts, text)
		}
		return texts
	}
	write("upstream (it depends on, 2 hops)", hopTexts(g.reach(name, true, 2)))
	write("downstream (depend on it, 2 hops)", hopTexts(g.reach(name, false, 2)))
	write("operations through it", g.operationsThrough(name))
	fmt.Fprintf(&out, "observe: %s\n", observe(name))
	return out.String()
}

// observability is, per component, where to look: the request metric its
// services expose (from the map's entries), its pods for `sdo logs`, and its
// trace service name when a trace store is bound. They are the arguments
// `sdo metrics`, `sdo logs` and `sdo traces` take, found from the map's
// components and nothing finer.
func (l *ResponderLoop) observability(ctx context.Context, g mapGraph, snapshot sdk.DetectionContext) func(string) string {
	var sources []telemetry.Source
	if l.Telemetry != nil {
		sources = l.Telemetry().Sources
	}
	traced := slices.ContainsFunc(sources, func(s telemetry.Source) bool { return s.Serves(telemetry.KindTraces) })
	return func(name string) string {
		component, ok := g.comps[name]
		if !ok {
			return "none"
		}
		var parts []string
		for _, entry := range g.m.Entries {
			if !slices.Contains(component.Services, entry.Service) {
				continue
			}
			selector := ""
			if len(entry.Selector) > 0 {
				keys := make([]string, 0, len(entry.Selector))
				for key := range entry.Selector {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				pairs := make([]string, len(keys))
				for i, key := range keys {
					pairs[i] = fmt.Sprintf("%s=%q", key, entry.Selector[key])
				}
				selector = "{" + strings.Join(pairs, ",") + "}"
			}
			labels := "route=" + entry.Labels.Route
			if entry.Labels.Status != "" {
				labels += ", status=" + entry.Labels.Status
			}
			parts = append(parts, fmt.Sprintf("metrics %s%s (labels %s)", entry.Metric, selector, labels))
		}
		logs := "logs: sdo logs " + name
		if pods, err := componentPods(l, snapshot, name); err == nil {
			names := make([]string, 0, len(pods))
			for _, pod := range pods {
				if len(names) < mapNearCap {
					names = append(names, pod.Name)
				}
			}
			logs += " (pods " + strings.Join(names, ",") + ")"
		}
		parts = append(parts, logs)
		if traced && len(component.Services) > 0 {
			parts = append(parts, "traces: service "+component.Services[0])
		}
		return strings.Join(parts, "; ")
	}
}
