package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
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
	g, readView, snapshot, err := l.loadMap(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	switch request.MapView {
	case "":
		response.Text = g.renderOverview()
		if request.Structured {
			response.Data = marshalMapData(g.overviewData(l.pointers(ctx, g, snapshot)))
		}
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
		if request.Structured {
			response.Data = marshalMapData(g.pathData(op, l.userTestVerdicts(readView), l.pointers(ctx, g, snapshot)))
		}
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
		if request.Structured {
			response.Data = marshalMapData(g.nearData(request.Name, l.pointers(ctx, g, snapshot)))
		}
	}
	response.Summary = strings.TrimSpace("sdo map " + request.MapView)
}

// loadMap reads the accepted application map and the fresh snapshot it is
// looked at through.
func (l *ResponderLoop) loadMap(ctx context.Context) (mapGraph, program.ReadView, sdk.DetectionContext, error) {
	if l.Provider == nil {
		return mapGraph{}, nil, nil, errors.New("fresh snapshot provider is unavailable")
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		return mapGraph{}, nil, nil, err
	}
	readView := program.ReadFrom(snapshot)
	if l.ReadView != nil {
		readView = l.ReadView(snapshot)
	}
	reader, ok := readView.(architectureReader)
	if !ok {
		return mapGraph{}, nil, nil, errors.New("no application map: the accepted architecture is unavailable")
	}
	doc, err := reader.ArchitectureText()
	if err != nil {
		return mapGraph{}, nil, nil, errors.New("no application map: " + err.Error())
	}
	g, err := newMapGraph(doc)
	if err != nil {
		return mapGraph{}, nil, nil, err
	}
	return g, readView, snapshot, nil
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

// mapPointers is, for one component, where to look: the request metrics its
// services expose, its pods for `sdo logs` and its service name in the trace
// store. They are the arguments `sdo metrics`, `sdo logs` and `sdo traces`
// take, found from the map's components and nothing finer.
type mapPointers struct {
	Metrics []metricPointer `json:"metrics,omitempty"`
	Logs    logPointer      `json:"logs"`
	// TracesService is the trace store's service name for the component, when
	// traces exist and name it.
	TracesService string `json:"traces_service,omitempty"`
}

type metricPointer struct {
	Metric   string            `json:"metric"`
	Selector map[string]string `json:"selector,omitempty"`
	Route    string            `json:"route_label"`
	Status   string            `json:"status_label,omitempty"`
}

type logPointer struct {
	Component string   `json:"component"`
	Pods      []string `json:"pods,omitempty"`
}

func (p metricPointer) text() string {
	selector := ""
	if len(p.Selector) > 0 {
		keys := make([]string, 0, len(p.Selector))
		for key := range p.Selector {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		pairs := make([]string, len(keys))
		for i, key := range keys {
			pairs[i] = fmt.Sprintf("%s=%q", key, p.Selector[key])
		}
		selector = "{" + strings.Join(pairs, ",") + "}"
	}
	labels := "route=" + p.Route
	if p.Status != "" {
		labels += ", status=" + p.Status
	}
	return fmt.Sprintf("metrics %s%s (labels %s)", p.Metric, selector, labels)
}

func (p mapPointers) text() string {
	var parts []string
	for _, metric := range p.Metrics {
		parts = append(parts, metric.text())
	}
	logs := "logs: sdo logs " + p.Logs.Component
	if len(p.Logs.Pods) > 0 {
		logs += " (pods " + strings.Join(p.Logs.Pods, ",") + ")"
	}
	parts = append(parts, logs)
	if p.TracesService != "" {
		parts = append(parts, "traces: service "+p.TracesService)
	}
	return strings.Join(parts, "; ")
}

// pointers returns the observation pointers of each component. The trace
// service is the store's own name for the component: the first of the
// component's name, workload name and Service names the store lists.
func (l *ResponderLoop) pointers(ctx context.Context, g mapGraph, snapshot sdk.DetectionContext) func(string) mapPointers {
	var traced []string
	if l.Telemetry != nil {
		if store := l.Telemetry().Traces; store != nil {
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			traced, _ = store.Services(readCtx)
		}
	}
	return func(name string) mapPointers {
		component, ok := g.comps[name]
		if !ok {
			return mapPointers{Logs: logPointer{Component: name}}
		}
		var out mapPointers
		for _, entry := range g.m.Entries {
			if slices.Contains(component.Services, entry.Service) {
				out.Metrics = append(out.Metrics, metricPointer{Metric: entry.Metric, Selector: entry.Selector, Route: entry.Labels.Route, Status: entry.Labels.Status})
			}
		}
		out.Logs.Component = name
		if pods, err := componentPods(l, snapshot, name); err == nil {
			for _, pod := range pods {
				if len(out.Logs.Pods) < mapNearCap {
					out.Logs.Pods = append(out.Logs.Pods, pod.Name)
				}
			}
		}
		_, short, _ := strings.Cut(component.Workload, "/")
		candidates := append([]string{name, short}, component.Services...)
		for _, candidate := range candidates {
			if i := slices.IndexFunc(traced, func(s string) bool { return strings.EqualFold(s, candidate) }); i >= 0 {
				out.TracesService = traced[i]
				break
			}
		}
		return out
	}
}

// observability is the text form of pointers.
func (l *ResponderLoop) observability(ctx context.Context, g mapGraph, snapshot sdk.DetectionContext) func(string) string {
	point := l.pointers(ctx, g, snapshot)
	return func(name string) string {
		if _, ok := g.comps[name]; !ok {
			return "none"
		}
		return point(name).text()
	}
}
