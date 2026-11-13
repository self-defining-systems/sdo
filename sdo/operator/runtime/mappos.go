package runtime

import (
	"errors"
	"fmt"
	"sort"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/program"
)

// mapGraph is the accepted application map with its possible edges (calls only
// a Secret reference suggests, never confirmed). It is pure data: every
// question about what affects what, for `sdo map` and for any view that places
// evidence on a component, goes through it so the answers agree.
type mapGraph struct {
	m        appmap.Map
	possible [][2]string
	comps    map[string]appmap.Component
}

// newMapGraph parses the application map region of an architecture document.
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

// testPath is the components a user test (probe scenario) exercises: the union
// of the paths of the operations its probe covers, sorted. It is empty when the
// map names no probe for the scenario, so a caller can fall back.
func (g mapGraph) testPath(scenario string) []string {
	set := map[string]bool{}
	for _, p := range g.m.Probes {
		if p.Scenario != scenario {
			continue
		}
		for _, id := range p.Operations {
			if op, ok := g.operation(id); ok {
				for _, name := range op.Path {
					set[name] = true
				}
			}
		}
	}
	return sortedKeys(set)
}

// operationsThrough are the IDs of the operations whose path includes the component, in map order.
func (g mapGraph) operationsThrough(component string) []string {
	var out []string
	for _, op := range g.m.Operations {
		for _, n := range op.Path {
			if n == component {
				out = append(out, op.ID)
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

// mapHop is one component reached by a walk over the edges.
type mapHop struct {
	Name     string
	Via      string // the component the walk came from; the start for depth 1
	Depth    int
	Possible bool // reached over a possible edge only
}

// reach walks the edges up to hops steps from start. dependsOn follows an
// edge's direction (what start depends on, the callee side); otherwise it
// follows it backwards (what depends on start). Mapped edges win over possible
// ones. Hops come breadth first, each depth sorted by the walk's own order.
func (g mapGraph) reach(start string, dependsOn bool, hops int) []mapHop {
	seen := map[string]bool{start: true}
	frontier := []string{start}
	var out []mapHop
	for depth := 1; depth <= hops; depth++ {
		var next []string
		visit := func(cur string, pair [2]string, possible bool) {
			from, to := pair[0], pair[1]
			if !dependsOn {
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
			out = append(out, mapHop{Name: to, Via: cur, Depth: depth, Possible: possible})
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

// Map-position classes of a component relative to the failing and passing
// user tests' paths.
const (
	// PositionFailingOnly is on a failing path and on no passing one.
	PositionFailingOnly = "failing-only"
	// PositionShared is on a failing path and on a passing one.
	PositionShared = "shared"
	// PositionDependency is on no failing path but upstream of one: a failing
	// component depends on it over the edges.
	PositionDependency = "dependency"
	// PositionOffPath is none of the above.
	PositionOffPath = "off-path"
	// PositionUnplaced is not a component of the map.
	PositionUnplaced = "unplaced"
)

// MapPosition is where a component stands relative to the failing and passing
// tests: its class, the tests through it, and how many edges separate it from
// the nearest failing-path component (0 on a failing path, -1 when none is
// reachable).
type MapPosition struct {
	Class      string
	Failing    []string
	Passing    []string
	FailingHop int
}

// position places component against the tests' paths, each keyed by test ID.
// Failing and passing tests are the sets `sdo detect` judges; a caller builds
// the paths with testPath, falling back to its own when the map names none.
func (g mapGraph) position(component string, failing, passing map[string][]string) MapPosition {
	if _, ok := g.comps[component]; !ok {
		return MapPosition{Class: PositionUnplaced, FailingHop: -1}
	}
	through := func(paths map[string][]string) []string {
		var ids []string
		for id, path := range paths {
			for _, n := range path {
				if n == component {
					ids = append(ids, id)
					break
				}
			}
		}
		sort.Strings(ids)
		return ids
	}
	p := MapPosition{Failing: through(failing), Passing: through(passing), FailingHop: -1}
	switch {
	case len(p.Failing) > 0 && len(p.Passing) > 0:
		p.Class, p.FailingHop = PositionShared, 0
	case len(p.Failing) > 0:
		p.Class, p.FailingHop = PositionFailingOnly, 0
	default:
		p.Class = PositionOffPath
		onFailing := map[string]bool{}
		for _, path := range failing {
			for _, n := range path {
				onFailing[n] = true
			}
		}
		for _, hop := range g.reach(component, false, len(g.comps)) {
			// What depends on the component: a failing-path caller makes it a dependency.
			if onFailing[hop.Name] {
				p.Class, p.FailingHop = PositionDependency, hop.Depth
				break
			}
		}
	}
	return p
}

// graphFromProgramMap is the graph a read view without the architecture
// document can offer: the components and edges of its AppMap, no operations,
// probes or possible edges.
func graphFromProgramMap(m program.AppMap) mapGraph {
	g := mapGraph{comps: map[string]appmap.Component{}}
	for _, c := range m.Components {
		component := appmap.Component{Name: c.Name, Workload: c.Workload, Services: c.Services, HoldsData: c.HoldsData}
		if c.Data != nil {
			component.Data = &appmap.Data{Engine: c.Data.Engine, Role: c.Data.Role, DerivedFrom: c.Data.DerivedFrom}
		}
		g.m.Components = append(g.m.Components, component)
		g.comps[c.Name] = component
	}
	for _, e := range m.Edges {
		g.m.Edges = append(g.m.Edges, appmap.Edge{From: e.From, To: e.To})
	}
	return g
}

// programMap is the program-level view of the graph: one document, two shapes.
func (g mapGraph) programMap() program.AppMap {
	var m program.AppMap
	for _, c := range g.m.Components {
		component := program.AppComponent{Name: c.Name, Workload: c.Workload, Services: c.Services, HoldsData: c.HoldsData}
		if c.Data != nil {
			component.Data = &program.DataRole{Engine: c.Data.Engine, Role: c.Data.Role, DerivedFrom: c.Data.DerivedFrom}
		}
		m.Components = append(m.Components, component)
	}
	for _, e := range g.m.Edges {
		m.Edges = append(m.Edges, program.AppEdge{From: e.From, To: e.To})
	}
	return m
}
