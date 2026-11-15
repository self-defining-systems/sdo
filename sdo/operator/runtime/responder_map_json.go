package runtime

import (
	"encoding/json"

	"sdo.dev/operator/memory/appmap"
)

// The structured answers of `sdo map --json`: the map's own records as JSON,
// with the observation pointers per component. Prose never rides in them.

type mapComponentJSON struct {
	Name      string   `json:"name"`
	Workload  string   `json:"workload"`
	Services  []string `json:"services"`
	HoldsData bool     `json:"holds_data"`
	Data      *struct {
		Engine      string   `json:"engine"`
		Role        string   `json:"role"`
		DerivedFrom []string `json:"derived_from,omitempty"`
	} `json:"data,omitempty"`
}

type mapEdgeJSON struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Evidence []string `json:"evidence,omitempty"`
	Possible bool     `json:"possible,omitempty"`
}

type mapEntryJSON struct {
	Service string `json:"service"`
	Port    int    `json:"port"`
	Method  string `json:"method"`
	Path    string `json:"path"`
}

type mapOperationJSON struct {
	ID    string       `json:"id"`
	Kind  string       `json:"kind"`
	Entry mapEntryJSON `json:"entry"`
	Path  []string     `json:"path"`
	Route string       `json:"route,omitempty"`
}

type mapProbeJSON struct {
	Scenario string `json:"scenario"`
	Kind     string `json:"kind"`
	Status   string `json:"status"`
}

type mapHopJSON struct {
	Component string `json:"component"`
	Via       string `json:"via,omitempty"`
	Depth     int    `json:"depth"`
	Possible  bool   `json:"possible,omitempty"`
}

type mapData struct {
	View              string                 `json:"view"`
	Components        []mapComponentJSON     `json:"components,omitempty"`
	Edges             []mapEdgeJSON          `json:"edges,omitempty"`
	Operations        []mapOperationJSON     `json:"operations,omitempty"`
	Operation         *mapOperationJSON      `json:"operation,omitempty"`
	Probes            []mapProbeJSON         `json:"probes,omitempty"`
	Component         *mapComponentJSON      `json:"component,omitempty"`
	Upstream          []mapHopJSON           `json:"upstream,omitempty"`
	Downstream        []mapHopJSON           `json:"downstream,omitempty"`
	OperationsThrough []string               `json:"operations_through,omitempty"`
	Observability     map[string]mapPointers `json:"observability,omitempty"`
}

func marshalMapData(data mapData) json.RawMessage {
	out, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	return out
}

func componentJSON(c appmap.Component) mapComponentJSON {
	out := mapComponentJSON{Name: c.Name, Workload: c.Workload, Services: c.Services, HoldsData: c.HoldsData || c.Data != nil}
	if c.Data != nil {
		out.Data = &struct {
			Engine      string   `json:"engine"`
			Role        string   `json:"role"`
			DerivedFrom []string `json:"derived_from,omitempty"`
		}{c.Data.Engine, c.Data.Role, c.Data.DerivedFrom}
	}
	return out
}

func operationJSON(op appmap.Operation) mapOperationJSON {
	return mapOperationJSON{ID: op.ID, Kind: opKind(op), Entry: mapEntryJSON{Service: op.Entry.Service, Port: op.Entry.Port, Method: op.Entry.Method, Path: op.Entry.Path}, Path: op.Path, Route: op.Route}
}

func hopsJSON(hops []mapHop) []mapHopJSON {
	var out []mapHopJSON
	for _, hop := range hops {
		via := ""
		if hop.Depth > 1 {
			via = hop.Via
		}
		out = append(out, mapHopJSON{Component: hop.Name, Via: via, Depth: hop.Depth, Possible: hop.Possible})
	}
	return out
}

func (g mapGraph) overviewData(point func(string) mapPointers) mapData {
	out := mapData{View: "overview", Observability: map[string]mapPointers{}}
	for _, c := range g.m.Components {
		out.Components = append(out.Components, componentJSON(c))
		out.Observability[c.Name] = point(c.Name)
	}
	for _, e := range g.m.Edges {
		out.Edges = append(out.Edges, mapEdgeJSON{From: e.From, To: e.To, Evidence: e.Evidence})
	}
	for _, e := range g.possible {
		if !g.hasEdge(e[0], e[1]) {
			out.Edges = append(out.Edges, mapEdgeJSON{From: e[0], To: e[1], Possible: true})
		}
	}
	for _, op := range g.m.Operations {
		out.Operations = append(out.Operations, operationJSON(op))
	}
	return out
}

func (g mapGraph) pathData(op appmap.Operation, verdicts map[string]string, point func(string) mapPointers) mapData {
	operation := operationJSON(op)
	out := mapData{View: "path", Operation: &operation, Observability: map[string]mapPointers{}}
	in := map[string]bool{}
	for _, name := range op.Path {
		in[name] = true
		if c, ok := g.comps[name]; ok {
			out.Components = append(out.Components, componentJSON(c))
		}
		out.Observability[name] = point(name)
	}
	for _, e := range g.m.Edges {
		if in[e.From] && in[e.To] {
			out.Edges = append(out.Edges, mapEdgeJSON{From: e.From, To: e.To, Evidence: e.Evidence})
		}
	}
	for _, p := range g.probesOf(op.ID) {
		status := verdicts[p.Scenario]
		if status == "" {
			status = "unknown"
		}
		out.Probes = append(out.Probes, mapProbeJSON{Scenario: p.Scenario, Kind: p.Kind, Status: status})
	}
	return out
}

func (g mapGraph) nearData(name string, point func(string) mapPointers) mapData {
	c := componentJSON(g.comps[name])
	return mapData{
		View: "near", Component: &c,
		Upstream: hopsJSON(g.reach(name, true, 2)), Downstream: hopsJSON(g.reach(name, false, 2)),
		OperationsThrough: g.operationsThrough(name),
		Observability:     map[string]mapPointers{name: point(name)},
	}
}
