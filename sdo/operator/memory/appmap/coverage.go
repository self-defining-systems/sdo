package appmap

import (
	"fmt"
	"sort"
	"strings"
)

// Coverage says, per component, whether a probed user operation observes it.
// It is the one computation of probe coverage: the designer's install
// obligation (map.coverage), the designer brief and the client-link ordering
// all read it.
type Coverage struct {
	Components map[string]ComponentCoverage
	// ExposedEntries is true when the entries are the Services the manifests
	// expose outside the cluster; false when nothing mapped is exposed and the
	// entries fall back to the entry Services of the declared operations.
	ExposedEntries bool
}

// ComponentCoverage is one component's coverage.
type ComponentCoverage struct {
	// Entry is true when a user entry Service belongs to the component.
	Entry bool
	// OnUserPath is true when the component is reachable from an entry
	// component over mapped (or possible) edges, the entry included.
	OnUserPath bool
	// Probes are the scenario IDs of the gated probes that exercise an
	// operation entering at a user entry Service whose path includes the
	// component, sorted.
	Probes []string
	// Unprobed is the designer's recorded reason no user operation can be
	// probed through the component.
	Unprobed string
}

// Obligation reports whether the designer still owes the component a covering
// probe or an unprobed reason.
func (c ComponentCoverage) Obligation() bool {
	return c.OnUserPath && len(c.Probes) == 0 && strings.TrimSpace(c.Unprobed) == ""
}

// Coverage computes probe coverage of the map. Entry Services are the
// Services of non-store components the manifests expose outside the cluster
// (NodePort, LoadBalancer or an Ingress backend, metrics-only Services
// excluded by the inventory writer). When none is mapped, they are the entry
// Services of the declared operations whose port is not a metrics port. A
// Service nothing calls is not an entry by itself.
//
// TODO(recovered-test U1): a structured entry list in the human-owned goal
// would take precedence over exposure; see the design doc.
func (m Map) Coverage(inv Inventory, workloads []Workload) Coverage {
	knownBroken := map[string]bool{}
	for _, w := range workloads {
		for _, id := range w.KnownBroken {
			knownBroken[id] = true
		}
	}
	out := Coverage{Components: map[string]ComponentCoverage{}}
	owner := map[string]string{}
	for _, comp := range m.Components {
		out.Components[comp.Name] = ComponentCoverage{Unprobed: strings.TrimSpace(comp.Unprobed)}
		for _, svc := range comp.Services {
			owner[svc] = comp.Name
		}
	}
	entryServices := map[string]bool{}
	for _, comp := range m.Components {
		if comp.Data != nil {
			continue
		}
		for _, svc := range comp.Services {
			if len(inv.Exposure[svc]) > 0 {
				entryServices[svc] = true
			}
		}
	}
	out.ExposedEntries = len(entryServices) > 0
	if !out.ExposedEntries {
		for _, op := range m.Operations {
			name, ok := owner[op.Entry.Service]
			if !ok || containsInt(inv.MetricsPorts[op.Entry.Service], op.Entry.Port) {
				continue
			}
			if comp := m.component(name); comp.Data == nil {
				entryServices[op.Entry.Service] = true
			}
		}
	}
	adjacent := map[string][]string{}
	for _, e := range m.Edges {
		adjacent[e.From] = append(adjacent[e.From], e.To)
	}
	for _, e := range inv.Possible {
		adjacent[e[0]] = append(adjacent[e[0]], e[1])
	}
	var queue []string
	for svc := range entryServices {
		name := owner[svc]
		c := out.Components[name]
		if !c.Entry {
			c.Entry, c.OnUserPath = true, true
			out.Components[name] = c
			queue = append(queue, name)
		}
	}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		for _, to := range adjacent[next] {
			c, ok := out.Components[to]
			if !ok || c.OnUserPath {
				continue
			}
			c.OnUserPath = true
			out.Components[to] = c
			queue = append(queue, to)
		}
	}
	ops := opPaths(m.Operations)
	for _, p := range m.Probes {
		if knownBroken[p.Scenario] {
			continue
		}
		for _, id := range p.Operations {
			op, ok := ops[id]
			if !ok || !entryServices[op.Entry.Service] || containsInt(inv.MetricsPorts[op.Entry.Service], op.Entry.Port) {
				continue
			}
			for _, name := range op.Path {
				c, ok := out.Components[name]
				if !ok || slicesContains(c.Probes, p.Scenario) {
					continue
				}
				c.Probes = append(c.Probes, p.Scenario)
				sort.Strings(c.Probes)
				out.Components[name] = c
			}
		}
	}
	return out
}

// Obligations lists, sorted, the components the designer still owes a
// covering probe or an unprobed reason.
func (c Coverage) Obligations() []string {
	var out []string
	for name, comp := range c.Components {
		if comp.Obligation() {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func (m Map) component(name string) Component {
	for _, comp := range m.Components {
		if comp.Name == name {
			return comp
		}
	}
	return Component{}
}

// coverageRule reports each user-path component no probed user operation
// observes and that records no unprobed reason, and notes the reasons
// recorded.
func (c *checker) coverageRule(inv Inventory, workloads []Workload) {
	cov := c.m.Coverage(inv, workloads)
	entries := map[string]bool{}
	for _, comp := range c.m.Components {
		if cov.Components[comp.Name].Entry {
			for _, svc := range comp.Services {
				entries[svc] = true
			}
		}
	}
	var via []string
	for svc := range entries {
		via = append(via, svc)
	}
	sort.Strings(via)
	for _, comp := range c.m.Components {
		if c.byName[comp.Name].Name != comp.Name {
			continue
		}
		got := cov.Components[comp.Name]
		reason := got.Unprobed
		switch {
		case reason != "" && len(got.Probes) > 0:
			c.add(RuleSchema, "remove unprobed: a probed user operation already covers it", "component %s is recorded unprobed (%s) but probe %s covers it", comp.Name, reason, strings.Join(got.Probes, ", "))
		case reason != "":
			c.add(RuleUnprobed, "note only: the component is verified at reachability strength (its link dials) until a user operation through an entry is probed", "component %s is not probed: %s", comp.Name, reason)
		case got.Obligation():
			c.add(RuleCoverage, fmt.Sprintf("add an operation entering at a user entry Service (%s) whose path includes %s, and a probe that exercises it; only when no synthetic request can, set `unprobed: <why>` on the component", strings.Join(via, ", "), comp.Name), "component %s is on the user path but no probed operation entering at a user entry Service includes it", comp.Name)
		}
	}
}

func containsInt(items []int, want int) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
