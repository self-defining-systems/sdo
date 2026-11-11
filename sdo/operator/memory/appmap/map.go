package appmap

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/layout"
)

// Rule IDs of the findings. The memory check re-exports them as its own rules.
const (
	RuleMissing         = "map.missing"
	RuleSchema          = "map.schema"
	RuleUnknownName     = "map.unknown-name"
	RulePathMismatch    = "map.path-mismatch"
	RuleUncoveredOp     = "map.uncovered-operation"
	RuleUncoveredWrite  = "map.uncovered-write"
	RuleUncoveredData   = "map.uncovered-data"
	RuleUnexercisedEdge = "map.unexercised-edge"
	RuleUncoveredEdge   = "map.uncovered-edge"
	RuleUnprobedEdge    = "map.unprobed-edge"
	RuleKnownBroken     = "map.known-broken"
)

// Probe kinds, and the health-probe workload that runs each.
const (
	KindCall      = "call"
	KindWriteRead = "write-read"
	KindReadLater = "read-later"
)

// WorkloadForKind names the health-probe workload that holds each probe kind.
var WorkloadForKind = map[string]string{
	KindCall:      "probe-call",
	KindWriteRead: "probe-write",
	KindReadLater: "probe-read-later",
}

// Version is the only map format this validator reads.
const Version = 1

// Map is the application map.
type Map struct {
	Version    int         `yaml:"version"`
	Components []Component `yaml:"components"`
	Edges      []Edge      `yaml:"edges"`
	Operations []Operation `yaml:"operations"`
	Probes     []Probe     `yaml:"probes"`
	// Procedures are the operations procedures the application ships.
	Procedures []Procedure `yaml:"procedures,omitempty"`
}

// Component is one application workload that is part of the app.
type Component struct {
	Name      string   `yaml:"name"`
	Workload  string   `yaml:"workload"`
	Services  []string `yaml:"services"`
	HoldsData bool     `yaml:"holds_data"`
	// Evidence says what grounds holds_data and data: the same vocabulary as
	// an edge's. Empty means the author read it from the application source.
	Evidence []string `yaml:"evidence,omitempty"`
	Data     *Data    `yaml:"data,omitempty"`
}

// Evidence kinds: what grounds a component's role or an edge. config is the
// application source or its config files; manifest is the rendered manifests
// (an image, a volume, an env value, a ConfigMap entry), which is all there is
// for an application deployed without source; observed is the live
// application.
const (
	EvidenceConfig   = "config"
	EvidenceManifest = "manifest"
	EvidenceObserved = "observed"
)

var knownEvidence = map[string]bool{EvidenceConfig: true, EvidenceManifest: true, EvidenceObserved: true}

const evidenceHint = "evidence is a non-empty subset of [config, manifest, observed]"

// Engines a component's data role may name. Other is a store SDO has no
// engine-specific reader for.
const (
	EnginePostgres  = "postgres"
	EngineMySQL     = "mysql"
	EngineMongoDB   = "mongodb"
	EngineRedis     = "redis"
	EngineMemcached = "memcached"
	EngineConsul    = "consul"
	EngineOther     = "other"
)

// Data roles. An authoritative store is the source of truth for its data; a
// derived one (a cache, an index, a projection) can be rebuilt from the
// components it is derived from.
const (
	RoleAuthoritative = "authoritative"
	RoleDerived       = "derived"
)

var knownEngines = map[string]bool{EnginePostgres: true, EngineMySQL: true, EngineMongoDB: true, EngineRedis: true, EngineMemcached: true, EngineConsul: true, EngineOther: true}

// Data declares that a component is a data store: its engine, and whether it is
// the source of truth or a copy rebuilt from other components.
type Data struct {
	Engine      string   `yaml:"engine"`
	Role        string   `yaml:"role"`
	DerivedFrom []string `yaml:"derived_from,omitempty"`
}

// Edge is a directed call from one component to another. Unprobed records,
// for an edge from an entry component that no probed operation crosses, why
// no synthetic request can cross it; the memory check reports it as a coverage
// gap instead of asking for a probe.
type Edge struct {
	From     string   `yaml:"from"`
	To       string   `yaml:"to"`
	Evidence []string `yaml:"evidence"`
	Source   string   `yaml:"source"`
	Unprobed string   `yaml:"unprobed,omitempty"`
}

// Entry is where an operation enters the application.
type Entry struct {
	Service string `yaml:"service"`
	Port    int    `yaml:"port"`
	Method  string `yaml:"method"`
	Path    string `yaml:"path"`
}

// Operation is one user-facing operation.
type Operation struct {
	ID     string   `yaml:"id"`
	Writes bool     `yaml:"writes"`
	Entry  Entry    `yaml:"entry"`
	Path   []string `yaml:"path"`
}

// Probe is one probe scenario and the operations its steps exercise.
type Probe struct {
	Scenario   string   `yaml:"scenario"`
	Kind       string   `yaml:"kind"`
	Operations []string `yaml:"operations"`
}

// Finding is one diagnostic about the map. Rules are blocking or notes by the
// caller's policy; every Finding names the operation, component, edge or
// scenario at fault and says what to change.
type Finding struct {
	Rule    string `json:"rule"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

// Parse strictly decodes the region's YAML. Unknown fields and duplicate keys
// are errors.
func Parse(text string) (Map, error) {
	if err := layout.RejectDuplicateKeys([]byte(text)); err != nil {
		return Map{}, err
	}
	var m Map
	decoder := yaml.NewDecoder(bytes.NewReader([]byte(text)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&m); err != nil {
		return Map{}, err
	}
	return m, nil
}

// Inventory is the accepted source inventory: the workloads and Services the
// Go upkeep lists in the architecture text.
type Inventory struct {
	Workloads map[string]bool // "Kind/name"
	Services  map[string]bool
}

var inventoryLine = regexp.MustCompile(`(?m)^- ([A-Za-z]+)/([^/\s]+)/([^\s]+) \(source:`)

// ParseInventory reads the manifest inventory lines Go writes outside the region.
func ParseInventory(doc string) Inventory {
	inv := Inventory{Workloads: map[string]bool{}, Services: map[string]bool{}}
	for _, m := range inventoryLine.FindAllStringSubmatch(Outside(doc), -1) {
		if m[1] == "Service" {
			inv.Services[m[3]] = true
		}
		inv.Workloads[m[1]+"/"+m[3]] = true
	}
	return inv
}

// Workload is a traffic workload: its name, the scenario IDs it runs, and
// those it declares knownBroken (failing on the application as deployed).
type Workload struct {
	Name        string
	Scenarios   []string
	KnownBroken []string
}

var (
	idPattern  = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	httpMethod = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "OPTIONS": true}
)

type checker struct {
	m        Map
	findings []Finding
	byName   map[string]Component
	ops      map[string]Operation
	// knownBroken holds the probe scenarios a probe workload declares
	// knownBroken; broken holds the operations only such probes exercise.
	knownBroken map[string]bool
	broken      map[string]bool
}

func (c *checker) add(rule, hint, format string, args ...any) {
	c.findings = append(c.findings, Finding{Rule: rule, Message: fmt.Sprintf(format, args...), Hint: hint})
}

func opPaths(ops []Operation) map[string]Operation {
	out := map[string]Operation{}
	for _, op := range ops {
		out[op.ID] = op
	}
	return out
}

// Check applies the field rules and coverage rules to the map, against the
// source inventory and the traffic workloads. Findings are sorted for a stable
// report. Scenario facts (side effect, DependsOn) come from CheckScenarios.
func (m Map) Check(inv Inventory, workloads []Workload) []Finding {
	c := &checker{m: m, byName: map[string]Component{}, ops: map[string]Operation{}, knownBroken: map[string]bool{}, broken: map[string]bool{}}
	c.schema()
	c.dataRoles()
	c.inventory(inv)
	c.paths()
	c.probes(workloads)
	c.knownBrokenOperations()
	c.coverage()
	c.procedures(inv)
	sort.SliceStable(c.findings, func(i, j int) bool { return c.findings[i].Rule < c.findings[j].Rule })
	return c.findings
}

func (c *checker) schema() {
	if c.m.Version != Version {
		c.add(RuleSchema, "write `version: 1`", "map version is %d; this validator reads only version %d", c.m.Version, Version)
	}
	for _, comp := range c.m.Components {
		switch {
		case !idPattern.MatchString(comp.Name):
			c.add(RuleSchema, "use the workload name, lowercase letters, digits and '-'", "component name %q is not a valid logical ID", comp.Name)
			continue
		case c.byName[comp.Name].Name != "":
			c.add(RuleSchema, "component names are unique", "component %q is declared more than once", comp.Name)
			continue
		}
		c.byName[comp.Name] = comp
		for _, ev := range comp.Evidence {
			if !knownEvidence[ev] {
				c.add(RuleSchema, evidenceHint+"; omit it when the source declares the role", "component %s: evidence %q is not config, manifest or observed", comp.Name, ev)
			}
		}
	}
	edges := map[[2]string]bool{}
	for _, e := range c.m.Edges {
		label := e.From + " -> " + e.To
		_, fromOK := c.byName[e.From]
		_, toOK := c.byName[e.To]
		if !fromOK {
			c.add(RuleUnknownName, "edges connect declared components; add the component or fix the name", "edge %s: caller %q is not a component", label, e.From)
		}
		if !toOK {
			c.add(RuleUnknownName, "edges connect declared components; add the component or fix the name", "edge %s: callee %q is not a component", label, e.To)
		}
		switch {
		case e.From == e.To:
			c.add(RuleSchema, "remove the self-edge", "edge %s is a self-edge", label)
		case edges[[2]string{e.From, e.To}]:
			c.add(RuleSchema, "list each directed edge once", "edge %s is declared more than once", label)
		}
		edges[[2]string{e.From, e.To}] = true
		if len(e.Evidence) == 0 {
			c.add(RuleSchema, evidenceHint, "edge %s has no evidence", label)
		}
		for _, ev := range e.Evidence {
			if !knownEvidence[ev] {
				c.add(RuleSchema, evidenceHint, "edge %s: evidence %q is not config, manifest or observed", label, ev)
			}
		}
		if strings.TrimSpace(e.Source) == "" {
			c.add(RuleSchema, "config: the source path that declares the call; manifest: the object and field that name the callee; observed: one line saying how it was observed", "edge %s has no source", label)
		}
	}
	seenOp := map[string]bool{}
	for _, op := range c.m.Operations {
		if !idPattern.MatchString(op.ID) {
			c.add(RuleSchema, "use lowercase letters, digits and '-'", "operation id %q is not a valid logical ID", op.ID)
			continue
		}
		if seenOp[op.ID] {
			c.add(RuleSchema, "operation IDs are unique", "operation %q is declared more than once", op.ID)
			continue
		}
		seenOp[op.ID] = true
		c.ops[op.ID] = op
		if !httpMethod[op.Entry.Method] {
			c.add(RuleSchema, "use an upper-case HTTP method such as GET or POST", "operation %s: entry method %q is not an HTTP method", op.ID, op.Entry.Method)
		}
		if op.Entry.Port < 1 || op.Entry.Port > 65535 || op.Entry.Service == "" || !strings.HasPrefix(op.Entry.Path, "/") {
			c.add(RuleSchema, "entry needs a service, a port in 1-65535 and a path starting with /", "operation %s: entry %+v is incomplete", op.ID, op.Entry)
		}
		if len(op.Path) == 0 {
			c.add(RuleSchema, "path lists the components the operation reaches, entry component first", "operation %s has an empty path", op.ID)
		}
	}
}

func (c *checker) dataRoles() {
	const hint = "data is {engine: postgres|mysql|mongodb|redis|memcached|consul|other, role: authoritative|derived, derived_from: [authoritative components]}"
	for _, comp := range c.m.Components {
		d := comp.Data
		if d == nil || c.byName[comp.Name].Name != comp.Name {
			continue
		}
		if !knownEngines[d.Engine] {
			c.add(RuleSchema, hint, "component %s: data engine %q is not a known engine", comp.Name, d.Engine)
		}
		switch d.Role {
		case RoleAuthoritative:
			if len(d.DerivedFrom) > 0 {
				c.add(RuleSchema, "an authoritative store is the source of truth; remove derived_from, or make the role derived", "component %s: authoritative data lists derived_from %v", comp.Name, d.DerivedFrom)
			}
		case RoleDerived:
			if comp.HoldsData {
				c.add(RuleSchema, "a derived copy refills from its sources, so it does not hold data; set holds_data: false, or make the role authoritative", "component %s: derived data cannot set holds_data: true", comp.Name)
			}
			if len(d.DerivedFrom) == 0 {
				c.add(RuleSchema, "list the authoritative components the copy is rebuilt from in derived_from", "component %s: derived data has no derived_from", comp.Name)
			}
			for _, source := range d.DerivedFrom {
				target, ok := c.byName[source]
				switch {
				case source == comp.Name:
					c.add(RuleSchema, "derived_from names the components the copy is rebuilt from", "component %s: derived_from names itself", comp.Name)
				case !ok:
					c.add(RuleUnknownName, "derived_from names declared components", "component %s: derived_from %q is not a component", comp.Name, source)
				case target.Data == nil || target.Data.Role != RoleAuthoritative:
					c.add(RuleSchema, fmt.Sprintf("declare data.role: authoritative on %s, or name its source of truth instead", source), "component %s: derived_from %s is not an authoritative data component", comp.Name, source)
				}
			}
		default:
			c.add(RuleSchema, hint, "component %s: data role %q is not authoritative or derived", comp.Name, d.Role)
		}
	}
}

func (c *checker) inventory(inv Inventory) {
	for _, comp := range c.m.Components {
		if c.byName[comp.Name].Name != comp.Name {
			continue
		}
		if !inv.Workloads[comp.Workload] {
			c.add(RuleUnknownName, "workload is Kind/name as listed in the manifest inventory above the map", "component %s: workload %q is not in the accepted source inventory", comp.Name, comp.Workload)
		}
		for _, s := range comp.Services {
			if !inv.Services[s] {
				c.add(RuleUnknownName, "services are Service names listed in the manifest inventory", "component %s: Service %q is not in the accepted source inventory", comp.Name, s)
			}
		}
	}
}

func (c *checker) servicesOf(component string) []string { return c.byName[component].Services }

func (c *checker) paths() {
	adjacent := map[string][]string{}
	for _, e := range c.m.Edges {
		adjacent[e.From] = append(adjacent[e.From], e.To)
	}
	for _, op := range c.m.Operations {
		if c.ops[op.ID].ID != op.ID || len(op.Path) == 0 {
			continue
		}
		in := map[string]bool{}
		for _, name := range op.Path {
			if _, ok := c.byName[name]; !ok {
				c.add(RuleUnknownName, "path lists declared components; add the component or fix the name", "operation %s: path component %q is not a component", op.ID, name)
				continue
			}
			in[name] = true
		}
		if !in[op.Path[0]] {
			continue
		}
		if !slicesContains(c.servicesOf(op.Path[0]), op.Entry.Service) {
			c.add(RulePathMismatch, fmt.Sprintf("entry.service must be one of the Services of %s: %v", op.Path[0], c.servicesOf(op.Path[0])), "operation %s: entry service %q is not a Service of its first path component %s", op.ID, op.Entry.Service, op.Path[0])
		}
		reached := map[string]bool{op.Path[0]: true}
		queue := []string{op.Path[0]}
		for len(queue) > 0 {
			next := queue[0]
			queue = queue[1:]
			for _, to := range adjacent[next] {
				if in[to] && !reached[to] {
					reached[to] = true
					queue = append(queue, to)
				}
			}
		}
		for _, name := range op.Path {
			if in[name] && !reached[name] {
				c.add(RulePathMismatch, "add the call edge that reaches it, or remove it from the path", "operation %s: component %s is on the path but no chain of edges inside the path reaches it from %s", op.ID, name, op.Path[0])
			}
		}
	}
}

func (c *checker) probes(workloads []Workload) {
	holder := map[string][]string{} // scenario -> probe workloads listing it
	listed := map[string]bool{}
	byKind := map[string]string{}
	for kind, name := range WorkloadForKind {
		byKind[name] = kind
	}
	for _, w := range workloads {
		if _, ok := byKind[w.Name]; !ok {
			continue
		}
		for _, id := range w.Scenarios {
			holder[id] = append(holder[id], w.Name)
		}
		for _, id := range w.KnownBroken {
			c.knownBroken[id] = true
		}
	}
	seen := map[string]bool{}
	for _, p := range c.m.Probes {
		if seen[p.Scenario] {
			c.add(RuleSchema, "one probe entry per scenario", "probe %q is declared more than once", p.Scenario)
			continue
		}
		seen[p.Scenario] = true
		listed[p.Scenario] = true
		want, kindOK := WorkloadForKind[p.Kind]
		if !kindOK {
			c.add(RuleSchema, "kind is call, write-read or read-later", "probe %s: kind %q is unknown", p.Scenario, p.Kind)
		}
		if len(p.Operations) == 0 {
			c.add(RuleSchema, "list the operations the probe's steps exercise", "probe %s lists no operations", p.Scenario)
		}
		writes, holds := false, false
		for _, id := range p.Operations {
			op, ok := c.ops[id]
			if !ok {
				c.add(RuleUnknownName, "probes list declared operations; add the operation or fix the name", "probe %s: operation %q is not declared", p.Scenario, id)
				continue
			}
			writes = writes || op.Writes
			for _, name := range op.Path {
				holds = holds || c.byName[name].HoldsData
			}
		}
		if p.Kind == KindWriteRead && !writes {
			c.add(RuleSchema, "include an operation with writes: true, or make the probe kind call", "probe %s is write-read but none of its operations writes", p.Scenario)
		}
		if p.Kind == KindReadLater && !holds {
			c.add(RuleSchema, "a read-later probe must reach a holds_data component; include the operation that reads it", "probe %s is read-later but its path holds no holds_data component", p.Scenario)
		}
		in := holder[p.Scenario]
		switch {
		case len(in) == 0:
			c.add(RuleSchema, fmt.Sprintf("add the scenario to health-probe workload %s, or remove the probe entry", want), "probe %s is in none of the workloads probe-call, probe-write, probe-read-later", p.Scenario)
		case len(in) > 1:
			c.add(RuleSchema, "a scenario belongs to exactly one of the three workloads", "probe %s is listed in several probe workloads: %s", p.Scenario, strings.Join(in, ", "))
		case kindOK && in[0] != want:
			c.add(RuleSchema, fmt.Sprintf("move the scenario to workload %s or change the probe kind to %s", want, byKind[in[0]]), "probe %s has kind %s but runs in workload %s (kind %s belongs in %s)", p.Scenario, p.Kind, in[0], p.Kind, want)
		}
	}
	for id, in := range holder {
		if !listed[id] {
			c.add(RuleSchema, "add a probe entry (scenario, kind, operations) to the map", "scenario %s in workload %s has no probe entry in the application map", id, strings.Join(in, ", "))
		}
	}
}

// knownBrokenOperations finds the operations that only knownBroken probes
// exercise. Such an operation is covered without a gated probe, so each
// knownBroken probe must exercise one, and they may be at most half of the
// operations: a broken-at-baseline declaration never hides a working one.
func (c *checker) knownBrokenOperations() {
	gated := map[string]bool{}
	for _, p := range c.m.Probes {
		if !c.knownBroken[p.Scenario] {
			for _, id := range p.Operations {
				gated[id] = true
			}
		}
	}
	for _, p := range c.m.Probes {
		if !c.knownBroken[p.Scenario] {
			continue
		}
		own := false
		for _, id := range p.Operations {
			if _, ok := c.ops[id]; ok && !gated[id] {
				c.broken[id], own = true, true
			}
		}
		if !own {
			c.add(RuleKnownBroken, "a knownBroken probe exercises an operation that is broken in the application; when its operations work, fix the scenario so it passes", "probe %s is knownBroken but a gated probe already exercises every operation it lists (%s)", p.Scenario, strings.Join(p.Operations, ", "))
		}
	}
	if len(c.broken) > 0 && 2*len(c.broken) > len(c.ops) {
		var ids []string
		for id := range c.broken {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		c.add(RuleKnownBroken, "declare knownBroken only for operations broken in the application as deployed; check that the probes match the live application", "%d of %d operations are known-broken (%s); at most half may be", len(c.broken), len(c.ops), strings.Join(ids, ", "))
	}
}

func (c *checker) coverage() {
	inKind := func(kind string) map[string]bool {
		out := map[string]bool{}
		for _, p := range c.m.Probes {
			if p.Kind == kind && !c.knownBroken[p.Scenario] {
				for _, id := range p.Operations {
					out[id] = true
				}
			}
		}
		return out
	}
	call, write := inKind(KindCall), inKind(KindWriteRead)
	for _, op := range c.m.Operations {
		if c.broken[op.ID] {
			continue
		}
		if !call[op.ID] {
			c.add(RuleUncoveredOp, fmt.Sprintf("add a `call` probe scenario (workload %s) that exercises operation %s", WorkloadForKind[KindCall], op.ID), "operation %s is in no call probe", op.ID)
		}
		if op.Writes && !write[op.ID] {
			c.add(RuleUncoveredWrite, fmt.Sprintf("add a `write-read` probe scenario (workload %s) that performs operation %s and reads the data back", WorkloadForKind[KindWriteRead], op.ID), "writing operation %s is in no write-read probe", op.ID)
		}
	}
	readLater := map[string]bool{}
	for _, p := range c.m.Probes {
		if p.Kind != KindReadLater || c.knownBroken[p.Scenario] {
			continue
		}
		for _, id := range p.Operations {
			for _, name := range c.ops[id].Path {
				readLater[name] = true
			}
		}
	}
	written := map[string][]string{}
	together := map[[2]string]bool{}
	for _, op := range c.m.Operations {
		for _, a := range op.Path {
			for _, b := range op.Path {
				together[[2]string{a, b}] = true
			}
			if op.Writes && !c.broken[op.ID] {
				written[a] = append(written[a], op.ID)
			}
		}
	}
	for _, comp := range c.m.Components {
		if comp.HoldsData && len(written[comp.Name]) > 0 && !readLater[comp.Name] {
			c.add(RuleUncoveredData, fmt.Sprintf("add a `read-later` probe scenario (workload %s) that reads back, in a later iteration, data written earlier through %s", WorkloadForKind[KindReadLater], strings.Join(dedupe(written[comp.Name]), ", ")), "data component %s (written by %s) is on no read-later probe path", comp.Name, strings.Join(dedupe(written[comp.Name]), ", "))
		}
	}
	probed := c.m.probedPairs()
	entry := c.m.entryComponents()
	for _, e := range c.m.Edges {
		reason := strings.TrimSpace(e.Unprobed)
		switch {
		case probed[[2]string{e.From, e.To}] && reason != "":
			c.add(RuleSchema, "remove unprobed: a probe already crosses this edge", "edge %s -> %s is recorded unprobed (%s) but a probed operation crosses it", e.From, e.To, reason)
		case probed[[2]string{e.From, e.To}]:
		case reason != "":
			c.add(RuleUnprobedEdge, "note only: a coverage gap the designer recorded; cover it with a probe when a request can cross it", "edge %s -> %s is not probed: %s", e.From, e.To, reason)
		case entry[e.From]:
			c.add(RuleUncoveredEdge, fmt.Sprintf("add an operation whose path crosses %s -> %s and a probe that exercises it; only when no synthetic request can cross it, set `unprobed: <why>` on the edge", e.From, e.To), "edge %s -> %s from entry component %s lies on no probed operation path", e.From, e.To, e.From)
		case !together[[2]string{e.From, e.To}]:
			c.add(RuleUnexercisedEdge, "note only: an operation whose path includes both ends would exercise it", "edge %s -> %s lies on no operation path", e.From, e.To)
		}
	}
}

// probedPairs holds the component pairs that share the path of an operation
// some probe exercises.
func (m Map) probedPairs() map[[2]string]bool {
	ops := opPaths(m.Operations)
	out := map[[2]string]bool{}
	for _, p := range m.Probes {
		for _, id := range p.Operations {
			path := ops[id].Path
			for _, a := range path {
				for _, b := range path {
					out[[2]string{a, b}] = true
				}
			}
		}
	}
	return out
}

// entryComponents are where requests enter the application: the first
// component of an operation's path, and every component with a Service that
// no edge calls.
func (m Map) entryComponents() map[string]bool {
	called := map[string]bool{}
	for _, e := range m.Edges {
		called[e.To] = true
	}
	out := map[string]bool{}
	for _, op := range m.Operations {
		if len(op.Path) > 0 {
			out[op.Path[0]] = true
		}
	}
	for _, comp := range m.Components {
		if len(comp.Services) > 0 && !called[comp.Name] {
			out[comp.Name] = true
		}
	}
	return out
}

// UncoveredEdges lists, sorted, the edges from an entry component that no
// probed operation crosses and that record no unprobed reason: the edges the
// detection designer must cover with a probe or explain.
func (m Map) UncoveredEdges() []Edge {
	probed := m.probedPairs()
	entry := m.entryComponents()
	var out []Edge
	for _, e := range m.Edges {
		if entry[e.From] && !probed[[2]string{e.From, e.To}] && strings.TrimSpace(e.Unprobed) == "" {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

// Scenario is what the compiled traffic catalog says about a scenario.
type Scenario struct {
	ID         string
	SideEffect string
	DependsOn  []string
	// SetupSteps counts the setup steps that create the scenario's own data.
	SetupSteps int
	// StatusOnly is true when no step checks more than the response status.
	StatusOnly bool
}

// CheckScenarios applies the rules that need the compiled scenarios: each
// probe's side effect matches its kind, and its DependsOn names a Service of
// every component on the probe's path and none outside it; a read-later
// scenario creates the data it reads in its own setup steps; no probe checks
// only statuses.
func (m Map) CheckScenarios(scenarios []Scenario) []Finding {
	c := &checker{m: m, byName: map[string]Component{}, ops: opPaths(m.Operations)}
	for _, comp := range m.Components {
		c.byName[comp.Name] = comp
	}
	byID := map[string]Scenario{}
	for _, s := range scenarios {
		byID[s.ID] = s
	}
	for _, p := range m.Probes {
		s, ok := byID[p.Scenario]
		if !ok {
			c.add(RuleSchema, "define the scenario in the traffic generators, or remove the probe entry", "probe %s is not a scenario of the traffic generators", p.Scenario)
			continue
		}
		switch p.Kind {
		case KindWriteRead:
			if s.SideEffect != "idempotent-write" && s.SideEffect != "write-with-cleanup" {
				c.add(RuleSchema, "a write-read scenario declares SideEffect idempotent-write or write-with-cleanup", "probe %s is write-read but scenario side effect is %q", p.Scenario, s.SideEffect)
			}
		case KindReadLater:
			if s.SideEffect != "read" {
				c.add(RuleSchema, "a read-later scenario declares SideEffect read", "probe %s is read-later but scenario side effect is %q", p.Scenario, s.SideEffect)
			}
			if s.SetupSteps == 0 {
				c.add(RuleSchema, "write the fixture the scenario reads in its Setup steps, through the application's own write operation, with the Marker and an ensure step (OrExisting); never rely on data set up by hand", "probe %s is read-later but its scenario has no setup steps, so on a freshly deployed application nothing creates the data it reads", p.Scenario)
			}
		}
		if s.StatusOnly {
			c.add(RuleSchema, "check the content the healthy application returns: the exact value a write chose (JSONEquals), the exact set of the probe's own data (ExactSet), the shape of every returned member (Each), or at least a text it always contains", "probe %s: its scenario checks only the status of every step, so a wrong or empty answer passes", p.Scenario)
		}
		path := map[string]bool{}
		for _, id := range p.Operations {
			for _, name := range c.ops[id].Path {
				path[name] = true
			}
		}
		onPath := map[string]string{}
		var names []string
		for name := range path {
			names = append(names, name)
		}
		sort.Strings(names)
		depends := map[string]bool{}
		for _, d := range s.DependsOn {
			depends[d] = true
		}
		for _, name := range names {
			services := c.byName[name].Services
			for _, svc := range services {
				onPath[svc] = name
			}
			if len(services) > 0 && !anyIn(services, depends) {
				c.add(RulePathMismatch, fmt.Sprintf("add a Service of %s, such as %q, to the scenario's DependsOn", name, services[0]), "probe %s: DependsOn names no Service of component %s, which is on its path (operations %s)", p.Scenario, name, strings.Join(p.Operations, ", "))
			}
		}
		for _, d := range s.DependsOn {
			if _, ok := onPath[d]; !ok {
				c.add(RulePathMismatch, "DependsOn lists only Services of components on the probe's path; remove it, or add the operation that reaches it to the probe", "probe %s: DependsOn names Service %q, which belongs to no component on the probe's path (path: %s)", p.Scenario, d, strings.Join(names, ", "))
			}
		}
	}
	sort.SliceStable(c.findings, func(i, j int) bool { return c.findings[i].Rule < c.findings[j].Rule })
	return c.findings
}

func anyIn(items []string, set map[string]bool) bool {
	for _, item := range items {
		if set[item] {
			return true
		}
	}
	return false
}

func slicesContains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func dedupe(items []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}
