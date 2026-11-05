package validate

import (
	"context"
	"slices"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// mapBlocks reports whether application-map gaps reject the candidate: they do
// for the detection designer's commits and are notes for every other actor, so
// reflection never fails on a gap the designer left.
func (r Request) mapBlocks() bool { return r.Actor == HealthJudge && r.Phase == InitialPhase }

// mapAlwaysNote lists the findings that never reject.
func mapAlwaysNote(rule string) bool {
	return rule == RuleMapMissing || rule == RuleMapUnexercisedEdge
}

// mapFinding records one map finding: blocking for the designer once a map
// exists, a non-blocking note otherwise. Notes repeat across the check's passes
// over the same tree, so each is recorded once.
func (s *run) mapFinding(layer Layer, f appmap.Finding, line int) {
	d := Diagnostic{Rule: f.Rule, Path: layout.ArchitectureFile, Line: line, Message: f.Message, Hint: f.Hint}
	if s.req.mapBlocks() && !mapAlwaysNote(f.Rule) {
		s.add(layer, d)
		return
	}
	if s.layers()&layer != 0 && s.req.CoverageGaps != nil && !slices.Contains(*s.req.CoverageGaps, d) {
		s.req.noteGaps(d)
	}
}

type workloadScenarios struct {
	Scenarios []struct {
		ID string `yaml:"id"`
	} `yaml:"scenarios"`
}

// applicationMap checks the application-map region of arch.md against the
// source inventory and the traffic workloads. Rules that need the compiled
// scenarios (side effect, DependsOn) run in the compile layer.
func (s *run) applicationMap(ctx context.Context) {
	if s.arch == nil {
		return
	}
	source := s.arch.doc.Source
	region, found, err := appmap.Find(source)
	if !found {
		s.mapFinding(Semantic, appmap.Finding{Rule: RuleMapMissing, Message: "arch.md has no application map", Hint: "the detection designer adds one between " + appmap.BeginMarker + " and " + appmap.EndMarker + " after the inventory, then writes probes from it"}, 0)
		return
	}
	if err != nil {
		s.mapFinding(Semantic, appmap.Finding{Rule: RuleMapSchema, Message: err.Error()}, 0)
		return
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		s.mapFinding(Semantic, appmap.Finding{Rule: RuleMapSchema, Message: "application map does not parse: " + err.Error(), Hint: "version 1 fields only: components, edges, operations, probes; unknown or duplicate keys are rejected"}, errorLine(err, region.YAMLLine-1))
		return
	}
	if line := firstLine([]byte(region.Block), layout.ContainsPhysicalPath); line > 0 {
		s.mapFinding(Semantic, appmap.Finding{Rule: RuleMapSchema, Message: "the application map names a physical operational-memory path", Hint: "the map holds logical names only: Service and workload names, operation and scenario IDs"}, region.Line+line-1)
	}
	var workloads []appmap.Workload
	for name, file := range s.workloads {
		var decoded workloadScenarios
		if yaml.Unmarshal(file.content, &decoded) != nil {
			continue // yaml.parse already reports it
		}
		w := appmap.Workload{Name: name}
		for _, scenario := range decoded.Scenarios {
			w.Scenarios = append(w.Scenarios, scenario.ID)
		}
		workloads = append(workloads, w)
	}
	for _, f := range parsed.Check(s.mapInventory(ctx), workloads) {
		s.mapFinding(Semantic, f, region.Line)
	}
}

// mapInventory is the inventory the map's names resolve against: the manifest
// lines in arch.md, or for older memories that lack them the accepted source
// commit arch.md names, read through the same storage edge that writes them.
// When neither resolves, the empty inventory makes every name a finding.
func (s *run) mapInventory(ctx context.Context) appmap.Inventory {
	source := s.arch.doc.Source
	inventory := appmap.ParseInventory(source)
	if len(inventory.Workloads) > 0 {
		return inventory
	}
	commit, _ := s.arch.doc.Meta["generated_at_commit"].(string)
	facts, err := memory.SourceInventory(ctx, s.req.Root, commit)
	if err != nil {
		return inventory
	}
	for _, resource := range facts.Resources {
		if resource.Kind == "Service" {
			inventory.Services[resource.Name] = true
		}
		inventory.Workloads[resource.Kind+"/"+resource.Name] = true
	}
	return inventory
}
