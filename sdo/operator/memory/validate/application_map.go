package validate

import (
	"context"
	"slices"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// mapBlocks reports whether application-map gaps reject the candidate: they do
// for the detection designer's commits and are notes for every other actor, so
// reflection never fails on a gap the designer left.
func (r Request) mapBlocks() bool { return r.Actor == HealthJudge && r.Phase == InitialPhase }

// mapAlwaysNote lists the findings that never reject.
func mapAlwaysNote(rule string) bool {
	return rule == RuleMapMissing || rule == RuleMapUnprobed || rule == RuleMapPossibleEdge
}

// AgentObligation reports whether a diagnostic names work the agent's edit is
// for rather than a defect of the tree: the empty diff, and the coverage the
// detection designer adds (operations, probes, user-path components). An unedited base
// carries these by construction (Go seeds the map without probes), so the
// base check never reports them; the designer's proposal is still held to them.
func AgentObligation(rule string) bool {
	switch rule {
	case RuleEmptyDiff, RuleMapUncoveredOp, RuleMapUncoveredWrite, RuleMapUncoveredData, RuleMapCoverage:
		return true
	}
	return false
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
		ID          string `yaml:"id"`
		KnownBroken any    `yaml:"knownBroken"`
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
		s.mapFinding(Semantic, appmap.Finding{Rule: RuleMapSchema, Message: "application map does not parse: " + err.Error(), Hint: "version 1 fields only: components, edges, operations, probes, procedures; unknown or duplicate keys are rejected"}, errorLine(err, region.YAMLLine-1))
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
			if scenario.KnownBroken != nil {
				w.KnownBroken = append(w.KnownBroken, scenario.ID)
			}
		}
		workloads = append(workloads, w)
	}
	for _, f := range parsed.Check(s.mapInventory(), workloads) {
		s.mapFinding(Semantic, f, region.Line)
	}
	for _, f := range parsed.CheckPossibleEdges(source) {
		s.mapFinding(Semantic, f, 0)
	}
}

// mapInventory is the inventory the map's names resolve against: the manifest
// lines Go wrote into arch.md from the rendered accepted source. Validation
// never reads the source tree itself, so Helm and Kustomize sources resolve
// the same way raw manifests do. When arch.md carries no lines, the empty
// inventory makes every name a finding.
func (s *run) mapInventory() appmap.Inventory {
	return appmap.ParseInventory(s.arch.doc.Source)
}
