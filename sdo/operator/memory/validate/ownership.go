package validate

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// kindSource stands for every repository file outside operational memory.
const kindSource layout.Kind = "source"

// grant says that an actor may change files of the listed kinds in a phase.
// only, when set, narrows the grant by the file's logical ID. A change that no
// grant covers is denied; there is no other source of write authority.
type grant struct {
	actor Actor
	phase Phase
	kinds []layout.Kind
	only  func(kind layout.Kind, id string) bool
}

func notIncidentTraffic(kind layout.Kind, id string) bool { return !layout.IncidentTraffic(kind, id) }

func incidentTraffic(kind layout.Kind, id string) bool { return layout.IncidentTraffic(kind, id) }

func healthObjectiveOnly(kind layout.Kind, id string) bool {
	return kind != layout.KindPlaybook || id == healthObjectivePlaybook
}

// grants is the whole ownership policy, kind by actor by phase.
//
// Phases: the human goal, Upkeep schema, deployer source and architecture,
// and health-judge bootstrap stages are InitialPhase. Responders change
// operational memory only in ReflectionPhase, after an independently verified
// outcome, and source only in ProposalPhase. No actor edits memory in
// ProposalPhase, and nobody but the controller writes outcomes or lifecycle
// provenance.
//
// Incident traffic: responders own the incident generator and incident-*
// workloads, and the health judge owns all other traffic. This is the broker's
// long-standing behavior and docs/architecture.md's ownership matrix. The
// typed OperationalArtifactProposal path in memory/artifact_proposal.go
// rejects responder traffic outright; it is retired by the move to
// file-based memory edits and does not define ownership.
var grants = []grant{
	{actor: Responder, phase: ProposalPhase, kinds: []layout.Kind{kindSource}},
	{actor: Deployer, phase: InitialPhase, kinds: []layout.Kind{kindSource}},

	{actor: Human, phase: InitialPhase, kinds: []layout.Kind{layout.KindGoal}},
	{actor: Deployer, phase: InitialPhase, kinds: []layout.Kind{layout.KindArchitecture}},
	{actor: Upkeep, phase: InitialPhase, kinds: []layout.Kind{
		layout.KindArchitecture, layout.KindSchemaVersion, layout.KindDiagnosticsModule, layout.KindLegacyDetectorManifest,
		layout.KindHealthDetectorManifest, layout.KindIncidentDetectorManifest, layout.KindHealthDetectorSource, layout.KindIncidentDetectorSource,
	}},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindHealthDetectorManifest, layout.KindHealthDetectorSource}},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindTrafficGenerator, layout.KindTrafficWorkload}, only: notIncidentTraffic},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindPlaybookIndex, layout.KindPlaybook}, only: healthObjectiveOnly},
	{actor: Responder, phase: ReflectionPhase, kinds: []layout.Kind{
		layout.KindIncidentDetectorManifest, layout.KindIncidentDetectorSource,
		layout.KindPlaybookIndex, layout.KindPlaybook, layout.KindPlaybookScript,
	}},
	{actor: Responder, phase: ReflectionPhase, kinds: []layout.Kind{layout.KindTrafficGenerator, layout.KindTrafficWorkload}, only: incidentTraffic},
}

func knownActor(a Actor) bool {
	switch a {
	case Human, Deployer, HealthJudge, Responder, Upkeep:
		return true
	}
	return false
}

func knownPhase(p Phase) bool {
	switch p {
	case ProposalPhase, ReflectionPhase, InitialPhase, OutcomePhase:
		return true
	}
	return false
}

func allowed(actor Actor, phase Phase, kind layout.Kind, id string) bool {
	for _, g := range grants {
		if g.actor != actor || g.phase != phase {
			continue
		}
		for _, k := range g.kinds {
			if k == kind && (g.only == nil || g.only(kind, id)) {
				return true
			}
		}
	}
	return false
}

// owners describes who may change a kind, for a diagnostic's hint.
func owners(kind layout.Kind, id string) string {
	var found []string
	for _, g := range grants {
		for _, k := range g.kinds {
			if k == kind && (g.only == nil || g.only(kind, id)) {
				found = append(found, fmt.Sprintf("%s in the %s phase", g.actor, g.phase))
			}
		}
	}
	if len(found) == 0 {
		return "no agent may change this file; the controller owns it"
	}
	sort.Strings(found)
	return "changeable by " + strings.Join(found, " or ")
}

func (s *run) ownership(ctx context.Context) error {
	if s.layers()&Ownership == 0 {
		return nil
	}
	actor, phase := s.req.Actor, s.req.Phase
	if !knownActor(actor) {
		s.add(Ownership, Diagnostic{Rule: RuleUnknownActor, Message: fmt.Sprintf("unknown actor %q", actor), Hint: "actors are human, deployer, health_judge, responder, and upkeep"})
		return nil
	}
	if !knownPhase(phase) {
		s.add(Ownership, Diagnostic{Rule: RuleUnknownPhase, Message: fmt.Sprintf("unknown phase %q", phase), Hint: "phases are proposal, reflection, initial, and outcome"})
		return nil
	}
	for _, p := range s.changed {
		if p == "" || path.IsAbs(p) || strings.Contains(p, "\\") || path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") {
			s.add(Ownership, Diagnostic{Rule: RuleUnsafePath, Path: p, Message: "unsafe path"})
			continue
		}
		kind, id := kindSource, ""
		if layout.IsMemoryPath(p) {
			var ok bool
			if kind, id, ok = layout.Classify(p); !ok {
				s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s cannot change an unclassified path", actor), Hint: "only paths in the operational-memory layout can be changed"})
				continue
			}
		}
		if !allowed(actor, phase, kind, id) {
			s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s may not change %s files in the %s phase", actor, kind, phase), Hint: owners(kind, id)})
		}
	}
	return s.ownershipPolicy(ctx)
}

func (s *run) ownershipPolicy(ctx context.Context) error {
	actor, phase := s.req.Actor, s.req.Phase
	manifests := changesManifests(s.changed)
	if phase == InitialPhase && actor == HealthJudge && !manifests {
		s.add(Ownership, Diagnostic{Rule: RuleInitialNeedsDetectors, Path: layout.DetectorsDir, Message: "the initial health-judge proposal must install detector manifests"})
	}
	if phase == InitialPhase && actor == Deployer && containsPath(s.changed, layout.ArchitectureFile) && len(s.changed) != 1 {
		s.add(Ownership, Diagnostic{Rule: RuleArchitectureAlone, Path: layout.ArchitectureFile, Message: "initial architecture must follow a separately accepted source proposal", Hint: "propose arch.md alone, after the source commit is accepted"})
	}
	needsBase := (actor == Upkeep && changesDetectors(s.changed)) || ((actor == Responder || actor == HealthJudge) && manifests)
	if !needsBase {
		return nil
	}
	base, err := exportBase(ctx, s.req.Root, s.req.Base)
	if err != nil {
		return err
	}
	defer os.RemoveAll(base)
	if actor == Upkeep {
		return s.upkeepMigration(base)
	}
	return s.detectorOwnership(base)
}

func changesManifests(paths []string) bool {
	for _, p := range paths {
		if kind, _, ok := layout.Classify(p); ok && (kind.IsDetectorManifest() || kind == layout.KindLegacyDetectorManifest) {
			return true
		}
	}
	return false
}

// candidateManifests are the candidate's registrations by detector ID. A
// manifest that failed to parse is present with a nil value.
func (s *run) candidateManifests() map[string]*detector {
	byID := map[string]*detector{}
	for _, det := range s.detectors {
		if det.hasManifest {
			byID[det.id] = det
		}
	}
	return byID
}

// upkeepMigration lets Upkeep touch detectors only as part of a schema
// migration: the same change must stamp the new schema version, and the
// accepted detector registrations must survive it unchanged.
func (s *run) upkeepMigration(base string) error {
	fail := func(message string) {
		s.add(Ownership, Diagnostic{Rule: RuleUpkeepMigration, Path: layout.DetectorsDir, Message: message})
	}
	if !containsPath(s.changed, layout.SchemaVersionFile) {
		fail("upkeep may change detectors only together with a schema-version migration")
		return nil
	}
	if _, err := os.Stat(filepath.Join(base, layout.LegacyDetectorManifestFile)); err != nil {
		fail("upkeep detector migration needs a legacy detector manifest in the accepted baseline")
		return nil
	}
	legacy, err := layout.LegacyDetectors(base)
	if err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	after := s.candidateManifests()
	if len(legacy) != len(after) {
		fail(fmt.Sprintf("migration changed the detector count from %d to %d", len(legacy), len(after)))
		return nil
	}
	for _, prior := range legacy {
		current, ok := after[prior.ID]
		if !ok || (current.manifest != nil && !reflect.DeepEqual(prior, *current.manifest)) {
			fail(fmt.Sprintf("migration changed detector %s", prior.ID))
		}
	}
	return nil
}

// detectorOwnership compares the candidate's registrations with the accepted
// baseline: each actor may add or change only detectors of its own class, and
// responders may not rewrite provenance.
func (s *run) detectorOwnership(base string) error {
	actor := s.req.Actor
	hadDetectors, err := layout.HasDetectors(base)
	if err != nil {
		return err
	}
	after := s.candidateManifests()
	deny := func(rule, p, message string) {
		s.add(Ownership, Diagnostic{Rule: rule, Path: p, Message: message})
	}
	if s.req.Phase == InitialPhase && actor == HealthJudge && !hadDetectors {
		for _, det := range after {
			if det.class != layout.ClassHealth {
				deny(RuleInitialHealthOnly, det.manifestAt, fmt.Sprintf("initial health judge cannot add %s detector %s", det.class, det.id))
			}
		}
		return nil
	}
	before, err := layout.ReadDetectors(filepath.Join(base, layout.DiagnosticsDir))
	if err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	protected := layout.ClassHealth
	if actor == HealthJudge {
		protected = layout.ClassIncident
	}
	old := map[string]layout.DetectorManifest{}
	for _, det := range before {
		old[det.Manifest.ID] = det.Manifest
	}
	ids := make([]string, 0, len(after))
	for id := range after {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		det := after[id]
		prior, existed := old[id]
		if det.manifest == nil {
			continue
		}
		current := *det.manifest
		switch {
		case existed && prior.DetectorClass() == protected && !reflect.DeepEqual(prior, current):
			deny(RuleProtectedDetector, det.manifestAt, fmt.Sprintf("actor %s changed protected %s detector %s", actor, protected, id))
		case !existed && det.class == protected:
			deny(RuleProtectedDetector, det.manifestAt, fmt.Sprintf("actor %s added protected %s detector %s", actor, protected, id))
		}
		if existed && actor == Responder && prior.DetectorClass() == layout.ClassIncident && (prior.OriginatingIncident != current.OriginatingIncident || prior.OriginatingCommit != current.OriginatingCommit) {
			deny(RuleProvenanceRewrite, det.manifestAt, fmt.Sprintf("responder rewrote detector %s provenance", id))
		}
		if actor == Responder && det.class == layout.ClassIncident && (!existed || !reflect.DeepEqual(prior, current)) && current.Persistence.Firing != 1 {
			deny(RuleResponderFiring, det.manifestAt, fmt.Sprintf("new or changed incident detector %s must fire on first match (persistence.firing: 1)", id))
		}
	}
	for _, det := range before {
		if det.Class == protected && after[det.Manifest.ID] == nil {
			deny(RuleProtectedDetector, layout.DetectorManifestPath(det.Class, det.Manifest.ID), fmt.Sprintf("actor %s removed protected %s detector %s", actor, protected, det.Manifest.ID))
		}
	}
	return nil
}
