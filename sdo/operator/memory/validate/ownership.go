package validate

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/telemetry"
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
	return kind != layout.KindPlaybook || id == layout.HealthObjectivePlaybook
}

var deploymentKinds = []layout.Kind{layout.KindDeploymentPlan, layout.KindDeploymentFile, layout.KindDeploymentOverlay, layout.KindDeploymentPatch}

// deployerKinds are the deployment kinds the deployer owns: everything but the
// detection designer's overlay slot.
var deployerKinds = []layout.Kind{layout.KindDeploymentPlan, layout.KindDeploymentFile, layout.KindDeploymentPatch}

// telemetryKinds are the binding files under .sdo/diagnostics/telemetry.
var telemetryKinds = []layout.Kind{layout.KindTelemetrySources, layout.KindTelemetryBackends}

// grants is the whole ownership policy, kind by actor by phase.
//
// Phases: the human goal, Upkeep schema and architecture, trusted source deployment,
// and health-judge bootstrap stages are InitialPhase. An upstream advance
// (AdvancePhase) changes source only. Responders change
// operational memory only in ReflectionPhase, after an independently verified
// outcome. No agent edits app files in any phase: only the upstream actor
// owns source, and an agent's change to upstream source is a deployment
// patch. No actor edits memory in ProposalPhase except responders adding
// deployment patches (add-only, patchOwnership), and nobody but the
// controller writes outcomes or lifecycle provenance.
//
// Deployment class (.sdo/deployment): the human seeds all of it, the deployer
// owns the plan, files and patches (in the initial and advance phases, and may
// change or delete only patches whose header names it), the health judge owns
// the telemetry overlay slot, and responders only add patches. Kinds not granted here (an overlay for
// the deployer or a responder) are denied.
//
// Incident traffic: responders own the incident generator and incident-*
// workloads, and the health judge owns all other traffic. This is the broker's
// long-standing behavior and docs/architecture.md's ownership matrix. The
// judge and reflection both edit final files and use this shared ownership policy.
var grants = []grant{
	{actor: Human, phase: ImportPhase, kinds: []layout.Kind{
		layout.KindIncidentDetectorManifest, layout.KindIncidentDetectorSource,
		layout.KindPlaybookIndex, layout.KindPlaybook, layout.KindPlaybookScript,
		layout.KindPlaybookProgram, layout.KindLibrary, layout.KindCorpus,
	}, only: func(kind layout.Kind, id string) bool {
		switch kind {
		case layout.KindPlaybook, layout.KindPlaybookProgram:
			return id != layout.HealthObjectivePlaybook
		case layout.KindPlaybookScript:
			return !strings.HasPrefix(id, layout.HealthObjectivePlaybook+"/")
		}
		return true
	}},
	{actor: Human, phase: InitialPhase, kinds: deploymentKinds},
	{actor: Deployer, phase: InitialPhase, kinds: deployerKinds},
	// The deployment agent's advance route (plan, files, refreshed patches);
	// patchOwnership still limits it to patches it owns.
	{actor: Deployer, phase: AdvancePhase, kinds: deployerKinds},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindDeploymentOverlay}},
	{actor: Responder, phase: ProposalPhase, kinds: []layout.Kind{layout.KindDeploymentPatch}},
	// An upstream advance changes only source: an upstream revision that
	// touches .sdo/ is held before any proposal exists.
	{actor: Upstream, phase: AdvancePhase, kinds: []layout.Kind{kindSource}},

	{actor: Human, phase: InitialPhase, kinds: []layout.Kind{layout.KindGoal, layout.KindSources}},
	{actor: Upkeep, phase: InitialPhase, kinds: []layout.Kind{
		layout.KindArchitecture, layout.KindSchemaVersion, layout.KindDiagnosticsModule, layout.KindLegacyDetectorManifest,
		layout.KindHealthDetectorManifest, layout.KindIncidentDetectorManifest, layout.KindHealthDetectorSource, layout.KindIncidentDetectorSource,
	}},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindHealthDetectorManifest, layout.KindHealthDetectorSource}},
	// Telemetry bindings are the detection designer's alone; responders name
	// bound sources in detectors but a binding change needs a designer run.
	{actor: HealthJudge, phase: InitialPhase, kinds: telemetryKinds},
	// The designer edits arch.md only between the application-map markers;
	// architectureRegionOwnership enforces that, since a grant is per file.
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindArchitecture}},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindTrafficGenerator, layout.KindTrafficWorkload}, only: notIncidentTraffic},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindPlaybookIndex, layout.KindPlaybook}, only: healthObjectiveOnly},
	{actor: Responder, phase: ReflectionPhase, kinds: []layout.Kind{
		layout.KindIncidentDetectorManifest, layout.KindIncidentDetectorSource,
		layout.KindPlaybookIndex, layout.KindPlaybook, layout.KindPlaybookScript, layout.KindPlaybookProgram, layout.KindLibrary, layout.KindCorpus,
	}},
	{actor: Responder, phase: ReflectionPhase, kinds: []layout.Kind{layout.KindTrafficGenerator, layout.KindTrafficWorkload}, only: incidentTraffic},
}

func knownActor(a Actor) bool {
	switch a {
	case Human, Deployer, HealthJudge, Responder, Upkeep, Upstream:
		return true
	}
	return false
}

func knownPhase(p Phase) bool {
	switch p {
	case ProposalPhase, ReflectionPhase, InitialPhase, OutcomePhase, ImportPhase, AdvancePhase:
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

// sourceHint explains a denied app-file change. App files are never edited by
// SDO: a change to upstream source is a patch under .sdo/deployment/patches/.
func sourceHint(actor Actor, phase Phase) string {
	if actor == Upstream {
		return owners(kindSource, "")
	}
	return "app files are never edited or published; make the change a patch (`sdo-operator patch create --reason \"<why>\"`), or revert the file. Only the upstream advance changes app files"
}

func (s *run) ownership(ctx context.Context) error {
	if s.layers()&Ownership == 0 {
		return nil
	}
	actor, phase := s.req.Actor, s.req.Phase
	if !knownActor(actor) {
		s.add(Ownership, Diagnostic{Rule: RuleUnknownActor, Message: fmt.Sprintf("unknown actor %q", actor), Hint: "actors are human, deployer, health_judge, responder, upkeep, and upstream"})
		return nil
	}
	if diagnostics := PhaseDiagnostics(phase); len(diagnostics) > 0 {
		s.diags = append(s.diags, diagnostics...)
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
			if kind, id, ok = s.req.classify(p); !ok {
				s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s cannot change an unclassified path", actor), Hint: "only paths in the operational-memory layout can be changed"})
				continue
			}
		}
		if slices.Contains(telemetryKinds, kind) && s.req.Telemetry.EffectiveMode() != telemetry.ModeOpen && allowed(actor, phase, kind, id) {
			s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("the environment's telemetry mode is %s: nobody changes telemetry bindings", s.req.Telemetry.EffectiveMode()), Hint: "the environment fixed the sources; query the sources it provides"})
			continue
		}
		if !allowed(actor, phase, kind, id) {
			hint := owners(kind, id)
			if kind == kindSource {
				hint = sourceHint(actor, phase)
			}
			s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s may not change %s files in the %s phase", actor, kind, phase), Hint: hint})
		}
	}
	return s.ownershipPolicy(ctx)
}

func (s *run) ownershipPolicy(ctx context.Context) error {
	actor, phase := s.req.Actor, s.req.Phase
	if containsPath(s.changed, layout.ArchitectureFile) {
		if err := s.architectureRegionOwnership(ctx); err != nil {
			return err
		}
	}
	if err := s.patchOwnership(ctx); err != nil {
		return err
	}
	manifests := changesManifests(s.changed)
	if phase == InitialPhase && actor == HealthJudge && len(s.changed) > 0 && !healthRevalidation(s.req, s.changed) {
		// An empty diff is reported once, by diff.empty; this rule covers a
		// diff whose resulting tree lacks a declared probe's traffic detector.
		// Go's detectors count as present before it has written them, so the
		// tree as Go will publish it decides, not the diff.
		if missing := s.missingInitialDetectors(); len(missing) > 0 {
			hint := "run `sdo-operator memory check` in the checkout: it installs Go's generic detectors (services, workloads, traffic-<workload>), which must stay in the tree; do not delete them"
			message := "the resulting tree lacks the traffic detectors for the probe workloads the application map declares: " + strings.Join(missing, ", ")
			s.add(Ownership, Diagnostic{Rule: RuleInitialNeedsDetectors, Path: layout.DetectorsDir, Message: message, Hint: hint})
		}
	}
	needsBase := (actor == Upkeep && changesDetectors(s.changed)) || (actor == Responder || actor == HealthJudge || actor == Human && phase == ImportPhase) && manifests
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

// patchOwnership applies the owner rule to patches the base already holds.
// Responders are add-only: a patch is repair history bound to an incident, so a
// later pass may supersede one with a later-numbered patch but not rewrite or
// delete it. The deployer may change or delete only patches whose header in the
// base names it; a base patch whose owner cannot be read is not provably its
// own. The human seed is unrestricted.
func (s *run) patchOwnership(ctx context.Context) error {
	actor, phase := s.req.Actor, s.req.Phase
	responder := actor == Responder && phase == ProposalPhase
	deployer := actor == Deployer && (phase == InitialPhase || phase == AdvancePhase)
	if !responder && !deployer {
		return nil
	}
	for _, p := range s.changed {
		if kind, _, ok := layout.Classify(p); !ok || kind != layout.KindDeploymentPatch {
			continue
		}
		listing, err := git(ctx, s.req.Root, "ls-tree", "--name-only", "-z", s.req.Base, "--", p)
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(listing)) == 0 {
			continue
		}
		if responder {
			s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: "responders may add deployment patches but not modify or delete an accepted one", Hint: "add a new, later-numbered patch that supersedes it"})
			continue
		}
		data, err := git(ctx, s.req.Root, "show", s.req.Base+":"+p)
		if err != nil {
			return err
		}
		parsed, perr := patchset.Parse(path.Base(p), data)
		switch {
		case perr != nil:
			s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: "the accepted patch has no readable owner header, so the deployer cannot show it owns it: " + perr.Error(), Hint: "add a new, later-numbered patch instead of changing this one"})
		case parsed.Header.Owner != patchset.OwnerDeployer:
			s.add(Ownership, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("the deployer may change or delete only its own patches; this one is owned by %s", parsed.Header.Owner), Hint: "add a new, later-numbered patch that supersedes it"})
		}
	}
	return nil
}

func changesManifests(paths []string) bool {
	for _, p := range paths {
		if kind, _, ok := layout.Classify(p); ok && (kind.IsDetectorManifest() || kind == layout.KindLegacyDetectorManifest) {
			return true
		}
	}
	return false
}

// candidateManifests are the candidate's registrations by manifest path. Keep
// both classes when their IDs collide: the semantic layer rejects duplicate
// IDs, but ownership must still compare every registration with its own base.
// A manifest that failed to parse is present with a nil detector.manifest.
func (s *run) candidateManifests() map[string]*detector {
	byPath := map[string]*detector{}
	for _, det := range s.sortedDetectors() {
		if det.hasManifest {
			byPath[det.manifestAt] = det
		}
	}
	return byPath
}

// upkeepMigration lets Upkeep touch detectors only as part of a schema
// migration: the same change must stamp the new schema version, and the
// accepted detector registrations must survive it unchanged, modulo the
// normalization of playbook references to logical IDs.
func (s *run) upkeepMigration(base string) error {
	fail := func(message string) {
		s.add(Ownership, Diagnostic{Rule: RuleUpkeepMigration, Path: layout.DetectorsDir, Message: message})
	}
	if !containsPath(s.changed, layout.SchemaVersionFile) {
		fail("upkeep may change detectors only together with a schema-version migration")
		return nil
	}
	// Recompute the registered transformation from the independently exported
	// accepted tree. A schema stamp is not authority to change detector behavior,
	// including when v1 packages move to their canonical ID directories.
	changed, err := layout.Migrate(base)
	if err != nil {
		return fmt.Errorf("plan accepted migration: %w", err)
	}
	if !changed {
		fail("upkeep requires an older accepted schema")
		return nil
	}
	expected, err := migrationDetectorFiles(base)
	if err != nil {
		return err
	}
	actual, err := migrationDetectorFiles(s.req.Root)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, actual) {
		fail("migration may change detector source only by the registered schema transformation")
	}
	legacy, err := layout.AcceptedDetectors(base)
	if err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	after := s.candidateManifests()
	if len(legacy) != len(after) {
		fail(fmt.Sprintf("migration changed the detector count from %d to %d", len(legacy), len(after)))
		return nil
	}
	for _, prior := range legacy {
		current, ok := after[layout.DetectorManifestPath(prior.DetectorClass(), prior.ID)]
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
		old[layout.DetectorManifestPath(det.Class, det.Manifest.ID)] = det.Manifest
	}
	paths := make([]string, 0, len(after))
	for p := range after {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		det := after[p]
		prior, existed := old[p]
		if det.manifest == nil {
			continue
		}
		current := *det.manifest
		switch {
		case existed && prior.DetectorClass() == protected && !reflect.DeepEqual(prior, current):
			deny(RuleProtectedDetector, det.manifestAt, fmt.Sprintf("actor %s changed protected %s detector %s", actor, protected, det.id))
		case !existed && det.class == protected:
			deny(RuleProtectedDetector, det.manifestAt, fmt.Sprintf("actor %s added protected %s detector %s", actor, protected, det.id))
		}
		if existed && (actor == Responder || actor == Human) && prior.DetectorClass() == layout.ClassIncident && (prior.OriginatingIncident != current.OriginatingIncident || prior.OriginatingCommit != current.OriginatingCommit) {
			deny(RuleProvenanceRewrite, det.manifestAt, fmt.Sprintf("responder rewrote detector %s provenance", det.id))
		}
		if (actor == Responder || actor == Human) && det.class == layout.ClassIncident && (!existed || !reflect.DeepEqual(prior, current)) && current.Persistence.Firing != 1 {
			deny(RuleResponderFiring, det.manifestAt, fmt.Sprintf("new or changed incident detector %s must fire on first match (persistence.firing: 1)", det.id))
		}
	}
	for _, det := range before {
		if det.Class == protected && after[layout.DetectorManifestPath(det.Class, det.Manifest.ID)] == nil {
			deny(RuleProtectedDetector, layout.DetectorManifestPath(det.Class, det.Manifest.ID), fmt.Sprintf("actor %s removed protected %s detector %s", actor, protected, det.Manifest.ID))
		}
	}
	return nil
}

func migrationDetectorFiles(root string) (map[string]string, error) {
	files := map[string]string{}
	base := filepath.Join(root, filepath.FromSlash(layout.DetectorsDir))
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = string(data)
		return nil
	})
	return files, err
}

// missingInitialDetectors evaluates the resulting tree (base plus diff) as Go
// will publish it: every detector the tree holds or Go prepares for it
// (memory.PreparedHealthDetectorIDs, the set PrepareHealthWith installs: the
// generic health detectors and traffic-<workload> per probe workload) counts
// as present. It returns the traffic detectors of the probe workloads the
// authored application map declares that neither the tree nor Go provides.
func (s *run) missingInitialDetectors() []string {
	have := map[string]bool{}
	for _, d := range s.detectors {
		if d.class == layout.ClassHealth && d.hasManifest {
			have[d.id] = true
		}
	}
	files := map[string][]byte{}
	for name, workload := range s.workloads {
		files[name+".yaml"] = workload.content
	}
	for _, id := range memory.PreparedHealthDetectorIDs(memory.HealthOptions{}, files) {
		have[id] = true
	}
	var missing []string
	for _, workload := range s.declaredProbeWorkloads() {
		if id := "traffic-" + workload; !have[id] {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)
	return missing
}

// declaredProbeWorkloads names the probe workloads, present in the tree, that
// the authored application map's probes live in.
func (s *run) declaredProbeWorkloads() []string {
	if s.arch == nil {
		return nil
	}
	region, found, err := appmap.Find(s.arch.doc.Source)
	if !found || err != nil {
		return nil
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	for _, probe := range parsed.Probes {
		name := appmap.WorkloadForKind[probe.Kind]
		if _, present := s.workloads[name]; name != "" && present && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names
}
