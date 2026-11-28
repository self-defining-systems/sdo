package validate

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
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

// telemetryKinds are the binding files under .sdo/diagnostics/telemetry.
var telemetryKinds = []layout.Kind{layout.KindTelemetrySources, layout.KindTelemetryBackends}

// grants is the whole ownership policy, kind by actor by phase.
//
// Phases: the human goal, Upkeep schema and architecture, trusted source deployment,
// and health-judge bootstrap stages are InitialPhase. An upstream advance
// (AdvancePhase) changes source only. Responders change
// operational memory only in ReflectionPhase, after an independently verified
// outcome. App source changes by commit: the deployer commits it in its
// initial proposal (published only through PublishVerifiedSource, bound to the
// deployed tree), a responder commits it in its proposal (published by the
// broker after the incident closes), and Go commits program source proposals
// in the reflection checkout as the responder. No actor edits memory in
// ProposalPhase, and nobody but the controller writes outcomes or lifecycle
// provenance.
//
// Deployment class (.sdo/deployment): only the optional override plan.yaml,
// written by the human or the deployer in the initial phase.
//
// Incident traffic: responders own the incident generator and incident-*
// workloads, and the health judge owns all other traffic. This is the broker's
// long-standing behavior and docs/architecture.md's ownership matrix. The
// judge and reflection both edit final files and use this shared ownership policy.
var grants = []grant{
	{actor: Human, phase: ImportPhase, kinds: []layout.Kind{
		layout.KindLearnedDetectorManifest, layout.KindLearnedDetectorSource,
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
	{actor: Human, phase: InitialPhase, kinds: []layout.Kind{layout.KindDeploymentPlan}},
	// The deployer commits app files like an engineer and may name the
	// deployment in the override.
	{actor: Deployer, phase: InitialPhase, kinds: []layout.Kind{layout.KindDeploymentPlan, kindSource}},
	// A responder commits application source; the broker publishes the
	// committed source after the incident closes.
	{actor: Responder, phase: ProposalPhase, kinds: []layout.Kind{kindSource}},
	// Go commits the source changes programs proposed in the reflection
	// checkout, as the responder.
	{actor: Responder, phase: ReflectionPhase, kinds: []layout.Kind{kindSource}},
	// An upstream advance changes only source: an upstream revision that
	// touches .sdo/ is held before any proposal exists.
	{actor: Upstream, phase: AdvancePhase, kinds: []layout.Kind{kindSource}},

	{actor: Human, phase: InitialPhase, kinds: []layout.Kind{layout.KindGoal, layout.KindSources}},
	{actor: Upkeep, phase: InitialPhase, kinds: []layout.Kind{
		layout.KindArchitecture, layout.KindSchemaVersion, layout.KindDiagnosticsModule, layout.KindLegacyDetectorManifest, layout.KindLegacyDetectorFile,
		layout.KindHealthDetectorManifest, layout.KindLearnedDetectorManifest, layout.KindHealthDetectorSource, layout.KindLearnedDetectorSource,
	}},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindHealthDetectorManifest, layout.KindHealthDetectorSource}},
	// Telemetry bindings are the detection designer's alone; responders name
	// bound sources in detectors but a binding change needs a designer run.
	{actor: HealthJudge, phase: InitialPhase, kinds: telemetryKinds},
	// Data checks are the detection designer's alone, like health detectors.
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindDataCheck}},
	// The designer edits arch.md only between the application-map markers;
	// architectureRegionOwnership enforces that, since a grant is per file.
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindArchitecture}},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindTrafficGenerator, layout.KindTrafficWorkload}, only: notIncidentTraffic},
	{actor: HealthJudge, phase: InitialPhase, kinds: []layout.Kind{layout.KindPlaybookIndex, layout.KindPlaybook}, only: healthObjectiveOnly},
	{actor: Responder, phase: ReflectionPhase, kinds: []layout.Kind{
		layout.KindLearnedDetectorManifest, layout.KindLearnedDetectorSource,
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

// sourceHint explains a denied app-file change.
func sourceHint(actor Actor, phase Phase) string {
	if actor == Upstream {
		return owners(kindSource, "")
	}
	return "only the deployer's and a responder's committed source and the upstream advance change app files; the detection designer and other actors never do"
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
	// A change no grant covers is denied, except that Upkeep's is held back:
	// the registered schema migration's own rewrites are Upkeep's whatever
	// artifact they touch, and upkeepMigration admits exactly those.
	var migration []Diagnostic
	deny := func(d Diagnostic) {
		if actor == Upkeep {
			migration = append(migration, d)
			return
		}
		s.add(Ownership, d)
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
				deny(Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s cannot change an unclassified path", actor), Hint: "only paths in the operational-memory layout can be changed"})
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
			deny(Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s may not change %s files in the %s phase", actor, kind, phase), Hint: hint})
		}
	}
	return s.ownershipPolicy(ctx, migration)
}

// ownershipPolicy runs the rules a per-file grant cannot express. migration
// holds Upkeep's changes outside its grants, each admitted only when it is
// the registered schema migration's own rewrite.
func (s *run) ownershipPolicy(ctx context.Context, migration []Diagnostic) error {
	actor, phase := s.req.Actor, s.req.Phase
	if containsPath(s.changed, layout.ArchitectureFile) {
		if err := s.architectureRegionOwnership(ctx); err != nil {
			return err
		}
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
	needsBase := (actor == Upkeep && (changesDetectors(s.changed) || len(migration) > 0)) || (actor == Responder || actor == HealthJudge || actor == Human && phase == ImportPhase) && manifests
	if !needsBase {
		return nil
	}
	export := exportBase
	if actor == Upkeep && len(migration) > 0 {
		// The migration reads and rewrites application files too (a retired
		// patch's targets, the moved views), so it runs on the whole base.
		export = exportCheckout
	}
	base, err := export(ctx, s.req.Root, s.req.Base)
	if err != nil {
		return err
	}
	defer os.RemoveAll(base)
	if actor == Upkeep {
		return s.upkeepMigration(base, migration)
	}
	return s.detectorOwnership(base)
}

func changesManifests(paths []string) bool {
	for _, p := range paths {
		if kind, _, ok := layout.Classify(p); ok && (kind.IsDetectorManifest() || kind == layout.KindLegacyDetectorManifest || kind == layout.KindLegacyDetectorFile) {
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

// upkeepMigration admits Upkeep's schema migration: the same change must stamp
// the new schema version, the accepted detector registrations must survive it
// unchanged, modulo the normalization of playbook references to logical IDs,
// and Upkeep's changes outside its grants (held in migration) must be exactly
// the registered transformation's rewrites of the accepted tree outside those
// grants: the same set of paths, each byte for byte.
func (s *run) upkeepMigration(base string, migration []Diagnostic) error {
	detectors := changesDetectors(s.changed)
	const held = "; Upkeep changes it only as the registered schema migration rewrites it"
	fail := func(message string) {
		if detectors {
			s.add(Ownership, Diagnostic{Rule: RuleUpkeepMigration, Path: layout.DetectorsDir, Message: message})
		}
		for _, d := range migration {
			d.Hint += held
			s.add(Ownership, d)
		}
	}
	if !containsPath(s.changed, layout.SchemaVersionFile) {
		fail("upkeep may change detectors only together with a schema-version migration")
		return nil
	}
	var before map[string]string
	if len(migration) > 0 {
		var err error
		if before, err = treeFingerprints(base); err != nil {
			return fmt.Errorf("read accepted tree: %w", err)
		}
	}
	// Recompute the registered transformation from the independently exported
	// accepted tree. A schema stamp is not authority to change detector behavior,
	// including when v1 packages move to their canonical ID directories.
	changed, err := layout.Migrate(base)
	if err == nil && changed {
		// The migration also drops Go-installed generic detectors Go no longer
		// plans (memory.WriteSchema); nothing else about a detector may change.
		err = memory.PruneStrandedGenericDetectors(base)
	}
	if err != nil {
		if len(migration) == 0 {
			return fmt.Errorf("plan accepted migration: %w", err)
		}
		fail("the registered schema migration refuses the accepted tree: " + err.Error())
		return nil
	}
	if !changed {
		fail("upkeep requires an older accepted schema")
		return nil
	}
	if len(migration) > 0 {
		if err := s.migrationRewrites(base, before, migration, held); err != nil {
			return err
		}
	}
	if !detectors {
		return nil
	}
	fail = func(message string) {
		s.add(Ownership, Diagnostic{Rule: RuleUpkeepMigration, Path: layout.DetectorsDir, Message: message})
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

// migrationRewrites compares Upkeep's held changes with the migration's own
// rewrites outside Upkeep's grants (base is the migrated accepted tree, before
// its fingerprints from ahead of the migration): a held change the migration
// does not make, or makes differently, is denied, and a rewrite the change
// leaves out is a partial migration.
func (s *run) migrationRewrites(base string, before map[string]string, migration []Diagnostic, held string) error {
	after, err := treeFingerprints(base)
	if err != nil {
		return fmt.Errorf("read migrated tree: %w", err)
	}
	expected := map[string]bool{}
	for _, paths := range []map[string]string{before, after} {
		for p := range paths {
			if before[p] != after[p] && !s.upkeepGranted(p) {
				expected[p] = true
			}
		}
	}
	for _, d := range migration {
		want, wantOK, err := fileFingerprint(base, d.Path)
		if err != nil {
			return err
		}
		got, gotOK, err := fileFingerprint(s.req.Root, d.Path)
		if err != nil {
			return err
		}
		if !expected[d.Path] || want != got || wantOK != gotOK {
			d.Hint += held
			s.add(Ownership, d)
		}
		delete(expected, d.Path)
	}
	left := make([]string, 0, len(expected))
	for p := range expected {
		left = append(left, p)
	}
	sort.Strings(left)
	for _, p := range left {
		s.add(Ownership, Diagnostic{Rule: RuleUpkeepMigration, Path: p, Message: "the registered schema migration rewrites this file, and the change leaves it as accepted", Hint: "commit the whole migration (`sdo-operator memory migrate`), not part of it"})
	}
	return nil
}

// upkeepGranted reports whether a grant lets Upkeep change p in this phase.
func (s *run) upkeepGranted(p string) bool {
	kind, id := kindSource, ""
	if layout.IsMemoryPath(p) {
		var ok bool
		if kind, id, ok = s.req.classify(p); !ok {
			return false
		}
	}
	return allowed(Upkeep, s.req.Phase, kind, id)
}

// treeFingerprints fingerprints every file below root by its
// slash-separated relative path.
func treeFingerprints(root string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(full string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, full)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		fingerprint, _, err := fileFingerprint(root, rel)
		out[rel] = fingerprint
		return err
	})
	return out, err
}

// fileFingerprint is rel's type, executable bit and content digest (a
// symlink's target), and whether it exists.
func fileFingerprint(root, rel string) (string, bool, error) {
	full := filepath.Join(root, filepath.FromSlash(rel))
	info, err := os.Lstat(full)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var content []byte
	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(full)
		if err != nil {
			return "", false, err
		}
		content = []byte(target)
	case info.Mode().IsRegular():
		if content, err = os.ReadFile(full); err != nil {
			return "", false, err
		}
	}
	sum := sha256.Sum256(content)
	return fmt.Sprintf("%v %o %x", info.Mode().Type(), info.Mode().Perm()&0o111, sum), true, nil
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
		protected = layout.ClassLearned
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
		if existed && (actor == Responder || actor == Human) && prior.DetectorClass() == layout.ClassLearned && (prior.OriginatingIncident != current.OriginatingIncident || prior.OriginatingCommit != current.OriginatingCommit) {
			deny(RuleProvenanceRewrite, det.manifestAt, fmt.Sprintf("responder rewrote detector %s provenance", det.id))
		}
		if (actor == Responder || actor == Human) && det.class == layout.ClassLearned && (!existed || !reflect.DeepEqual(prior, current)) && current.Persistence.Firing != 1 {
			deny(RuleResponderFiring, det.manifestAt, fmt.Sprintf("new or changed learned detector %s must fire on first match (persistence.firing: 1)", det.id))
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
