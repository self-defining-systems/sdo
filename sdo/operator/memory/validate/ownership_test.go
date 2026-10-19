package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

const (
	cacheManifest  = ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml"
	cacheDetector  = ".sdo/diagnostics/detectors/incidents/cache/detector.go"
	healthManifest = ".sdo/diagnostics/detectors/health/health/manifest.yaml"
	healthDetector = ".sdo/diagnostics/detectors/health/health/detector.go"
)

func edit(t *testing.T, root, rel, from, to string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), from) {
		t.Fatalf("%s does not contain %q", rel, from)
	}
	write(t, root, rel, strings.Replace(string(data), from, to, 1))
}

func incidentManifest(id string, firing string, origin string) string {
	return "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: " + id + "\nconstructor: New\nclass: incident\nowner: responder\nwatches:\n  - apiVersion: v1\n    kind: Pod\ninterval: 30s\npersistence:\n  firing: " + firing + "\n  clearing: 1\nbatching:\n  severity: warn\n  debounce: \"0\"\npossiblePlaybooks:\n  - base\noriginatingIncident: " + origin + "\noriginatingCommit: abc\n"
}

// TestOwnershipTable pins the kind x actor x phase policy. Each case edits a
// committed good tree and names the exact rules the diff must trigger.
func TestOwnershipTable(t *testing.T) {
	denied := []string{RuleOwnershipDenied}
	for _, test := range []struct {
		name  string
		actor Actor
		phase Phase
		edit  func(t *testing.T, root string)
		want  []string
	}{
		{"responder edits incident detector source in reflection", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, cacheDetector, "package cache\n\n// tuned\n")
		}, nil},
		{"responder adds incident detector in reflection", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", incidentManifest("dns", "1", "inc-2"))
			write(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", "package dns\n")
		}, nil},
		{"responder edits playbook script and index in reflection", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/bash\ntrue\n")
			write(t, r, ".sdo/playbooks/README.md", "# Playbooks\n\n- [base](base/README.md)\n\nmore\n")
		}, nil},
		{"responder edits incident traffic in reflection", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/traffic/workloads/incident-x.yaml", "name: incident-x\n")
			write(t, r, ".sdo/diagnostics/traffic/generators/incident/gen.go", "package incident\n")
		}, nil},
		{"responder cannot edit health traffic", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/traffic/workloads/health.yaml", "name: health\n")
		}, denied},
		{"responder cannot edit health detector source", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, healthDetector, "package objective\n\n// edit\n")
		}, denied},
		{"responder cannot change health manifest registration", Responder, ReflectionPhase, func(t *testing.T, r string) {
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, []string{RuleOwnershipDenied, RuleProtectedDetector}},
		{"responder cannot remove a health detector", Responder, ReflectionPhase, func(t *testing.T, r string) {
			remove(t, r, ".sdo/diagnostics/detectors/health/health")
		}, []string{RuleOwnershipDenied, RuleProtectedDetector}},
		{"responder incident detector must fire on first match", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", incidentManifest("dns", "2", "inc-2"))
			write(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", "package dns\n")
		}, []string{RuleResponderFiring}},
		{"responder cannot rewrite detector provenance", Responder, ReflectionPhase, func(t *testing.T, r string) {
			edit(t, r, cacheManifest, "originatingIncident: inc-1", "originatingIncident: inc-9")
		}, []string{RuleProvenanceRewrite}},
		{"responder cannot edit goal", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")
		}, denied},
		{"responder cannot edit outcomes", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, layout.OutcomePath("inc"), "{}\n")
		}, denied},
		{"responder cannot edit source in reflection", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, "app.txt", "changed\n")
		}, denied},
		{"responder edits source in proposal", Responder, ProposalPhase, func(t *testing.T, r string) {
			write(t, r, "app.txt", "repair\n")
		}, nil},
		{"responder cannot edit memory in proposal", Responder, ProposalPhase, func(t *testing.T, r string) {
			write(t, r, cacheDetector, "package cache\n\n// early\n")
		}, denied},
		{"responder cannot publish initial memory", Responder, InitialPhase, func(t *testing.T, r string) {
			write(t, r, cacheDetector, "package cache\n\n// early\n")
		}, denied},
		{"health judge edits health detector in initial", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, healthDetector, "package objective\n\n// edit\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, nil},
		{"health judge cannot add incident detector", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", incidentManifest("dns", "1", "inc-2"))
			write(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", "package dns\n")
		}, []string{RuleOwnershipDenied, RuleProtectedDetector}},
		{"health judge cannot change incident detector", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			edit(t, r, cacheManifest, "interval: 30s", "interval: 20s")
		}, []string{RuleOwnershipDenied, RuleProtectedDetector}},
		{"health judge owns health traffic only", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/traffic/workloads/health.yaml", "name: health\n")
			write(t, r, ".sdo/diagnostics/detectors/health/health/detector.go", "package objective\n\n// x\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, nil},
		{"health judge cannot edit incident traffic", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/diagnostics/traffic/workloads/incident-x.yaml", "name: incident-x\n")
			write(t, r, healthDetector, "package objective\n\n// x\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, denied},
		{"health judge may write the objective playbook only", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/health-objective/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: i\n---\n<RESOURCE_NAME>\n")
			write(t, r, healthDetector, "package objective\n\n// x\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, nil},
		{"health judge cannot write other playbooks", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/other/README.md", "x\n")
			write(t, r, healthDetector, "package objective\n\n// x\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, denied},
		{"health judge cannot write objective playbook scripts", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/health-objective/scripts/x.sh", "true\n")
			write(t, r, healthDetector, "package objective\n\n// x\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, denied},
		{"health judge initial proposal must install manifests", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/playbooks/health-objective/README.md", "x\n")
		}, []string{RuleInitialNeedsDetectors}},
		{"health judge cannot edit source", HealthJudge, InitialPhase, func(t *testing.T, r string) {
			write(t, r, "app.txt", "x\n")
			write(t, r, healthDetector, "package objective\n\n// x\n")
			edit(t, r, healthManifest, "interval: 30s", "interval: 20s")
		}, denied},
		{"health judge cannot publish in reflection", HealthJudge, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, healthDetector, "package objective\n\n// x\n")
		}, denied},
		{"human edits goal in initial", Human, InitialPhase, func(t *testing.T, r string) {
			write(t, r, layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")
		}, nil},
		{"human cannot edit architecture", Human, InitialPhase, func(t *testing.T, r string) {
			write(t, r, layout.ArchitectureFile, "---\nschema_version: 1\n---\nx\n")
		}, denied},
		{"deployer edits source in initial", Deployer, InitialPhase, func(t *testing.T, r string) {
			write(t, r, "app.txt", "deployed\n")
		}, nil},
		{"deployer cannot edit source later", Deployer, ProposalPhase, func(t *testing.T, r string) {
			write(t, r, "app.txt", "deployed\n")
		}, denied},
		{"deployer architecture must stand alone", Deployer, InitialPhase, func(t *testing.T, r string) {
			write(t, r, layout.ArchitectureFile, "---\nschema_version: 1\ngenerated_at_commit: x\n---\nx\n")
			write(t, r, "app.txt", "deployed\n")
		}, []string{RuleArchitectureAlone}},
		{"deployer cannot edit detectors", Deployer, InitialPhase, func(t *testing.T, r string) {
			write(t, r, healthDetector, "package objective\n\n// x\n")
		}, denied},
		{"upkeep cannot change detectors outside a migration", Upkeep, InitialPhase, func(t *testing.T, r string) {
			write(t, r, cacheDetector, "package cache\n\n// x\n")
		}, []string{RuleUpkeepMigration}},
		{"upkeep stamps schema alone", Upkeep, InitialPhase, func(t *testing.T, r string) {
			write(t, r, layout.SchemaVersionFile, "3\n")
			write(t, r, layout.DiagnosticsGoModFile, "module m\n")
		}, nil},
		{"unknown actor", Actor("intern"), InitialPhase, func(t *testing.T, r string) {
			write(t, r, "app.txt", "x\n")
		}, []string{RuleUnknownActor}},
		{"unknown phase", Responder, Phase("whenever"), func(t *testing.T, r string) {
			write(t, r, "app.txt", "x\n")
		}, []string{RuleUnknownPhase}},
		{"unclassified memory path", Responder, ReflectionPhase, func(t *testing.T, r string) {
			write(t, r, ".sdo/notes.txt", "x\n")
		}, denied},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := goodTree(t)
			commit(t, root, "base")
			test.edit(t, root)
			diagnostics := diagnose(t, Request{Root: root, Base: "HEAD", Actor: test.actor, Phase: test.phase, Layers: Ownership})
			if got := ruleSet(diagnostics); !same(got, test.want) {
				t.Fatalf("rules = %v, want %v\n%v", got, test.want, diagnostics)
			}
		})
	}
}

func TestInitialHealthJudgeMayAddOnlyHealthDetectorsToEmptyMemory(t *testing.T) {
	root := goodTree(t)
	remove(t, root, ".sdo/diagnostics")
	commit(t, root, "memory without detectors")
	copyTree(t, filepath.Join("testdata", "good", ".sdo", "diagnostics"), filepath.Join(root, ".sdo", "diagnostics"))
	got := ruleSet(diagnose(t, Request{Root: root, Base: "HEAD", Actor: HealthJudge, Phase: InitialPhase, Layers: Ownership}))
	if !same(got, []string{RuleOwnershipDenied, RuleInitialHealthOnly}) {
		t.Fatalf("rules = %v", got)
	}
	remove(t, root, ".sdo/diagnostics/detectors/incidents")
	if got := diagnose(t, Request{Root: root, Base: "HEAD", Actor: HealthJudge, Phase: InitialPhase, Layers: Ownership}); len(got) != 0 {
		t.Fatalf("health detectors alone were rejected: %v", got)
	}
}

const legacyIndex = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
detectors:
  - id: health
    package: ./detectors/health/objective
    constructor: New
    class: health
    owner: health_judge
    watches:
      - apiVersion: v1
        kind: Pod
    interval: 30s
    persistence: {firing: 1, clearing: 1}
    batching: {severity: critical, debounce: 500ms}
    possiblePlaybooks:
      - base
    originatingCommit: initial
`

func legacyRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range map[string]string{
		".sdo/schema-version":            "1\n",
		".sdo/goal.md":                   "---\nschema_version: 1\nowner: human\n---\nGoal\n",
		".sdo/arch.md":                   "---\nschema_version: 1\n---\nArch\n",
		".sdo/playbooks/README.md":       "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md":  "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: i\n---\n<RESOURCE_NAME>\n",
		".sdo/diagnostics/manifest.yaml": legacyIndex,
		".sdo/diagnostics/detectors/health/objective/detector.go": "package objective\n",
	} {
		write(t, root, rel, content)
	}
	commit(t, root, "legacy memory")
	return root
}

func TestUpkeepMayMigrateLegacyDetectorsButNotRewriteThem(t *testing.T) {
	request := func(root string) Request {
		return Request{Root: root, Base: "HEAD", Actor: Upkeep, Phase: InitialPhase, Layers: Ownership}
	}
	root := legacyRepository(t)
	if _, err := layout.Migrate(root); err != nil {
		t.Fatal(err)
	}
	if got := diagnose(t, request(root)); len(got) != 0 {
		t.Fatalf("faithful migration rejected: %v", got)
	}
	edit(t, root, healthManifest, "interval: 30s", "interval: 5s")
	if got := ruleSet(diagnose(t, request(root))); !same(got, []string{RuleUpkeepMigration}) {
		t.Fatalf("rewriting migration: rules = %v", got)
	}
	// A schema stamp does not license edits to accepted detector source.
	current := goodTree(t)
	commit(t, current, "base")
	write(t, current, cacheDetector, "package cache\n\n// x\n")
	write(t, current, layout.SchemaVersionFile, "3\n\n")
	if got := ruleSet(diagnose(t, request(current))); !same(got, []string{RuleUpkeepMigration}) {
		t.Fatalf("detector source edit during a schema stamp: rules = %v", got)
	}
}

func TestInitialArchitectureMustCiteTheAcceptedSourceCommit(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\nGoal\n")
	commit(t, root, "human goal")
	write(t, root, "app.txt", "deployed\n")
	gitIn(t, root, "add", "-A")
	gitIn(t, root, "commit", "-q", "-m", "sdo: validated\n\nSDO-Actor: deployer\nSDO-Phase: initial\nSDO-Validation: passed")
	source := gitIn(t, root, "rev-parse", "HEAD")
	request := Request{Root: root, Base: "HEAD", Actor: Deployer, Phase: InitialPhase, Layers: Semantic}

	write(t, root, layout.ArchitectureFile, "---\nschema_version: 1\ngenerated_at_commit: "+source+"\n---\nArch\n")
	if got := diagnose(t, request); len(got) != 0 {
		t.Fatalf("architecture citing the accepted source was rejected: %v", got)
	}
	write(t, root, layout.ArchitectureFile, "---\nschema_version: 1\ngenerated_at_commit: deadbeef\n---\nArch\n")
	if got := ruleSet(diagnose(t, request)); !same(got, []string{RuleArchitectureCommit}) {
		t.Fatalf("wrong commit: rules = %v", got)
	}

	// A base that is not a deployer source commit cannot anchor architecture.
	gitIn(t, root, "checkout", "-q", "--detach", "HEAD~1")
	if got := ruleSet(diagnose(t, request)); !same(got, []string{RuleArchitectureBaseline}) {
		t.Fatalf("unaccepted base: rules = %v", got)
	}
}

func TestChangedPathsReportsTheActualDiff(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, "new.txt", "untracked\n")
	remove(t, root, ".sdo/goal.md")
	write(t, root, "app.txt", "modified\n")
	gitIn(t, root, "add", "app.txt")
	paths, err := ChangedPaths(context.Background(), root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(paths, ",") != ".sdo/goal.md,app.txt,new.txt" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestEveryKindIsOwnedByEveryoneOrNoAgent(t *testing.T) {
	// Outcomes, lifecycle provenance, and the retired outcome stream belong to
	// the controller: no actor may change them in any phase.
	for _, kind := range []layout.Kind{layout.KindOutcome, layout.KindLifecycleProvenance, layout.KindLegacyOutcomes} {
		for _, actor := range []Actor{Human, Deployer, HealthJudge, Responder, Upkeep} {
			for _, phase := range []Phase{ProposalPhase, ReflectionPhase, InitialPhase, OutcomePhase} {
				if allowed(actor, phase, kind, "") {
					t.Errorf("%s may change %s in %s", actor, kind, phase)
				}
			}
		}
	}
}

// Ported from the broker's path-ownership test: the same paths, actors, and
// phases must keep their verdicts.
func TestOwnershipFollowsMemoryKindAndActor(t *testing.T) {
	health := layout.DetectorManifestPath(layout.ClassHealth, "h")
	incident := layout.DetectorManifestPath(layout.ClassIncident, "i")
	for _, test := range []struct {
		path  string
		actor Actor
		phase Phase
		ok    bool
	}{
		{health, HealthJudge, InitialPhase, true},
		{health, Responder, ReflectionPhase, false},
		{incident, Responder, ReflectionPhase, true},
		{incident, HealthJudge, InitialPhase, false},
		{layout.LegacyDetectorManifestFile, Responder, ReflectionPhase, false},
		{layout.LegacyDetectorManifestFile, HealthJudge, InitialPhase, false},
		{layout.LegacyDetectorManifestFile, Upkeep, InitialPhase, true},
		{layout.DetectorDir(layout.ClassIncident, "i") + "/detector.go", Responder, ReflectionPhase, true},
		{layout.DetectorDir(layout.ClassHealth, "h") + "/detector.go", Responder, ReflectionPhase, false},
		{".sdo/diagnostics/traffic/workloads/incident-x.yaml", Responder, ReflectionPhase, true},
		{".sdo/diagnostics/traffic/workloads/health.yaml", Responder, ReflectionPhase, false},
		{".sdo/diagnostics/traffic/workloads/health.yaml", HealthJudge, InitialPhase, true},
		{".sdo/playbooks/p/scripts/fix.sh", Responder, ReflectionPhase, true},
		{".sdo/playbooks/p/scripts/fix.sh", HealthJudge, InitialPhase, false},
		{".sdo/playbooks/health-objective/README.md", HealthJudge, InitialPhase, true},
		{".sdo/playbooks/other/README.md", HealthJudge, InitialPhase, false},
		{layout.GoalFile, Human, InitialPhase, true},
		{layout.GoalFile, Deployer, InitialPhase, false},
		{layout.SchemaVersionFile, Upkeep, InitialPhase, true},
		{".sdo/unclassified.txt", Responder, ReflectionPhase, false},
	} {
		kind, id, classified := layout.Classify(test.path)
		if got := classified && allowed(test.actor, test.phase, kind, id); got != test.ok {
			t.Errorf("%s may change %s in %s = %v, want %v", test.actor, test.path, test.phase, got, test.ok)
		}
	}
}
