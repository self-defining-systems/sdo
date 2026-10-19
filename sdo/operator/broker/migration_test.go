package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

const legacyDetectorIndex = `apiVersion: sdo.dev/v1alpha1
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
      - .sdo/playbooks/base/README.md
    originatingCommit: initial
  - id: cache
    package: ./detectors/incidents/cache
    constructor: New
    class: incident
    owner: responder
    watches:
      - apiVersion: v1
        kind: Pod
    interval: 30s
    persistence: {firing: 1, clearing: 1}
    batching: {severity: warn, debounce: "0"}
    possiblePlaybooks:
      - base
    originatingIncident: inc-1
    originatingCommit: abc
`

// legacyRepository is an accepted schema-version 1 repository: one shared
// detector manifest and detector packages named by package, not by ID.
func legacyRepository(t *testing.T) string {
	t.Helper()
	root := repository(t)
	for path, content := range map[string]string{
		".sdo/schema-version":            "1\n",
		".sdo/goal.md":                   "---\nschema_version: 1\nowner: human\n---\nGoal\n",
		".sdo/arch.md":                   "---\nschema_version: 1\n---\nArchitecture\n",
		".sdo/playbooks/README.md":       "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md":  "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>\n",
		".sdo/diagnostics/manifest.yaml": legacyDetectorIndex,
		".sdo/diagnostics/detectors/health/objective/detector.go": "package objective\n",
		".sdo/diagnostics/detectors/incidents/cache/detector.go":  "package cache\n",
	} {
		absolute := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "legacy memory")
	return root
}

func TestUpkeepMigratesLegacyRepositoryToPerDetectorManifests(t *testing.T) {
	ctx := context.Background()
	target := legacyRepository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "migrate-schema", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.WriteSchema(proposal.Path); err != nil {
		t.Fatal(err)
	}
	receipt, err := b.Publish(ctx, "migrate-schema", Upkeep, InitialPhase)
	if err != nil {
		t.Fatalf("Upkeep migration was rejected: %v", err)
	}
	if !contains(receipt.ChangedPaths, layout.LegacyDetectorManifestFile) || !contains(receipt.ChangedPaths, layout.SchemaVersionFile) {
		t.Fatalf("migration did not retire the shared manifest: %v", receipt.ChangedPaths)
	}
	tree, err := layout.ReadTree(target)
	if err != nil {
		t.Fatalf("accepted repository is not readable at the new schema: %v", err)
	}
	ids := []string{}
	for _, detector := range tree.Detectors {
		ids = append(ids, string(detector.Class)+"/"+detector.Manifest.ID)
	}
	if strings.Join(ids, ",") != "health/health,incident/cache" {
		t.Fatalf("migrated detectors = %v", ids)
	}
	if _, err := os.Stat(filepath.Join(target, layout.LegacyDetectorManifestFile)); !os.IsNotExist(err) {
		t.Fatalf("shared manifest survived migration: %v", err)
	}
	if again, err := layout.Migrate(target); err != nil || again {
		t.Fatalf("accepted repository needs another migration: %v, %v", again, err)
	}
}

func TestUpkeepCannotEditDetectorsOutsideSchemaMigration(t *testing.T) {
	ctx := context.Background()
	target := legacyRepository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "sneaky", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layout.Migrate(proposal.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(proposal.Path, layout.SchemaVersionFile)); err != nil {
		t.Fatal(err)
	}
	git(t, proposal.Path, "checkout", "--", layout.SchemaVersionFile)
	if _, err := b.Publish(ctx, "sneaky", Upkeep, InitialPhase); err == nil {
		t.Fatal("upkeep changed detectors without stamping a schema migration")
	}
}

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
		err := checkOwnership(test.path, test.actor, test.phase)
		if (err == nil) != test.ok {
			t.Errorf("checkOwnership(%s, %s, %s) = %v, want ok=%v", test.path, test.actor, test.phase, err, test.ok)
		}
	}
}
