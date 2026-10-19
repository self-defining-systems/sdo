package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory/layout"
)

const schemaOneManifest = `apiVersion: sdo.dev/v1alpha1
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

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// schemaOneRepository is an accepted, already-bootstrapped repository written
// before per-detector manifests existed.
func schemaOneRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "accepted")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	gitIn(t, root, "init", "-q")
	gitIn(t, root, "config", "user.name", "Test")
	gitIn(t, root, "config", "user.email", "test@example.com")
	for path, content := range map[string]string{
		"app.txt":                        "base\n",
		".sdo/schema-version":            "1\n",
		".sdo/goal.md":                   "---\nschema_version: 1\nowner: human\n---\nGoal\n",
		".sdo/arch.md":                   "---\nschema_version: 1\n---\nArchitecture\n",
		".sdo/playbooks/README.md":       "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md":  "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>\n",
		".sdo/diagnostics/manifest.yaml": schemaOneManifest,
		".sdo/diagnostics/detectors/health/objective/detector.go": "package objective\n",
	} {
		absolute := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitIn(t, root, "add", ".")
	gitIn(t, root, "commit", "-qm", "schema one")
	return root
}

func TestControllerStartupMigratesSchemaOneRepositoryAsUpkeep(t *testing.T) {
	ctx := context.Background()
	root := schemaOneRepository(t)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layout.ReadTree(root); err == nil {
		t.Fatal("schema-one repository was readable before migration")
	}
	before := gitIn(t, root, "rev-parse", "HEAD")
	projection, err := resolveAcceptedDiagnostics(ctx, root, repository)
	if err != nil {
		t.Fatalf("startup read after migration failed: %v", err)
	}
	if projection.Directory == "" || projection.AcceptedRevision == before {
		t.Fatalf("projection did not follow the accepted migration commit: %+v", projection)
	}
	receipt, ok, err := repository.AcceptedCommit(ctx, memoryMigrationOperationID()+"-schema", broker.InitialPhase)
	if err != nil || !ok || receipt.Commit != projection.AcceptedRevision {
		t.Fatalf("no accepted Upkeep migration commit: %+v %v %v", receipt, ok, err)
	}
	if got := gitIn(t, root, "rev-parse", "HEAD"); got != receipt.Commit {
		t.Fatalf("HEAD %s is not the migration commit %s", got, receipt.Commit)
	}
	if _, err := layout.ReadTree(root); err != nil {
		t.Fatalf("memory unreadable after migration: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "health"))); err != nil {
		t.Fatalf("per-detector manifest missing: %v", err)
	}

	// A restarted controller reuses the accepted migration.
	again, err := resolveAcceptedDiagnostics(ctx, root, repository)
	if err != nil {
		t.Fatal(err)
	}
	if again.AcceptedRevision != projection.AcceptedRevision || gitIn(t, root, "rev-parse", "HEAD") != receipt.Commit {
		t.Fatalf("restart created another commit: %+v", again)
	}
	fresh, err := broker.New(root, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolveAcceptedDiagnostics(ctx, root, fresh); err != nil || gitIn(t, root, "rev-parse", "HEAD") != receipt.Commit {
		t.Fatalf("restart with a new broker changed the repository: %v", err)
	}
}
