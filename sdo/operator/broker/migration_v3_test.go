package broker

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// pathReferenceRepository is an accepted schema-version 2 repository whose
// detector refers to its playbook by physical path, in the manifest and source.
func pathReferenceRepository(t *testing.T) string {
	t.Helper()
	root := repository(t)
	for name, content := range map[string]string{
		".sdo/schema-version":           "2\n",
		".sdo/goal.md":                  "---\nschema_version: 1\nowner: human\n---\nGoal\n",
		".sdo/arch.md":                  "---\nschema_version: 1\n---\nArchitecture\n",
		".sdo/playbooks/README.md":      "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md": "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>\n",
	} {
		absolute := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	err := layout.WriteDetector(root, layout.Detector{
		Class: layout.ClassHealth,
		Files: []layout.File{{Name: "detector.go", Content: []byte("package objective\n\nconst playbook = \".sdo/playbooks/base/README.md\"\n")}},
		Manifest: layout.DetectorManifest{
			APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion,
			ID: "health", Constructor: "New", Class: "health", Owner: "health_judge",
			Watches: []layout.Watch{{APIVersion: "v1", Kind: "Pod"}}, Interval: "30s",
			Persistence: layout.Persistence{Firing: 1, Clearing: 1}, Batching: layout.Batching{Severity: "critical", Debounce: "500ms"},
			PossiblePlaybooks: []string{".sdo/playbooks/base/README.md"}, OriginatingCommit: "initial",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "version 2 memory")
	return root
}

func migrationProposal(t *testing.T, target string) (*Broker, Proposal) {
	t.Helper()
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(context.Background(), "migrate-schema", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.WriteSchema(proposal.Path); err != nil {
		t.Fatal(err)
	}
	return b, proposal
}

func TestUpkeepMigratesPlaybookPathsToLogicalIDs(t *testing.T) {
	target := pathReferenceRepository(t)
	b, _ := migrationProposal(t, target)
	receipt, err := b.Publish(context.Background(), "migrate-schema", Upkeep, InitialPhase)
	if err != nil {
		t.Fatalf("Upkeep migration was rejected: %v", err)
	}
	source := path.Join(layout.DetectorDir(layout.ClassHealth, "health"), layout.DetectorFileName)
	if !slices.Contains(receipt.ChangedPaths, source) {
		t.Fatalf("migration did not rewrite detector source: %v", receipt.ChangedPaths)
	}
	tree, err := layout.ReadTree(target)
	if err != nil {
		t.Fatalf("accepted repository is not readable at the new schema: %v", err)
	}
	if got := tree.Detectors[0].Manifest.PossiblePlaybooks; len(got) != 1 || got[0] != "base" {
		t.Fatalf("possiblePlaybooks = %v", got)
	}
}

func TestUpkeepMigrationCannotChangeDetectorsBeyondPlaybookReferences(t *testing.T) {
	target := pathReferenceRepository(t)
	b, proposal := migrationProposal(t, target)
	manifestPath := filepath.Join(proposal.Path, layout.DetectorManifestPath(layout.ClassHealth, "health"))
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte(strings.Replace(string(data), "interval: 30s", "interval: 5s", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "migrate-schema", Upkeep, InitialPhase); err == nil {
		t.Fatal("upkeep migration changed a detector registration")
	}
}
