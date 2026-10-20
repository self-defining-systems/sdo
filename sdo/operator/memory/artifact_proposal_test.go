package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestMaterializeJudgeProposalOwnsPhysicalLayout(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\ndetectors: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	proposal := &contracts.OperationalArtifactProposal{
		Detectors: []*contracts.ProposedDetector{{Id: "health-objective", DetectorClass: "health", Owner: "health_judge", Constructor: "New", Watches: []*contracts.MemoryWatch{{ApiVersion: "v1", Kind: "Pod"}}, Interval: "30s", Firing: 2, Clearing: 2, Severity: "critical", Debounce: "500ms", PossiblePlaybookIds: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package objective\n"}, {Name: "detector_test.go", Content: "package objective\n"}}}},
		Playbooks: []*contracts.ProposedPlaybook{{Id: "health-objective", Owner: "responder", FaultClass: "health-objective-failure", OriginatingIncident: "lifecycle-bootstrap", Markdown: "# Restore <AFFECTED_RESOURCE>\n"}},
	}
	if err := MaterializeArtifacts(root, proposal, HealthJudgeArtifacts); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "package: ./detectors/health/health-objective") || !strings.Contains(string(data), "possiblePlaybooks:") || !strings.Contains(string(data), "- health-objective") || strings.Contains(string(data), ".sdo/playbooks/") {
		t.Fatalf("incomplete manifest: %s", data)
	}
	for _, name := range []string{".sdo/diagnostics/detectors/health/health-objective/detector.go", ".sdo/diagnostics/detectors/health/service-endpoints/detector.go", ".sdo/playbooks/health-objective/README.md", ".sdo/playbooks/README.md"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	generated, err := os.ReadFile(filepath.Join(root, ".sdo", "diagnostics", "detectors", "health", "service-endpoints", "detector.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), `Playbooks: []string{"health-objective"}`) || strings.Contains(string(generated), ".sdo/") {
		t.Fatalf("generated SDK detector must reference logical playbook IDs: %s", generated)
	}
}

func TestMaterializeRejectsUntrustedPathsBeforeMutation(t *testing.T) {
	root := t.TempDir()
	proposal := &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{Id: "../../outside", DetectorClass: "incident", Owner: "responder", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package bad"}}}}}
	if err := MaterializeArtifacts(root, proposal, ResponderArtifacts); err == nil {
		t.Fatal("unsafe ID accepted")
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo")); !os.IsNotExist(err) {
		t.Fatalf("proposal mutated checkout before validation: %v", err)
	}
}

func TestMaterializeRejectsPhysicalReferencesInWorkerContent(t *testing.T) {
	for _, test := range []struct {
		name     string
		proposal *contracts.OperationalArtifactProposal
	}{
		{name: "detector source", proposal: &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{Id: "fault", DetectorClass: "incident", Owner: "responder", Constructor: "New", Firing: 1, OriginatingIncident: "incident", OriginatingCommit: "accepted", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package fault\n// .sdo/playbooks/fault/README.md\n"}}}}}},
		{name: "playbook markdown", proposal: &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: "fault", Owner: "responder", FaultClass: "fault", OriginatingIncident: "incident", Markdown: "Read .sdo/playbooks/fault/README.md"}}}},
		{name: "playbook script", proposal: &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: "fault", Owner: "responder", FaultClass: "fault", OriginatingIncident: "incident", Markdown: "# Repair", Scripts: []*contracts.MemoryScript{{Name: "verify.sh", Content: "cat .sdo/goal.md"}}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := MaterializeArtifacts(root, test.proposal, ResponderArtifacts); err == nil || !strings.Contains(err.Error(), "physical operational memory path") {
				t.Fatalf("worker content with storage path was accepted: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, ".sdo")); !os.IsNotExist(err) {
				t.Fatalf("proposal mutated checkout before rejection: %v", err)
			}
		})
	}
}

func TestMaterializeJudgeBootstrapsAbsentManifest(t *testing.T) {
	root := t.TempDir()
	proposal := &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{Id: "health-objective", DetectorClass: "health", Owner: "health_judge", Constructor: "New", OriginatingCommit: "lifecycle-bootstrap", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package objective\n"}}}}, Playbooks: []*contracts.ProposedPlaybook{{Id: "health-objective", Owner: "responder", FaultClass: "health-objective-failure", OriginatingIncident: "lifecycle-bootstrap", Markdown: "# Restore <AFFECTED_RESOURCE>"}}}
	if err := MaterializeArtifacts(root, proposal, HealthJudgeArtifacts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")); err != nil {
		t.Fatal(err)
	}
}

func TestTypedArtifactsRejectDirectWorkerFilesystemEdits(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := RequireCleanProposalCheckout(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "direct-memory-edit"), []byte("untrusted"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanProposalCheckout(context.Background(), root); err == nil {
		t.Fatal("direct worker edit accepted")
	}
}

func TestTypedArtifactsRejectIgnoredSharedLibraryEdits(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("lib/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "add", ".gitignore")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git add: %v: %s", err, output)
	}
	command = exec.Command("git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "ignore")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v: %s", err, output)
	}
	path := filepath.Join(root, ".sdo", "lib", "ports", "ports.go")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package ports\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RequireCleanProposalCheckout(context.Background(), root); err == nil {
		t.Fatal("ignored library direct edit accepted")
	}
}
