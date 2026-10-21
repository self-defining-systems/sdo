package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

func TestMaterializeJudgeProposalOwnsPhysicalLayout(t *testing.T) {
	root := t.TempDir()
	proposal := &contracts.OperationalArtifactProposal{
		Detectors: []*contracts.ProposedDetector{{Id: "health-objective", DetectorClass: "health", Owner: "health_judge", Constructor: "New", Watches: []*contracts.MemoryWatch{{ApiVersion: "v1", Kind: "Pod"}}, Interval: "30s", Firing: 2, Clearing: 2, Severity: "critical", Debounce: "500ms", PossiblePlaybookIds: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package objective\n"}, {Name: "detector_test.go", Content: "package objective\n"}}}},
		Playbooks: []*contracts.ProposedPlaybook{{Id: "health-objective", Owner: "responder", FaultClass: "health-objective-failure", OriginatingIncident: "lifecycle-bootstrap", Markdown: "# Restore <AFFECTED_RESOURCE>\n"}},
	}
	if err := MaterializeArtifacts(root, proposal, HealthJudgeArtifacts); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "health-objective")))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "id: health-objective") || !strings.Contains(string(data), "possiblePlaybooks:") || !strings.Contains(string(data), "- health-objective") || strings.Contains(string(data), "package:") || strings.Contains(string(data), ".sdo/playbooks/") {
		t.Fatalf("incomplete manifest: %s", data)
	}
	if _, err := os.Stat(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "service-endpoints"))); err != nil {
		t.Fatalf("operator-owned detector has no manifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, layout.LegacyDetectorManifestFile)); !os.IsNotExist(err) {
		t.Fatalf("shared manifest must not be written: %v", err)
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
	proposal := &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{Id: "../../outside", DetectorClass: "health", Owner: "health_judge", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package bad"}}}}}
	if err := MaterializeArtifacts(root, proposal, HealthJudgeArtifacts); err == nil {
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
		{name: "detector source", proposal: &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{Id: "fault", DetectorClass: "health", Owner: "health_judge", Constructor: "New", Firing: 1, OriginatingIncident: "incident", OriginatingCommit: "accepted", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package fault\n// .sdo/playbooks/fault/README.md\n"}}}}}},
		{name: "playbook markdown", proposal: &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: "health-objective", Owner: "responder", FaultClass: "fault", OriginatingIncident: "incident", Markdown: "Read .sdo/playbooks/fault/README.md"}}}},
		{name: "playbook script", proposal: &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: "health-objective", Owner: "responder", FaultClass: "fault", OriginatingIncident: "incident", Markdown: "# Repair", Scripts: []*contracts.MemoryScript{{Name: "verify.sh", Content: "cat .sdo/goal.md"}}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := MaterializeArtifacts(root, test.proposal, HealthJudgeArtifacts); err == nil || !strings.Contains(err.Error(), "physical operational memory path") {
				t.Fatalf("worker content with storage path was accepted: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, ".sdo")); !os.IsNotExist(err) {
				t.Fatalf("proposal mutated checkout before rejection: %v", err)
			}
		})
	}
}

func TestMaterializeJudgeBootstrapsAbsentDiagnostics(t *testing.T) {
	root := t.TempDir()
	proposal := &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{Id: "health-objective", DetectorClass: "health", Owner: "health_judge", Constructor: "New", OriginatingCommit: "lifecycle-bootstrap", Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package objective\n"}}}}, Playbooks: []*contracts.ProposedPlaybook{{Id: "health-objective", Owner: "responder", FaultClass: "health-objective-failure", OriginatingIncident: "lifecycle-bootstrap", Markdown: "# Restore <AFFECTED_RESOURCE>"}}}
	if err := MaterializeArtifacts(root, proposal, HealthJudgeArtifacts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "health-objective"))); err != nil {
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

func TestMaterializeArtifactsRejectsResponderRole(t *testing.T) {
	root := t.TempDir()
	if err := MaterializeArtifacts(root, &contracts.OperationalArtifactProposal{}, ArtifactRole("responder")); err == nil {
		t.Fatal("reflection retained a route into the judge writer")
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo")); !os.IsNotExist(err) {
		t.Fatalf("rejected role mutated checkout: %v", err)
	}
}
