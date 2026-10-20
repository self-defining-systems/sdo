package memory

import (
	"os"
	"path/filepath"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestMaterializeProgramAndSharedLibrary(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\ndetectors: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	proposal := &contracts.OperationalArtifactProposal{
		Playbooks: []*contracts.ProposedPlaybook{{Id: "targetport", Owner: "responder", FaultClass: "targetport", OriginatingIncident: "inc-1", Markdown: "# Diagnose <SERVICE>", Program: &contracts.ProposedProgram{Sources: []*contracts.ProposedGoSource{{Name: "diagnose.go", Content: "package targetport\n"}}, CorpusCases: []*contracts.ProposedCorpusCase{{Id: "case-1", OpenJson: "{}", FixedJson: "{}", HealthyJson: "{}", RecordingsJson: "[]", LabelsJson: "{}"}}}}},
		Libraries: []*contracts.ProposedLibrary{{Package: "ports", Sources: []*contracts.ProposedGoSource{{Name: "ports.go", Content: "package ports\n"}}}},
	}
	if err := MaterializeArtifacts(root, proposal, ResponderArtifacts); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"playbooks/targetport/diagnose.go", "corpus/case-1/open.json", "corpus/case-1/fixed.json", "corpus/case-1/healthy.json", "corpus/case-1/recordings.json", "corpus/case-1/labels.json", "lib/ports/ports.go", "lib/ports/metadata.yaml"} {
		if _, err := os.Stat(filepath.Join(root, ".sdo", path)); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestMaterializeRejectsUnsafeProgramPathsBeforeMutation(t *testing.T) {
	for _, proposal := range []*contracts.OperationalArtifactProposal{
		{Libraries: []*contracts.ProposedLibrary{{Package: "../outside", Sources: []*contracts.ProposedGoSource{{Name: "lib.go", Content: "package lib\n"}}}}},
		{Playbooks: []*contracts.ProposedPlaybook{{Id: "fault", Owner: "responder", FaultClass: "fault", OriginatingIncident: "incident", Markdown: "# Check <SERVICE>", Program: &contracts.ProposedProgram{Sources: []*contracts.ProposedGoSource{{Name: "diagnose.go", Content: "package fault\n"}}, CorpusCases: []*contracts.ProposedCorpusCase{{Id: "../outside"}}}}}},
	} {
		root := t.TempDir()
		if err := MaterializeArtifacts(root, proposal, ResponderArtifacts); err == nil {
			t.Fatal("unsafe proposal accepted")
		}
		if _, err := os.Stat(filepath.Join(root, ".sdo")); !os.IsNotExist(err) {
			t.Fatalf("unsafe proposal mutated checkout: %v", err)
		}
	}
}

func TestSharedLibraryRequiresPlaybookIncidentProvenance(t *testing.T) {
	root := t.TempDir()
	proposal := &contracts.OperationalArtifactProposal{Libraries: []*contracts.ProposedLibrary{{Package: "ports", Sources: []*contracts.ProposedGoSource{{Name: "ports.go", Content: "package ports\n"}}}}}
	if err := MaterializeArtifacts(root, proposal, ResponderArtifacts); err == nil {
		t.Fatal("library without originating playbook accepted")
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo")); !os.IsNotExist(err) {
		t.Fatalf("proposal changed checkout: %v", err)
	}
}
