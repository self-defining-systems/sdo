package memory

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
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
	for _, path := range []string{"playbooks/targetport/diagnose.go", "corpus/case-1/open.json", "corpus/case-1/fixed.json", "corpus/case-1/healthy.json", "corpus/case-1/recordings.json", "corpus/case-1/labels.json", "lib/ports/ports.go"} {
		if _, err := os.Stat(filepath.Join(root, ".sdo", path)); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestMaterializeCapturedCorpusReference(t *testing.T) {
	root, store := t.TempDir(), t.TempDir()
	manifest := filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\ndetectors: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	open, err := json.Marshal(sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "configuration", Namespace: "app"}, Data: map[string]string{"settings": strings.Repeat("x", 1400000)}}}})
	if err != nil || len(open) < 1400000 {
		t.Fatalf("capture fixture: %d bytes, %v", len(open), err)
	}
	fixed, err := json.Marshal(sdktest.Snapshot{NamespaceName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	recordings := []byte("[]")
	for name, data := range map[string][]byte{"open@detect.json": open, "fixed.json": fixed, "open@detect-recordings.json": recordings} {
		if err := os.WriteFile(filepath.Join(store, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ref := &contracts.CorpusCaptureReference{IncidentId: "incident", CaptureLabel: "open@detect", OpenSha256: fmt.Sprintf("%x", sha256.Sum256(open)), FixedSha256: fmt.Sprintf("%x", sha256.Sum256(fixed)), RecordingsSha256: fmt.Sprintf("%x", sha256.Sum256(recordings)), RootCauseProgramIds: []string{"fault"}}
	proposal := &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: "fault", Owner: "responder", FaultClass: "fault", OriginatingIncident: "incident", Markdown: "# Fault", Program: &contracts.ProposedProgram{Sources: []*contracts.ProposedGoSource{{Name: "diagnose.go", Content: "package fault\n"}}, CorpusCases: []*contracts.ProposedCorpusCase{{Id: "fault-case", Reference: ref}}}}}}
	wire, err := protojson.Marshal(proposal)
	if err != nil || len(wire) > 20<<10 {
		t.Fatalf("reference proposal too large: %d bytes, %v", len(wire), err)
	}
	second := proto.Clone(proposal.Playbooks[0]).(*contracts.ProposedPlaybook)
	second.Id, second.FaultClass = "other-fault", "other-fault"
	second.Program.CorpusCases[0].Id = "other-case"
	second.Program.CorpusCases[0].Reference.RootCauseProgramIds = []string{"other-fault"}
	whole := proto.Clone(proposal).(*contracts.OperationalArtifactProposal)
	whole.Playbooks = append(whole.Playbooks, second)
	whole.Libraries = []*contracts.ProposedLibrary{{Package: "helpers", Sources: []*contracts.ProposedGoSource{{Name: "helpers.go", Content: "package helpers\n"}}}}
	response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ArtifactProposal: whole})
	if err != nil {
		t.Fatal(err)
	}
	result, err := protojson.Marshal(&contracts.AgentResult{SchemaVersion: "sdo.dev/v1alpha1", Status: "completed", Response: response})
	if err != nil || len(result) > 700<<10 {
		t.Fatalf("two-program worker result too large: %d bytes, %v", len(result), err)
	}
	if err := MaterializeArtifacts(root, proposal, ResponderArtifacts, store); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]byte{"open.json": open, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": recordings} {
		got, err := os.ReadFile(filepath.Join(root, ".sdo", "corpus", "fault-case", name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s differs from capture: %v", name, err)
		}
	}
	if err := sdktest.ValidateCorpusProvenance(filepath.Join(root, ".sdo", "corpus", "fault-case")); err != nil {
		t.Fatalf("materialized provenance: %v", err)
	}
	ref.OpenSha256 = strings.Repeat("0", 64)
	if err := MaterializeArtifacts(t.TempDir(), proposal, ResponderArtifacts, store); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("digest mismatch: %v", err)
	}
	ref.OpenSha256 = fmt.Sprintf("%x", sha256.Sum256(open))
	ref.IncidentId = "unknown"
	if err := MaterializeArtifacts(t.TempDir(), proposal, ResponderArtifacts, store); err == nil || !strings.Contains(err.Error(), "incident") {
		t.Fatalf("unknown incident: %v", err)
	}
	ref.IncidentId = "incident"
	if err := MaterializeArtifacts(t.TempDir(), proposal, ResponderArtifacts, t.TempDir()); err == nil || !strings.Contains(err.Error(), "read incident") {
		t.Fatalf("missing incident captures: %v", err)
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

func TestSharedLibraryDoesNotRequirePlaybookIncidentProvenance(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\ndetectors: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	proposal := &contracts.OperationalArtifactProposal{Libraries: []*contracts.ProposedLibrary{{Package: "ports", Sources: []*contracts.ProposedGoSource{{Name: "ports.go", Content: "package ports\n"}}}}}
	if err := MaterializeArtifacts(root, proposal, ResponderArtifacts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo", "lib", "ports", "ports.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo", "lib", "ports", "metadata.yaml")); !os.IsNotExist(err) {
		t.Fatalf("unexpected incident metadata: %v", err)
	}
}
