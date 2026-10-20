package operatorcli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

func TestParseDraftTestRequiresTypedProposal(t *testing.T) {
	if _, err := ParseDraftTestLaunch([]string{"draft-test", "--app", "."}); err == nil {
		t.Fatal("draft-test accepted no typed artifact proposal")
	}
	config, err := ParseDraftTestLaunch([]string{"draft-test", "--app", ".", "--proposal-json", "draft.json", "--operator-root", ".."})
	if err != nil || config.ProposalJSON != "draft.json" {
		t.Fatalf("parse draft-test: %+v, %v", config, err)
	}
}

func TestDraftAllowsRealisticCapturedCorpusWithSeparateBound(t *testing.T) {
	view := sdktest.Snapshot{NamespaceName: "app"}
	for i := 0; i < 110; i++ {
		view.ConfigMapList = append(view.ConfigMapList, corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("configuration-%03d", i), Namespace: "app"}, Data: map[string]string{"settings": strings.Repeat("x", 20<<10)}})
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	large := string(encoded)
	if len(large) < 2<<20 {
		t.Fatalf("fixture too small: %d", len(large))
	}
	proposal := &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: "large", Owner: "responder", FaultClass: "large", OriginatingIncident: "incident", Markdown: "# Large", Program: &contracts.ProposedProgram{CorpusCases: []*contracts.ProposedCorpusCase{{Id: "large", OpenJson: large, FixedJson: "{}", HealthyJson: "{}", RecordingsJson: "[]", LabelsJson: "{}"}}}}}}
	app := draftFixtureApp(t)
	path := filepath.Join(t.TempDir(), "draft.json")
	check := func() error {
		data, err := protojson.Marshal(proposal)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return RunDraftTest(context.Background(), DraftTestConfig{ApplicationRoot: app, OperatorRoot: "..", ProposalJSON: path})
	}
	if err := check(); err == nil || !strings.Contains(err.Error(), "program needs diagnose.go") {
		t.Fatalf("2 MiB corpus did not reach proposal validation: %v", err)
	}
	proposal.Playbooks[0].Program.CorpusCases[0].OpenJson = strings.Repeat("x", 8<<20)
	if err := check(); err == nil || !strings.Contains(err.Error(), "exceeds 8 MiB") {
		t.Fatalf("oversized corpus accepted: %v", err)
	}
}

func TestDraftTestCompilesTypedArtifactsWithoutEditingCheckout(t *testing.T) {
	app := draftFixtureApp(t)
	proposal := &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{
		Id: "draft-incident", DetectorClass: "incident", Owner: "responder", Constructor: "New", OriginatingIncident: "draft-incident", OriginatingCommit: "accepted",
		Watches: []*contracts.MemoryWatch{{ApiVersion: "v1", Kind: "Pod"}}, Interval: "30s", Firing: 1, Clearing: 1, Severity: "critical", Debounce: "500ms",
		Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package draft\nfunc New() undeclared.Type { return nil }\n"}},
	}}}
	data, err := protojson.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	proposalPath := filepath.Join(t.TempDir(), "draft.json")
	if err := os.WriteFile(proposalPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = RunDraftTest(context.Background(), DraftTestConfig{ApplicationRoot: app, OperatorRoot: "..", ProposalJSON: proposalPath})
	if err == nil || !strings.Contains(err.Error(), "go test") {
		t.Fatalf("draft compile error was not reported: %v", err)
	}
	status := exec.Command("git", "-C", app, "status", "--porcelain")
	output, err := status.CombinedOutput()
	if err != nil || len(output) != 0 {
		t.Fatalf("draft-test changed accepted checkout: %v: %s", err, output)
	}
}

func draftFixtureApp(t *testing.T) string {
	return draftFixtureAppFrom(t, "missing_configmap_app")
}

func draftFixtureAppFrom(t *testing.T, fixtureName string) string {
	t.Helper()
	app := t.TempDir()
	fixture := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", fixtureName)
	copy := exec.Command("cp", "-a", fixture+"/.", app)
	if output, err := copy.CombinedOutput(); err != nil {
		t.Fatalf("copy fixture: %v: %s", err, output)
	}
	memory := filepath.Join(app, ".sdo")
	playbookIndex := "# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n"
	if fixtureName == "playbook_program_app" {
		playbookIndex += "- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n"
	}
	for name, content := range map[string]string{
		"schema-version":      "1\n",
		"goal.md":             "---\nschema_version: 1\nowner: human\n---\n\nKeep the app healthy.\n",
		"arch.md":             "---\nschema_version: 1\nowner: health_judge\n---\n\nOne service.\n",
		"playbooks/README.md": playbookIndex,
	} {
		if err := os.WriteFile(filepath.Join(memory, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The shared fixture predates the owned-directory layout the broker enforces.
	diagnostics := filepath.Join(memory, "diagnostics")
	if err := os.MkdirAll(filepath.Join(diagnostics, "detectors", "incidents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(diagnostics, "detectors", "missing_configmap"), filepath.Join(diagnostics, "detectors", "incidents", "missing_configmap")); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(diagnostics, "manifest.yaml")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte(strings.Replace(string(manifest), "./detectors/missing_configmap", "./detectors/incidents/missing_configmap", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(memory, "playbooks", "missing-configmap", "README.md")
	body, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	front := "---\nschema_version: 1\nowner: \"responder\"\nfault_class: \"missing-configmap\"\noriginating_incident: \"fixture\"\n---\n\n"
	if err := os.WriteFile(existing, append([]byte(front), body...), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = app
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	detach := exec.Command("git", "-C", app, "checkout", "--detach", "--quiet", "HEAD")
	if output, err := detach.CombinedOutput(); err != nil {
		t.Fatalf("detach proposal checkout: %v: %s", err, output)
	}
	return app
}

func TestDraftTestValidatesProgramProposal(t *testing.T) {
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "-mod=mod -p=2")
	app := draftFixtureAppFrom(t, "playbook_program_app")
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(app, ".sdo", path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	program := read("playbooks/service-targetport-mismatch/diagnose.go")
	proposal := &contracts.OperationalArtifactProposal{
		Playbooks: []*contracts.ProposedPlaybook{{Id: "service-targetport-mismatch", Owner: "responder", FaultClass: "service-targetport-mismatch", OriginatingIncident: "targetport-incident", Markdown: "Check the <SERVICE> target port.", Program: &contracts.ProposedProgram{Sources: []*contracts.ProposedGoSource{{Name: "diagnose.go", Content: program}}, CorpusCases: []*contracts.ProposedCorpusCase{{Id: "targetport-mismatch", OpenJson: read("corpus/targetport-mismatch/open.json"), FixedJson: read("corpus/targetport-mismatch/fixed.json"), HealthyJson: read("corpus/targetport-mismatch/healthy.json"), RecordingsJson: read("corpus/targetport-mismatch/recordings.json"), LabelsJson: read("corpus/targetport-mismatch/labels.json")}}}}},
		Libraries: []*contracts.ProposedLibrary{{Package: "ports", Sources: []*contracts.ProposedGoSource{{Name: "ports.go", Content: read("lib/ports/ports.go")}}}},
	}
	path := filepath.Join(t.TempDir(), "proposal.json")
	write := func() {
		t.Helper()
		data, err := protojson.Marshal(proposal)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	config := DraftTestConfig{ApplicationRoot: app, OperatorRoot: "..", ProposalJSON: path}
	if err := RunDraftTest(context.Background(), config); err != nil {
		t.Fatalf("valid program draft: %v", err)
	}
	proposal.Libraries[0].Sources[0].Content = "package ports\nimport \"net/http\"\nvar _ = http.Get\n"
	write()
	if err := RunDraftTest(context.Background(), config); err == nil || !strings.Contains(err.Error(), "may not import") {
		t.Fatalf("unsafe draft: %v", err)
	}
}

// The structured artifact schema defaults owner, class, constructor, cadence, persistence, and severity, so the
// local check must accept a hand-written draft that relies on the same defaults.
func TestDraftTestAppliesTheArtifactSchemaDefaults(t *testing.T) {
	app := draftFixtureApp(t)
	proposal := &contracts.OperationalArtifactProposal{
		Detectors: []*contracts.ProposedDetector{{
			Id: "draft-incident", OriginatingIncident: "draft-incident", OriginatingCommit: "accepted",
			Watches: []*contracts.MemoryWatch{{ApiVersion: "v1", Kind: "Pod"}},
			Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package draft\nfunc New() undeclared.Type { return nil }\n"}},
		}},
		Playbooks: []*contracts.ProposedPlaybook{{Id: "draft-playbook", FaultClass: "draft-playbook", OriginatingIncident: "draft-incident", Markdown: "# Draft <NAMESPACE>\n"}},
	}
	data, err := protojson.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	proposalPath := filepath.Join(t.TempDir(), "draft.json")
	if err := os.WriteFile(proposalPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = RunDraftTest(context.Background(), DraftTestConfig{ApplicationRoot: app, OperatorRoot: "..", ProposalJSON: proposalPath})
	if err == nil || !strings.Contains(err.Error(), "go test") {
		t.Fatalf("defaults were not applied before validation: %v", err)
	}
}

func TestDraftTestReportsTheBrokersPlaybookRules(t *testing.T) {
	app := draftFixtureApp(t)
	proposal := &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{
		Id: "draft-playbook", FaultClass: "some-other-class", OriginatingIncident: "draft-incident", Markdown: "# Draft <NAMESPACE>\n",
	}}}
	data, err := protojson.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	proposalPath := filepath.Join(t.TempDir(), "draft.json")
	if err := os.WriteFile(proposalPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = RunDraftTest(context.Background(), DraftTestConfig{ApplicationRoot: app, OperatorRoot: "..", ProposalJSON: proposalPath})
	if err == nil || !strings.Contains(err.Error(), "fault_class must be draft-playbook") {
		t.Fatalf("broker playbook rule was not reported by the local check: %v", err)
	}
}
