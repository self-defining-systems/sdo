package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestProgramScaffoldWritesProgramAndCorpusLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "scratch")
	config, err := ParseProgramScaffold([]string{"program", "scaffold", "--fault-class", "targetport", "--param", "service=Service", "--mitigate", "--out", dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := RunProgramScaffold(config); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"README.md", "diagnose.go", "mitigate.go", "diagnose_test.go", "corpus/targetport/open.json", "corpus/targetport/fixed.json", "corpus/targetport/healthy.json", "corpus/targetport/recordings.json", "corpus/targetport/labels.json"} {
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			t.Errorf("missing %s: %v", path, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "diagnose.go"))
	if err != nil || !strings.Contains(string(data), `ID: "targetport"`) || !strings.Contains(string(data), `Name: "service", Kind: "Service"`) {
		t.Fatalf("diagnoser skeleton: %s, %v", data, err)
	}
	if err := RunProgramScaffold(config); err == nil {
		t.Fatal("scaffold overwrote existing files")
	}
}

func TestProgramProposeAssemblesTypedProposalWithIncident(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "scratch")
	if err := RunProgramScaffold(ProgramScaffoldConfig{FaultClass: "targetport", Params: []ProgramParam{{Name: "service", Kind: "Service"}}, Mitigate: true, Out: dir}); err != nil {
		t.Fatal(err)
	}
	lib := filepath.Join(t.TempDir(), "ports")
	if err := os.Mkdir(lib, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lib, "ports.go"), []byte("package ports\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := ParseProgramPropose([]string{"program", "propose", "--dir", dir, "--lib", lib, "--incident", "inc-42"})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := AssembleProgramProposal(config)
	if err != nil {
		t.Fatal(err)
	}
	data, err := protojson.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &contracts.OperationalArtifactProposal{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.GetPlaybooks()) != 1 || decoded.GetPlaybooks()[0].GetId() != "targetport" || decoded.GetPlaybooks()[0].GetOwner() != "responder" || decoded.GetPlaybooks()[0].GetOriginatingIncident() != "inc-42" || len(decoded.GetPlaybooks()[0].GetProgram().GetCorpusCases()) != 1 || len(decoded.GetLibraries()) != 1 {
		t.Fatalf("assembled proposal: %s", data)
	}
}

func TestProgramProposeRunsDraftTest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "targetport")
	if err := RunProgramScaffold(ProgramScaffoldConfig{FaultClass: "targetport", Params: []ProgramParam{{Name: "service", Kind: "Service"}}, Out: dir}); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	err := RunProgramPropose(context.Background(), ProgramProposeConfig{Dir: dir, Incident: "inc-42", App: t.TempDir(), OperatorRoot: ".."}, &output)
	if err == nil || !strings.Contains(err.Error(), "draft baseline") {
		t.Fatalf("draft-test was not run: %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("unvalidated proposal emitted: %s", output.String())
	}
}

func TestProgramProposeEmitsValidatedFixture(t *testing.T) {
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "-mod=mod -p=2")
	app := t.TempDir()
	fixture := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "playbook_program_app")
	if output, err := exec.Command("cp", "-a", fixture+"/.", app).CombinedOutput(); err != nil {
		t.Fatalf("copy fixture: %v: %s", err, output)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = app
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	config := ProgramProposeConfig{Dir: filepath.Join(app, ".sdo", "playbooks", "service-targetport-mismatch"), Corpus: []string{filepath.Join(app, ".sdo", "corpus", "targetport-mismatch")}, Incident: "inc-42", App: app, OperatorRoot: ".."}
	var output strings.Builder
	if err := RunProgramPropose(context.Background(), config, &output); err != nil {
		t.Fatalf("valid proposal: %v", err)
	}
	proposal := &contracts.OperationalArtifactProposal{}
	if err := protojson.Unmarshal([]byte(output.String()), proposal); err != nil {
		t.Fatal(err)
	}
	if proposal.GetPlaybooks()[0].GetOriginatingIncident() != "inc-42" {
		t.Fatalf("wrong provenance: %s", output.String())
	}
}
