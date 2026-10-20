package operatorcli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

func TestProgramScaffoldWritesProgramLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "scratch")
	config, err := ParseProgramScaffold([]string{"program", "scaffold", "--fault-class", "targetport", "--param", "service=Service", "--mitigate", "--out", dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := RunProgramScaffold(config); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"README.md", "diagnose.go", "mitigate.go", "diagnose_test.go"} {
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
	if len(decoded.GetPlaybooks()) != 1 || decoded.GetPlaybooks()[0].GetId() != "targetport" || decoded.GetPlaybooks()[0].GetOwner() != "responder" || decoded.GetPlaybooks()[0].GetOriginatingIncident() != "inc-42" || len(decoded.GetPlaybooks()[0].GetProgram().GetCorpusCases()) != 0 || len(decoded.GetLibraries()) != 1 {
		t.Fatalf("assembled proposal: %s", data)
	}
}

func TestProgramProposeCanonicalizesGoSources(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fault")
	if err := RunProgramScaffold(ProgramScaffoldConfig{FaultClass: "fault", Params: []ProgramParam{{Name: "service", Kind: "Service"}}, Out: dir}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "diagnose.go")
	if err := os.WriteFile(path, []byte("package fault\nfunc f(){println(1)}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	proposal, err := AssembleProgramProposal(ProgramProposeConfig{Dir: dir, Incident: "incident"})
	if err != nil {
		t.Fatal(err)
	}
	if got := proposal.Playbooks[0].Program.Sources[0].Content; got != "package fault\n\nfunc f() { println(1) }\n" {
		t.Fatalf("source not canonical: %q", got)
	}
	if err := os.WriteFile(path, []byte("package fault\nfunc f( {\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := AssembleProgramProposal(ProgramProposeConfig{Dir: dir, Incident: "incident"}); err == nil || !strings.Contains(err.Error(), "expected") {
		t.Fatalf("bad source accepted: %v", err)
	}
}

func TestProgramProposeBuildsCorpusFromIncidentCapture(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "demo", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo"}}, {ObjectMeta: metav1.ObjectMeta{Name: "peer", Namespace: "demo"}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "peer-pod", Namespace: "demo"}}}}
	if err := memory.CaptureIncidentSnapshot(root, "inc-42", "open", view, "demo"); err != nil {
		t.Fatal(err)
	}
	if err := memory.CaptureIncidentSnapshot(root, "inc-42", "fixed", view, "demo"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "targetport")
	if err := RunProgramScaffold(ProgramScaffoldConfig{FaultClass: "targetport", Params: []ProgramParam{{Name: "service", Kind: "Service"}}, Out: dir}); err != nil {
		t.Fatal(err)
	}
	captureDir, err := memory.IncidentCorpusDir(root, "inc-42")
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := AssembleProgramProposal(ProgramProposeConfig{Dir: dir, Incident: "inc-42", CorpusFromIncident: "inc-42", Bindings: []string{"service=Service/demo/front"}, PeerBindings: []string{"service=Service/demo/peer"}, CaptureDir: captureDir})
	if err != nil {
		t.Fatal(err)
	}
	cases := proposal.Playbooks[0].Program.CorpusCases
	if len(cases) != 1 || cases[0].FixedJson != cases[0].HealthyJson || !strings.Contains(cases[0].LabelsJson, `"incident_id":"inc-42"`) || !strings.Contains(cases[0].LabelsJson, "open_sha256") {
		t.Fatalf("not a captured corpus case: %+v", cases)
	}
	if !strings.Contains(cases[0].OpenJson, `"name":"front"`) {
		t.Fatal("captured snapshot missing application object")
	}
	if _, err := AssembleProgramProposal(ProgramProposeConfig{Dir: dir, Incident: "inc-42", CorpusFromIncident: "inc-42", Bindings: []string{"service=Service/demo/front"}, PeerBindings: []string{"service=Pod/demo/peer-pod"}, CaptureDir: captureDir}); err == nil || !strings.Contains(err.Error(), "must have kind Service") {
		t.Fatalf("different-kind peer accepted: %v", err)
	}
}

func TestProgramProposeAcceptsLocalDraftRoots(t *testing.T) {
	config, err := ParseProgramPropose([]string{"program", "propose", "--dir", "scratch", "--incident", "inc-42", "--app", "accepted", "--operator-root", "operator"})
	if err != nil {
		t.Fatal(err)
	}
	if config.App != "accepted" || config.OperatorRoot != "operator" {
		t.Fatalf("draft roots: %+v", config)
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
	app := draftFixtureAppFrom(t, "playbook_program_app")
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
