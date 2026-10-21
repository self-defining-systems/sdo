package operatorcli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/worker_registry"
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
	view := sdktest.Snapshot{NamespaceName: "demo", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo"}}, {ObjectMeta: metav1.ObjectMeta{Name: "peer", Namespace: "demo"}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "peer-pod", Namespace: "demo"}}}, ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "demo"}, Data: map[string]string{"configuration": strings.Repeat("x", 1400000)}}}}
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
	wire, err := protojson.Marshal(proposal)
	if err != nil || len(wire) > 20<<10 {
		t.Fatalf("1.4 MB capture produced %d-byte proposal: %v", len(wire), err)
	}
	if len(cases) != 1 || cases[0].GetReference() == nil || cases[0].GetReference().GetIncidentId() != "inc-42" || cases[0].OpenJson != "" {
		t.Fatalf("not a captured corpus case: %+v", cases)
	}
	if len(cases[0].GetReference().GetBindings()) != 1 || len(cases[0].GetReference().GetPeerBindings()) != 1 {
		t.Fatal("captured reference missing bindings")
	}
	if _, err := AssembleProgramProposal(ProgramProposeConfig{Dir: dir, Incident: "inc-42", CorpusFromIncident: "inc-42", Bindings: []string{"service=Service/demo/front"}, PeerBindings: []string{"service=Pod/demo/peer-pod"}, CaptureDir: captureDir}); err == nil || !strings.Contains(err.Error(), "must have kind Service") {
		t.Fatalf("different-kind peer accepted: %v", err)
	}
}

func TestProgramProposeSelectsLatestPremitigationCapture(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "demo", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo"}}, {ObjectMeta: metav1.ObjectMeta{Name: "peer", Namespace: "demo"}}}}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		view.ServiceList[0].Annotations = map[string]string{"moment": phase}
		if err := memory.CaptureIncidentSnapshot(root, "inc-42", phase, view, "demo"); err != nil {
			t.Fatal(err)
		}
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
	config := ProgramProposeConfig{Dir: dir, Incident: "inc-42", CorpusFromIncident: "inc-42", CaptureDir: captureDir, Bindings: []string{"service=Service/demo/front"}, PeerBindings: []string{"service=Service/demo/peer"}}
	proposal, err := AssembleProgramProposal(config)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.Playbooks[0].Program.CorpusCases[0].GetReference().GetCaptureLabel() != "open@pre-apply" {
		t.Fatal("latest open capture was not selected")
	}
	config.OpenAt = "detect"
	proposal, err = AssembleProgramProposal(config)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.Playbooks[0].Program.CorpusCases[0].GetReference().GetCaptureLabel() != "open@detect" {
		t.Fatal("requested open capture was not selected")
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
	seedUnformattedHealthSource(t, app)
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

func TestScriptedReflectionScaffoldProposeAndWorkerResult(t *testing.T) {
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "-mod=mod -p=2")
	app := draftFixtureAppFrom(t, "playbook_program_app")
	seedUnformattedHealthSource(t, app)
	incident := "scripted-incident"
	captureRoot := t.TempDir()
	fixture := filepath.Join(app, ".sdo", "corpus", "targetport-mismatch")
	for _, phase := range []string{"open@pre-apply", "fixed"} {
		name := "open.json"
		if phase == "fixed" {
			name = "fixed.json"
		}
		wire, err := os.ReadFile(filepath.Join(fixture, name))
		if err != nil {
			t.Fatal(err)
		}
		var snapshot sdktest.Snapshot
		if err := json.Unmarshal(wire, &snapshot); err != nil {
			t.Fatal(err)
		}
		if phase == "fixed" {
			healthy, err := os.ReadFile(filepath.Join(fixture, "healthy.json"))
			if err != nil {
				t.Fatal(err)
			}
			var peer sdktest.Snapshot
			if err := json.Unmarshal(healthy, &peer); err != nil {
				t.Fatal(err)
			}
			snapshot.ServiceList = append(snapshot.ServiceList, peer.ServiceList...)
			snapshot.PodList = append(snapshot.PodList, peer.PodList...)
		}
		if err := memory.CaptureIncidentSnapshot(captureRoot, incident, phase, snapshot, "demo"); err != nil {
			t.Fatal(err)
		}
	}
	captureDir, err := memory.IncidentCorpusDir(captureRoot, incident)
	if err != nil {
		t.Fatal(err)
	}
	recordings, err := os.ReadFile(filepath.Join(fixture, "recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(recordings, &calls); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		if err := memory.AppendCapturedRecording(captureRoot, incident, "open@pre-apply", call); err != nil {
			t.Fatal(err)
		}
	}
	dir := filepath.Join(t.TempDir(), "service-targetport-mismatch")
	if err := RunProgramScaffold(ProgramScaffoldConfig{FaultClass: "service-targetport-mismatch", Params: []ProgramParam{{Name: "service", Kind: "Service"}}, Mitigate: true, Out: dir}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "diagnose_test.go")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"README.md", "diagnose.go", "mitigate.go"} {
		source := filepath.Join("..", "..", "..", "benchmarks", "sregym", "fastloop", "scripted_program", name)
		wire, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), wire, 0600); err != nil {
			t.Fatal(err)
		}
	}
	config := ProgramProposeConfig{Dir: dir, Incident: incident, App: app, OperatorRoot: "..", CorpusFromIncident: incident,
		OpenAt: "pre-apply", Bindings: []string{"service=Service/demo/front"}, PeerBindings: []string{"service=Service/demo/peer"}, CaptureDir: captureDir}
	var output strings.Builder
	if err := RunProgramPropose(context.Background(), config, &output); err != nil {
		t.Fatal(err)
	}
	proposal := &contracts.OperationalArtifactProposal{}
	if err := protojson.Unmarshal([]byte(output.String()), proposal); err != nil {
		t.Fatal(err)
	}
	cases := proposal.GetPlaybooks()[0].GetProgram().GetCorpusCases()
	if len(cases) != 1 || cases[0].GetReference().GetCaptureLabel() != "open@pre-apply" {
		t.Fatalf("proposal did not use bounded capture reference: %+v", cases)
	}
	response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ArtifactProposal: proposal})
	if err != nil {
		t.Fatal(err)
	}
	result := &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: "reflection", Status: "completed", ProposalId: "reflection", Response: response}
	wire, err := protojson.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) > 700<<10 {
		t.Fatalf("reflection result exceeds worker transport: %d bytes", len(wire))
	}
	registry, err := worker_registry.LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	request, err := anypb.New(&contracts.ReflectionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{SchemaVersion: worker_registry.SchemaVersion, TaskId: "reflection", AgentId: "sdo-reflector", TaskType: "outcome-reflection", Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "reflection"}}
	if err := registry.ValidateResult(task, result); err != nil {
		t.Fatal(err)
	}
	repository, err := broker.New(app, filepath.Join(t.TempDir(), "proposals"), func(ctx context.Context, candidate string, changed []string) error {
		if err := memory.ValidateCapturedCorpusTree(captureRoot, candidate, incident, changed); err != nil {
			return err
		}
		workspace, err := builder.BuildWorkspace(ctx, builder.Config{ApplicationRoot: candidate, DiagnosticsRoot: filepath.Join(candidate, ".sdo", "diagnostics"), OperatorRoot: ".."})
		if err != nil {
			return err
		}
		defer workspace.Close()
		_, err = workspace.ValidateAndBuild(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	repair, err := repository.CreateProposal(context.Background(), incident)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repair.Path, "scripted-repair.txt"), []byte("repaired\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Publish(context.Background(), incident, broker.Responder, broker.ProposalPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.RecordOutcome(context.Background(), incident, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: incident, Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	reflection, err := repository.CreateProposalForPhase(context.Background(), incident, broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.MaterializeArtifacts(reflection.Path, proposal, memory.ResponderArtifacts, captureDir); err != nil {
		t.Fatal(err)
	}
	accepted, err := repository.Publish(context.Background(), incident, broker.Responder, broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Commit == "" {
		t.Fatal("broker did not accept the compiled reflection program")
	}
	if _, err := os.Stat(filepath.Join(app, ".sdo", "playbooks", "service-targetport-mismatch", "mitigate.go")); err != nil {
		t.Fatalf("accepted program is missing: %v", err)
	}
	rebuilt, err := builder.BuildWorkspace(context.Background(), builder.Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo", "diagnostics"), OperatorRoot: ".."})
	if err != nil {
		t.Fatal(err)
	}
	defer rebuilt.Close()
	if _, err := rebuilt.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("accepted program did not rebuild: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rebuilt.Directory, "generated", "programs.go")); err != nil {
		t.Fatalf("rebuilt registry omitted accepted program: %v", err)
	}
}
