package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const fastloopProgramRunner = `package main

import (
	"context"
	"encoding/json"
	"os"
	"app-diagnostics/generated"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func main() {
	var snapshot sdktest.Snapshot
	file, err := os.Open(os.Args[1]); if err != nil { panic(err) }
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil { panic(err) }
	registry, err := generated.Programs(); if err != nil { panic(err) }
	diagnoser, ok := registry.Diagnoser("service-targetport-mismatch"); if !ok { panic("missing compiled diagnoser") }
	mitigator, ok := registry.Mitigator("service-targetport-mismatch"); if !ok { panic("missing compiled mitigator") }
	bindings := program.Bindings{"service": sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "peer"}}
	view := &sdktest.ReplayReadView{Snapshot: snapshot}
	verdict, err := diagnoser.Diagnose(context.Background(), view, bindings); if err != nil { panic(err) }
	plan, err := mitigator.Mitigate(context.Background(), view, bindings); if err != nil { panic(err) }
	if len(plan.Actions) != 1 { panic("expected one action") }
	action, ok := plan.Actions[0].(program.SetField); if !ok { panic("expected SetField") }
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"result": verdict.Result, "path": action.Path, "value": action.Value}); err != nil { panic(err) }
}
`

func copyFastloopTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func fastloopProgramProposal(t *testing.T, captureDir, incidentID string) *contracts.OperationalArtifactProposal {
	t.Helper()
	digest := func(name string) string {
		data, err := os.ReadFile(filepath.Join(captureDir, name))
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%x", sha256.Sum256(data))
	}
	sources := []*contracts.ProposedGoSource{}
	for _, name := range []string{"diagnose.go", "mitigate.go"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "benchmarks", "sregym", "fastloop", "scripted_program", name))
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, &contracts.ProposedGoSource{Name: name, Content: string(data)})
	}
	bind := func(name string) *contracts.CorpusBinding {
		return &contracts.CorpusBinding{Name: "service", Object: &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: name}}
	}
	return &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{
		Id: "service-targetport-mismatch", Owner: "responder", FaultClass: "service-targetport-mismatch", OriginatingIncident: incidentID,
		Markdown: "Check the selected pod port against the `<SERVICE>` target port.",
		Program: &contracts.ProposedProgram{Sources: sources, CorpusCases: []*contracts.ProposedCorpusCase{{Id: "targetport-" + incidentID, Reference: &contracts.CorpusCaptureReference{
			IncidentId: incidentID, CaptureLabel: "open@detect", OpenSha256: digest("open@detect.json"), FixedSha256: digest("fixed.json"), RecordingsSha256: digest("open@detect-recordings.json"),
			Bindings: []*contracts.CorpusBinding{bind("api")}, PeerBindings: []*contracts.CorpusBinding{bind("peer")}, RootCauseProgramIds: []string{"service-targetport-mismatch"},
		}}}},
	}}}
}

func fastloopGateResult(request IncidentRequest, resources []sdk.ObjectRef) IncidentResult {
	result := completedResult(request.IncidentID)
	result.Timing.StartedAt = request.DetectorHistory[len(request.DetectorHistory)-1].EvaluatedAt.Add(100 * time.Millisecond)
	result.Timing.CompletedAt = result.Timing.StartedAt.Add(100 * time.Millisecond)
	detectorID := request.Findings[0].DetectorID
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "Service target port drift", Resources: resources, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: detectorID, Observation: "targetPort mismatch"}}, ExplainedDetectors: []string{detectorID}}}
	result.RepairActions = []RepairActionReceipt{{ActionID: "repair", Kind: "kubectl", Target: "app", Summary: "restored source target port", Details: "fake Kubernetes API reports the source port", Resources: resources, StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt, Success: true}}
	result.VerificationEvidence = []VerificationEvidence{{Name: "detector sweep", Passed: true, Details: "drift finding clears", ObservedAt: result.Timing.CompletedAt}}
	return result
}

type fastloopDriftWorker struct {
	requests []*contracts.DriftInvestigationTask
}

type fastloopDriftView struct{ driftReadView }

func (v fastloopDriftView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if ref.Name == "jaeger" {
		return program.Diff{Changed: true}, nil
	}
	return v.driftReadView.DiffAgainstSource(ref)
}

func (w *fastloopDriftWorker) Investigate(_ context.Context, task *contracts.DriftInvestigationTask) (*contracts.DriftInvestigationResult, error) {
	w.requests = append(w.requests, task)
	result := &contracts.DriftInvestigationResult{}
	for index, event := range task.Events {
		verdict := &contracts.DriftEventInvestigation{EventIndex: int32(index), Verdict: "unsure", Reason: "pre-existing difference"}
		if event.GetRef().GetName() == "api" {
			verdict.Verdict = "bug"
			verdict.Reason = "targetPort contradicts source and no selected pod serves it"
			verdict.Evidence = []*contracts.DriftEvidence{{Kind: "source_contradiction", Observation: "source targetPort is 8080"}, {Kind: "violated_relation", Observation: "no selected pod serves 9999"}}
		}
		result.Events = append(result.Events, verdict)
	}
	return result, nil
}

type fastloopGateDispatch struct {
	requests  chan IncidentRequest
	results   chan IncidentResult
	failFirst bool
	attempts  int
}

func (d *fastloopGateDispatch) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.attempts++
	if d.failFirst && d.attempts == 1 {
		return IncidentResult{}, errors.New("temporary result transport failure")
	}
	select {
	case d.requests <- request:
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
	select {
	case result := <-d.results:
		return result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

func fastloopGateWait(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for controller effect")
}

func fastloopSameRequest(left, right IncidentRequest) bool {
	left.Findings = append([]sdk.Finding(nil), left.Findings...)
	right.Findings = append([]sdk.Finding(nil), right.Findings...)
	for index := range left.Findings {
		left.Findings[index].Metadata = nil
	}
	for index := range right.Findings {
		right.Findings[index].Metadata = nil
	}
	return sameJSON(left, right)
}

func fastloopGateIncident(t *testing.T, ctx context.Context, client *fake.Clientset, native NativeIncidentBroker, accepted, captureRoot, stateName string, at time.Time, retry bool, configure ...func(*Controller)) (*Controller, *fastloopGateDispatch, IncidentRequest) {
	t.Helper()
	dispatcher := &fastloopGateDispatch{requests: make(chan IncidentRequest, 1), results: make(chan IncidentResult, 1), failFirst: retry}
	commit := nativeGit(t, accepted, "rev-parse", "HEAD")
	config := ControllerConfig{Application: "app", Namespace: "app", SourceCommit: commit, DeployedCommit: commit, RepositoryWorktree: accepted, CorpusSessionRoot: captureRoot, ResponseTimeout: time.Minute, VerificationTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions"}
	controller, err := NewController(config, []sdk.Detector{fastloopClearDetector{}}, fastloopObjects{client: client}, dispatcher, at.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	controller.OnError = func(err error) { t.Logf("controller effect: %v", err) }
	if err := controller.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	if err := controller.AttachStateStore(ctx, NewConfigMapStateStore(client, "app", stateName)); err != nil {
		t.Fatal(err)
	}
	if err := controller.EvaluateAll(ctx, at.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, setup := range configure {
		setup(controller)
	}
	if err := controller.Step(ctx, at, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := controller.PendingWorkspaceEffect(); !ok {
		if err := controller.Step(ctx, at.Add(time.Millisecond), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	workspace, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("no broker workspace effect")
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, workspace); err != nil {
		t.Fatal(err)
	}
	fastloopGateWait(t, 90*time.Second, func() bool { return len(controller.workspaceResults) > 0 })
	if err := controller.Step(ctx, at.Add(time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	state, _, err := NewConfigMapStateStore(client, "app", stateName).Load(ctx)
	if err != nil || state.IncidentRequest == nil {
		t.Fatalf("persist dispatch: %+v %v", state, err)
	}
	request := *state.IncidentRequest
	if retry {
		controller.now = func() time.Time { return at }
		executePendingEffect(t, controller)
		fastloopGateWait(t, 90*time.Second, func() bool { return len(controller.results) > 0 })
		if err := controller.Step(ctx, at.Add(2*time.Millisecond), nil); err != nil {
			t.Fatal(err)
		}
		if err := controller.PersistState(ctx); err != nil {
			t.Fatal(err)
		}
		controller.now = func() time.Time { return at.Add(5 * time.Second) }
		reloaded, _, err := NewConfigMapStateStore(client, "app", stateName).Load(ctx)
		if err != nil || reloaded.IncidentRequest == nil || !fastloopSameRequest(*reloaded.IncidentRequest, request) {
			t.Fatalf("retry lost persisted dispatch request: %+v %v", reloaded.IncidentRequest, err)
		}
	}
	executePendingEffect(t, controller)
	var actual IncidentRequest
	select {
	case actual = <-dispatcher.requests:
	case <-time.After(90 * time.Second):
		t.Fatal("timed out waiting for dispatched request")
	}
	if !fastloopSameRequest(actual, request) {
		t.Fatalf("dispatch differed from persisted request: %+v %+v", actual, request)
	}
	if retry && dispatcher.attempts != 2 {
		t.Fatalf("expected one dispatch retry, got %d attempts", dispatcher.attempts)
	}
	return controller, dispatcher, request
}

type fastloopClearDetector struct{}

func (fastloopClearDetector) Spec() sdk.DetectorSpec {
	spec := fastloopDetector{}.Spec()
	return spec
}
func (fastloopClearDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func fastloopGateClose(t *testing.T, ctx context.Context, controller *Controller, dispatcher *fastloopGateDispatch, result IncidentResult, at time.Time) IncidentClosure {
	t.Helper()
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	dispatcher.results <- result
	fastloopGateWait(t, 90*time.Second, func() bool { return len(controller.results) > 0 })
	if err := controller.Step(ctx, at.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case closure := <-closed:
		return closure
	case <-time.After(time.Second):
		t.Fatal("controller did not verify and close")
		return IncidentClosure{}
	}
}

func fastloopGateProcessClosure(t *testing.T, ctx context.Context, controller *Controller, incidentID string) ClosureReceipt {
	t.Helper()
	effect, ok := controller.PendingClosureEffect()
	if !ok || effect.Closure.Request.IncidentID != incidentID {
		t.Fatalf("controller did not persist closure for %s", incidentID)
	}
	if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	fastloopGateWait(t, 7*time.Minute, func() bool { return len(controller.closureResults) > 0 })
	controller.processBrokerCompletions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	state := controller.ExportState()
	if state.ClosureState != "committed" || state.ClosureReceipt == nil {
		t.Fatalf("native broker did not commit controller closure: %s", state.ClosureState)
	}
	return *state.ClosureReceipt
}

func fastloopGateAcknowledge(t *testing.T, ctx context.Context, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("controller did not persist closure acknowledgment")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	fastloopGateWait(t, 90*time.Second, func() bool { return len(controller.acknowledgmentResults) > 0 })
	controller.processBrokerCompletions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestHermeticFastloopRealGoGate(t *testing.T) {
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "-mod=mod -p=2")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	client := fastloopClient("good:v1")
	accepted := nativeRepository(t)
	nativeMemoryFixture(t, accepted)
	fixture := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "playbook_program_app", ".sdo")
	copyFastloopTree(t, filepath.Join(fixture, "diagnostics"), filepath.Join(accepted, ".sdo", "diagnostics"))
	diagnostics := filepath.Join(accepted, ".sdo", "diagnostics")
	if err := os.MkdirAll(filepath.Join(diagnostics, "detectors", "incidents"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(diagnostics, "detectors", "missing_configmap"), filepath.Join(diagnostics, "detectors", "incidents", "missing_configmap")); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(diagnostics, "manifest.yaml")
	manifestData, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte(strings.ReplaceAll(string(manifestData), "./detectors/missing_configmap", "./detectors/incidents/missing_configmap")), 0644); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(accepted, ".sdo", "playbooks", "missing-configmap")
	if err := os.MkdirAll(missing, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(missing, "README.md"), []byte("---\nschema_version: 1\nowner: responder\nfault_class: missing-configmap\noriginating_incident: fixture\n---\n\nRestore the required `<RESOURCE_NAME>` ConfigMap from source.\n"), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, accepted, "add", ".sdo")
	nativeGit(t, accepted, "commit", "-qm", "seed diagnostics")
	var built *builder.Workspace
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(ctx context.Context, candidate string, changed []string) error {
		if !strings.Contains(strings.Join(changed, " "), ".sdo/playbooks/service-targetport-mismatch/") {
			return nil
		}
		t.Log("validating reflected program")
		workspace, err := builder.BuildWorkspace(ctx, builder.Config{ApplicationRoot: candidate, DiagnosticsRoot: filepath.Join(candidate, ".sdo", "diagnostics"), OperatorRoot: ".."})
		if err != nil {
			return err
		}
		if _, err := workspace.ValidateAndBuild(ctx); err != nil {
			workspace.Close()
			return err
		}
		t.Log("validated reflected program")
		built = workspace
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if built != nil {
			built.Close()
		}
	}()
	captureRoot := t.TempDir()
	reflected := 0
	worker := RegisteredReflectionWorker{AgentID: "sdo-reflector", Outcomes: memory.OpenReader(accepted), Catalog: memory.OpenCatalogReader(accepted), Timeout: time.Minute, SessionMode: "fresh", CorpusSessionRoot: captureRoot}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		reflected++
		if reflected > 1 {
			response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "program already covers this class"})
			if err != nil {
				return nil, err
			}
			return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
		}
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		incidentID := request.GetClosure().GetRequest().GetIncidentId()
		captureDir, err := memory.IncidentCorpusDir(captureRoot, incidentID)
		if err != nil {
			return nil, err
		}
		proposal := fastloopProgramProposal(t, captureDir, incidentID)
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ArtifactProposal: proposal})
		if err != nil {
			return nil, err
		}
		result := &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}
		wire, err := protojson.Marshal(result)
		if err != nil {
			return nil, err
		}
		if len(wire) >= 700<<10 {
			t.Fatalf("corpus reference exceeded reflection transport: %d", len(wire))
		}
		return result, nil
	})
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, Reflector: worker, ResponderBackend: "scripted", ResponderModel: "none", SessionRoot: captureRoot}
	provider := fastloopObjects{client: client}
	peerService, _ := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	peerService.Name = "peer"
	peerService.Spec.Selector = map[string]string{"app": "peer"}
	peerService.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	if _, err := client.CoreV1().Services("app").Create(ctx, peerService, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	peerPod, _ := client.CoreV1().Pods("app").Get(ctx, "api-pod", metav1.GetOptions{})
	peerPod.Name = "peer-pod"
	peerPod.Labels = map[string]string{"app": "peer"}
	peerPod.Spec.Containers[0].Image = "good:v1"
	if _, err := client.CoreV1().Pods("app").Create(ctx, peerPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	apiDeployment, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	apiDeployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
	if _, err := client.AppsV1().Deployments("app").Update(ctx, apiDeployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	apiPod, err := client.CoreV1().Pods("app").Get(ctx, "api-pod", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	apiPod.Spec.Containers[0].Image = "good:v1"
	if _, err := client.CoreV1().Pods("app").Update(ctx, apiPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	start := time.Now().UTC()
	before, broken := driftService(8080), driftService(9999)
	jaegerBefore, jaegerLive := driftService(8080), driftService(9999)
	jaegerBefore.Name, jaegerLive.Name = "jaeger", "jaeger"
	jaegerChange := objectChange(t, jaegerBefore, jaegerLive, "v1", "Service")
	jaegerChange.Adoption = true
	changes := &fixedObjectChanges{changes: []ObjectChange{jaegerChange, objectChange(t, before, broken, "v1", "Service")}}
	driftWorker := &fastloopDriftWorker{}
	var buildDriftTask func(context.Context, string, []sdk.DriftEvent) (*contracts.DriftInvestigationTask, error)
	first, firstDispatch, firstRequest := fastloopGateIncident(t, ctx, client, native, accepted, captureRoot, "fastloop-first", start, false, func(controller *Controller) {
		buildDriftTask = func(ctx context.Context, batchID string, events []sdk.DriftEvent) (*contracts.DriftInvestigationTask, error) {
			snapshot, err := memory.OpenCatalogReader(accepted).Snapshot(ctx)
			if err != nil {
				return nil, err
			}
			if snapshot.GetAcceptedCommit() != controller.CurrentAcceptedCommit() {
				return nil, fmt.Errorf("drift snapshot differs from current accepted commit")
			}
			view, err := provider.Snapshot(ctx)
			if err != nil {
				return nil, err
			}
			if err := memory.CaptureIncidentSnapshot(captureRoot, batchID, "open@detect", view, "app"); err != nil {
				return nil, err
			}
			return &contracts.DriftInvestigationTask{SchemaVersion: ProtocolSchemaVersion, BatchId: batchID, AcceptedCommit: controller.CurrentAcceptedCommit(), MemorySnapshot: snapshot, CaptureId: batchID, Events: driftEventsContract(events)}, nil
		}
		controller.ConfigureDrift(DriftControllerConfig{Events: changes, View: func(snapshot sdk.DetectionContext) program.ReadView {
			services := snapshot.(sdktest.Snapshot).ServiceList
			changed := false
			for _, service := range services {
				if service.Name == "api" && service.Spec.Ports[0].TargetPort.IntVal == 9999 {
					changed = true
				}
			}
			return fastloopDriftView{driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: changed}}}
		}, Investigation: DriftInvestigationConfig{Worker: driftWorker, BatchWindow: time.Nanosecond, BuildTask: buildDriftTask}})
	})
	if len(driftWorker.requests) != 1 || len(firstRequest.Findings) != 1 || firstRequest.Findings[0].DetectorID != "drift/investigator" || len(firstRequest.DriftEvents) != 2 {
		t.Fatalf("first incident did not follow investigator verdict: %+v", firstRequest)
	}
	if firstRequest.DetectorHistory[0].Status != DetectorEvaluationClear || !firstRequest.DetectorHistory[0].EvaluatedAt.Before(start) {
		t.Fatal("health was not clear before latent incident dispatch")
	}
	if capture, err := memory.IncidentCorpusDir(captureRoot, firstRequest.IncidentID); err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(filepath.Join(capture, "open@detect.json")); err != nil {
		t.Fatalf("reflection lost drift capture: %v", err)
	}
	port := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	if err := (TypedPlanClient{Client: client}).Patch(ctx, port, types.MergePatchType, []byte(`{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`)); err != nil {
		t.Fatal(err)
	}
	changes.changes = []ObjectChange{objectChange(t, broken, before, "v1", "Service")}
	firstResult := fastloopGateResult(firstRequest, []sdk.ObjectRef{port})
	fixed, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.CaptureIncidentSnapshot(captureRoot, firstRequest.IncidentID, "fixed", fixed, "app"); err != nil {
		t.Fatal(err)
	}
	closure := fastloopGateClose(t, ctx, first, firstDispatch, firstResult, start)
	if closure.HealthClearedAt == nil {
		t.Fatal("controller did not verify the first repair")
	}
	if closure.HealthClearedAt.Before(closure.DispatchedAt) {
		t.Fatalf("latent repair cutoff preceded dispatch: %s < %s", closure.HealthClearedAt, closure.DispatchedAt)
	}
	receipt := fastloopGateProcessClosure(t, ctx, first, firstRequest.IncidentID)
	if receipt.ReflectionCommit == "" {
		t.Fatal("real broker did not accept reflected program")
	}
	if first.CurrentAcceptedCommit() != receipt.ReflectionCommit {
		t.Fatalf("controller retained stale accepted commit: %s", first.CurrentAcceptedCommit())
	}
	if task, err := buildDriftTask(ctx, "drift-after-commit", []sdk.DriftEvent{firstRequest.DriftEvents[0]}); err != nil || task.GetAcceptedCommit() != receipt.ReflectionCommit {
		t.Fatalf("post-commit drift task: %+v %v", task, err)
	}
	changes.changes = []ObjectChange{jaegerChange}
	if err := first.StepEvents(ctx, start.Add(1500*time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	if len(driftWorker.requests) != 1 || first.driftInvestigation.Pending() != 0 {
		t.Fatal("unchanged pre-existing difference was redispatched")
	}
	if built == nil {
		t.Fatal("program builder did not validate accepted reflection")
	}
	if expectation, pending, err := native.PendingControllerRollout(receipt.ReflectionCommit); err != nil {
		t.Fatal(err)
	} else if pending {
		if err := native.RecordControllerRollout(ControllerRolloutReceipt{Expectation: expectation, BuildArtifact: filepath.Join(built.Directory, "sdo-controller"), ControllerRevision: receipt.ReflectionCommit, ObservedDetectorFingerprint: expectation.AfterDetectorFingerprint, Success: true, VerifiedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	fastloopGateAcknowledge(t, ctx, first)
	if _, err := os.Stat(firstRequest.RepositoryWorktree); !os.IsNotExist(err) {
		t.Fatalf("acknowledged first proposal checkout remains: %v", err)
	}
	for _, name := range []string{
		filepath.Join(".sdo", "playbooks", "service-targetport-mismatch", "diagnose.go"),
		filepath.Join(".sdo", "corpus", "targetport-"+firstRequest.IncidentID, "open.json"),
	} {
		if _, err := os.Stat(filepath.Join(accepted, name)); err != nil {
			t.Fatalf("accepted reflection omitted %s: %v", name, err)
		}
	}
	stored, err := memory.OpenReader(accepted).Get(ctx, firstRequest.IncidentID)
	if err != nil || stored.Record.GetClassification() != "success" {
		t.Fatalf("broker outcome: %+v %v", stored, err)
	}
	deployment, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if image := deployment.Spec.Template.Spec.Containers[0].Image; image != "good:v1" {
		t.Fatalf("rendered source restore lost deployed image: %s", image)
	}

	peerService, err = client.CoreV1().Services("app").Get(ctx, "peer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	peerService.ManagedFields = driftService(8080).ManagedFields
	peerService.Spec.Ports[0].Protocol = corev1.ProtocolTCP
	healthyPeer := peerService.DeepCopy()
	peerService.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	peerService.ManagedFields = append(peerService.ManagedFields, metav1.ManagedFieldsEntry{Manager: "kubectl-patch", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: healthyPeer.ManagedFields[0].FieldsV1})
	if _, err := client.CoreV1().Services("app").Update(ctx, peerService, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	change := objectChange(t, healthyPeer, peerService, "v1", "Service")
	secondSnapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	classified, _, err := (DriftClassifier{Programs: registry}).Classify(ctx, change, driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: secondSnapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}, start.Add(2*time.Second))
	if err != nil || classified == nil || classified.Classification != sdk.DriftFault {
		t.Fatalf("changed peer was not classified: %+v %v; before=%+v after=%+v owned=%+v managed=%+v", classified, err, flattenDriftObject(change.Old), flattenDriftObject(change.New), ownedDriftPaths(change.Old, "sdo-deployer"), change.Old.GetManagedFields())
	}
	second, secondDispatch, secondRequest := fastloopGateIncident(t, ctx, client, native, accepted, captureRoot, "fastloop-second", start.Add(2*time.Second), true, func(controller *Controller) {
		controller.ConfigureDrift(DriftControllerConfig{Events: &fixedObjectChanges{changes: []ObjectChange{change}}, Programs: registry, View: func(snapshot sdk.DetectionContext) program.ReadView {
			return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
		}})
	})
	if len(secondRequest.DriftEvents) != 1 || secondRequest.DriftEvents[0].Classification != sdk.DriftFault || secondRequest.DriftEvents[0].ProgramID != "service-targetport-mismatch" {
		t.Fatalf("second fault missed sweep-on-change: %+v", secondRequest.DriftEvents)
	}
	secondView, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := sdktest.CaptureSnapshot(secondView, "app")
	if err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(t.TempDir(), "second.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	programDir := filepath.Join(built.Directory, "cmd", "sweep")
	if err := os.MkdirAll(programDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(programDir, "main.go"), []byte(strings.Replace(fastloopProgramRunner, "app-diagnostics/generated", built.ModulePath+"/generated", 1)), 0644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "sweep")
	command := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", bin, "./cmd/sweep")
	command.Dir = built.Directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build reflected program runner: %v: %s", err, output)
	}
	output, err := exec.CommandContext(ctx, bin, snapshotPath).CombinedOutput()
	if err != nil {
		t.Fatalf("run reflected program: %v: %s", err, output)
	}
	var run struct {
		Result string `json:"result"`
		Path   string `json:"path"`
		Value  int32  `json:"value"`
	}
	if err := json.Unmarshal(output, &run); err != nil {
		t.Fatal(err)
	}
	if run.Result != string(program.Match) || run.Path != "spec.ports[0].targetPort" || run.Value != 8080 {
		t.Fatalf("compiled program did not reuse learned fix: %+v", run)
	}
	peer := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}
	if err := (TypedPlanClient{Client: client}).Patch(ctx, peer, types.MergePatchType, []byte(`{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`)); err != nil {
		t.Fatal(err)
	}
	secondResult := fastloopGateResult(secondRequest, []sdk.ObjectRef{peer})
	secondResult.ProgramRuns = []ProgramRun{{ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{"service": peer}, Verdict: program.Verdict{Result: program.NoMatch}}}
	secondResult.ProgramToolCalls = []ToolCall{{Command: "diagnose"}, {Command: "plan"}, {Command: "apply"}, {Command: "verify"}}
	secondResult.ResponseLoopsUsed = 1
	secondClosure := fastloopGateClose(t, ctx, second, secondDispatch, secondResult, start.Add(2*time.Second))
	if secondClosure.HealthClearedAt == nil {
		t.Fatal("controller did not verify the second repair")
	}
	secondReceipt := fastloopGateProcessClosure(t, ctx, second, secondRequest.IncidentID)
	if secondReceipt.OutcomeCommit == "" {
		t.Fatal("second closure lacks an outcome commit")
	}
	fastloopGateAcknowledge(t, ctx, second)
	if _, err := os.Stat(secondRequest.RepositoryWorktree); !os.IsNotExist(err) {
		t.Fatalf("acknowledged second proposal checkout remains: %v", err)
	}
	secondStored, err := memory.OpenReader(accepted).Get(ctx, secondRequest.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if secondStored.Record.GetClassification() != "success" {
		t.Fatalf("second outcome classification: %s", secondStored.Record.GetClassification())
	}
	if reflected != 2 {
		t.Fatalf("reflection decisions: %d", reflected)
	}
}
