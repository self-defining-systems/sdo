package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	jsonpatch "github.com/evanphx/json-patch"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/worker_registry/registrytest"
)

type productionMenuProgram struct {
	id   string
	port int
}

func (p productionMenuProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: p.port}}}
}
func (p productionMenuProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if len(view.Services()) != 1 {
		return program.Verdict{Result: program.Unknown}, nil
	}
	if view.Services()[0].Spec.Ports[0].TargetPort.IntVal != 8080 {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p productionMenuProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromParam("port")}}}, nil
}

type productionMenuHealth struct{}

func (productionMenuHealth) Spec() sdk.DetectorSpec {
	spec := (fastloopDetector{}).Spec()
	spec.ID = "production-menu-health"
	spec.Interval = 20 * time.Millisecond
	spec.Watches = []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}
	return spec
}
func (productionMenuHealth) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if len(view.Services()) == 1 && view.Services()[0].Spec.Ports[0].TargetPort.IntVal == 8080 {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: "production-menu-health", RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: menuService, Summary: "synthetic route is unhealthy", Evidence: "independent route port check", Playbooks: []string{"base"}, ParameterBindings: map[string]sdk.ObjectRef{"service": menuService}}}, nil
}

// The watch cache's causal write barrier relies on API resource versions.
func productionMenuClient(service *corev1.Service) *productionMenuAPI {
	client := fake.NewSimpleClientset(service)
	// client-go's tracker does not advance resourceVersion on patches. Reusing
	// the pre-write version lets AwaitWrites certify an older cached snapshot.
	client.PrependReactor("patch", "services", func(action ktesting.Action) (bool, kruntime.Object, error) {
		patch := action.(ktesting.PatchAction)
		if patch.GetPatchType() != types.MergePatchType {
			return false, nil, nil
		}
		current, err := client.Tracker().Get(patch.GetResource(), patch.GetNamespace(), patch.GetName())
		if err != nil {
			return true, nil, err
		}
		data, err := json.Marshal(current)
		if err != nil {
			return true, nil, err
		}
		data, err = jsonpatch.MergePatch(data, patch.GetPatch())
		if err != nil {
			return true, nil, err
		}
		next := new(corev1.Service)
		if err := json.Unmarshal(data, next); err != nil {
			return true, nil, err
		}
		version, err := strconv.ParseUint(current.(*corev1.Service).ResourceVersion, 10, 64)
		if err != nil {
			return true, nil, err
		}
		next.ResourceVersion = strconv.FormatUint(version+1, 10)
		if err := client.Tracker().Update(patch.GetResource(), next, patch.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, next.DeepCopy(), nil
	})
	return &productionMenuAPI{client}
}

func TestProductionMenuFixtureAdvancesServiceWriteVersions(t *testing.T) {
	ctx := context.Background()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: menuService.Name, Namespace: "app", ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt(9999)}}}}
	client := productionMenuClient(service)
	previous := service.ResourceVersion
	for _, port := range []int{7777, 8080} {
		patch := []byte(fmt.Sprintf(`{"spec":{"ports":[{"port":80,"targetPort":%d}]}}`, port))
		updated, err := client.CoreV1().Services("app").Patch(ctx, menuService.Name, types.MergePatchType, patch, metav1.PatchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !resourceVersionAtLeast(updated.ResourceVersion, previous) || updated.ResourceVersion == previous {
			t.Fatalf("fixture write did not publish a new causal version: before=%q after=%q", previous, updated.ResourceVersion)
		}
		previous = updated.ResourceVersion
	}
}

// Starts RunWithOptions with its real job dispatcher and LoopFactory,
// the ordinary executor and broker; only the Kubernetes API is synthetic.
func TestProductionResponderMenuScriptedFactoryStrictReceipt(t *testing.T) {
	testProductionResponderMenuFactory(t, true)
}
func TestProductionResponderMenuExplicitOptOut(t *testing.T) {
	testProductionResponderMenuFactory(t, false)
}
func testProductionResponderMenuFactory(t *testing.T, enabled bool) {
	launch := "0"
	if enabled {
		launch = "1"
	}
	t.Setenv("SDO_RESPONDER_MENU", launch)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	mount := t.TempDir()
	accepted := nativeRepository(t)
	nativeMemoryFixture(t, accepted)
	// Declare the independently healthy application before injecting the bad
	// route, so production source-inventory closure has accepted evidence.
	healthyService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: menuService.Name, Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "backend"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt(8080)}}}}
	revision := seedDeclaredInventory(t, accepted, "app", []kruntime.Object{healthyService})
	repository, err := broker.New(accepted, filepath.Join(mount, "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := &NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(mount, "ledger"), SessionRoot: mount, Deriver: OutcomeDeriverFunc(func(ctx context.Context, closure IncidentClosure, repairCommit, backend, model string) (DerivedOutcome, error) {
		t.Logf("closure evidence: status=%s repairs=%d plans=%d calls=%d dispatch_error=%s", closure.Result.Status, len(closure.Result.RepairActions), len(closure.Result.PlanApplications), len(closure.Result.ProgramToolCalls), closure.DispatchError)
		return (ProtoOutcomeDeriver{}).Derive(ctx, closure, repairCommit, backend, model)
	}), ResponderBackend: "scripted", ResponderModel: "none", Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, outcome *contracts.OutcomeRecord, _ string, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
		var gaps []*contracts.SdkGap
		for _, cause := range outcome.ConfirmedRootCauses {
			gaps = append(gaps, &contracts.SdkGap{FaultClass: "synthetic-fixture", Primitive: "fixture-capability", Reason: "menu transport fixture has no executable learning observations", ScratchScript: "scratch/test", Resources: cause.Resources})
		}
		return ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "synthetic menu transport fixture", SDKGaps: gaps}, nil
	})}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: menuService.Name, Namespace: "app", UID: types.UID("fixture-uid"), ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "backend"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt(9999)}}}}
	bad, good := productionMenuProgram{"try-wrong-port", 7777}, productionMenuProgram{"restore-route", 8080}
	registry, err := program.NewRegistry([]program.Diagnoser{bad, good}, []program.Mitigator{bad, good})
	if err != nil {
		t.Fatal(err)
	}
	client := productionMenuClient(service)
	// client-go's object tracker omits list resource versions. Supply an opaque
	// direct-API version in this synthetic transport; never use this in production.
	client.PrependReactor("list", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		list, err := client.Tracker().List(action.GetResource(), action.(ktesting.ListActionImpl).Kind, action.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		object, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(list)
		if err != nil {
			return true, nil, err
		}
		_ = unstructured.SetNestedField(object, "synthetic-list-current", "metadata", "resourceVersion")
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object, list); err != nil {
			return true, nil, err
		}
		return true, list, nil
	})
	jobs := make(chan string, 4)
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		job.UID = types.UID("scripted-worker-uid")
		jobs <- job.Name
		return false, nil, nil
	})
	logs := &lockedLines{}
	reader, writer := io.Pipe()
	go collectLines(reader, logs)
	done := make(chan error, 1)
	start := time.Now().UTC()
	go func() {
		done <- RunWithOptions(ctx, []sdk.Detector{productionMenuHealth{}}, RuntimeOptions{Args: []string{"--toolchain-image", "synthetic/controller:compiler", "--namespace", "app", "--control-namespace", "demo", "--app-root", accepted, "--source-commit", revision, "--deployed-manifest-path", "app.yaml", "--deployed-commit", revision, "--repository-pvc", "synthetic-repository", "--repository-mount-path", mount, "--responder-credentials-secret", "unused-placeholder", "--responder-image", "synthetic/responder:scripted", "--agent-registration-directory", registrytest.Directory(), "--repair-policy", "recorded-actions", "--state-baseline=false", "--synthetic-traffic=false", "--clean-responder-helpers=false", "--firing-telemetry-path", "off", "--lifecycle-telemetry-path", "off", "--verification-timeout", "5s", "--exit-after-closure"}, Client: client, Programs: registry, NativeBroker: native, Stdout: writer, Stderr: writer})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("production controller did not stop")
		}
		_ = writer.Close()
		_ = reader.Close()
	})
	var jobName string
	select {
	case jobName = <-jobs:
	case err := <-done:
		done <- err
		t.Fatalf("factory exited: %v; %s", err, logs)
	case <-ctx.Done():
		t.Fatalf("no production responder job: %s", logs)
	}
	job := awaitJob(t, client.Clientset, jobName)
	if !enabled {
		if containsEnvironment(job.Spec.Template.Spec.Containers[0].Env, "SDO_MITIGATION_SESSION", "1") {
			t.Fatal("menu enabled without explicit launch input")
		}
		return
	}
	if !containsEnvironment(job.Spec.Template.Spec.Containers[0].Env, "SDO_MITIGATION_SESSION", "1") {
		t.Fatal("real LoopFactory did not configure the menu")
	}
	taskMap, err := client.CoreV1().ConfigMaps("demo").Get(ctx, jobName+"-request", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{}
	if err := protojson.Unmarshal([]byte(taskMap.Data[jobRequestDataKey]), task); err != nil {
		t.Fatal(err)
	}
	typedRequest := &contracts.IncidentRequest{}
	if err := task.GetRequest().UnmarshalTo(typedRequest); err != nil {
		t.Fatal(err)
	}
	request := incidentRequestState(typedRequest)
	sessionDir := ""
	for _, volume := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if volume.MountPath == "/sdo/session" {
			sessionDir = filepath.Join(mount, volume.SubPath)
		}
	}
	if sessionDir == "" {
		t.Fatal("missing session mount")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("uv")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(mount, "factory-worker.py")
	if err := os.WriteFile(script, []byte(productionMenuScript), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, python, "run", "--no-sync", "python", script, request.IncidentID, sessionDir)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONPATH="+root, "SDO_TOOLS_DIR="+filepath.Join(sessionDir, "tools"), "CODEX_HOME="+filepath.Join(sessionDir, "codex"), "SDO_SESSION_SCRATCH="+filepath.Join(sessionDir, "scratch"), "SDO_MITIGATION_SESSION=1")
	output, err := cmd.Output()
	if err != nil {
		live, readErr := client.CoreV1().Services("app").Get(ctx, menuService.Name, metav1.GetOptions{})
		for _, action := range client.Actions() {
			if patch, ok := action.(ktesting.PatchAction); ok && patch.GetResource().Resource == "services" {
				t.Logf("Service write: %s", patch.GetPatch())
			}
		}
		t.Fatalf("factory scripted worker: %v; live Service=%+v read error=%v\n%s\n%s", err, live, readErr, output, logs)
	}
	var scripted struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(output, &scripted); err != nil {
		t.Fatal(err)
	}
	result := completedResult(request.IncidentID)
	result.ResponderSessionID = scripted.SessionID
	result.Timing.StartedAt = start
	result.Timing.CompletedAt = time.Now().UTC()
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "synthetic service routing mismatch", Resources: []sdk.ObjectRef{menuService}, Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "synthetic-api", Observation: "independent service route check"}}, ExplainedDetectors: []string{"production-menu-health"}}}
	result.VerificationEvidence = []VerificationEvidence{{Name: "fresh independent routing check", Passed: true, Details: "route restored", ObservedAt: result.Timing.CompletedAt}}
	payload, err := testAgentResultPayload(t, *request, result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: jobName + "-result"}, Data: map[string]string{jobResultDataKey: string(payload)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		done <- err
		if err != nil {
			t.Fatalf("production closure: %v; %s", err, logs)
		}
	case <-ctx.Done():
		t.Fatalf("no production closure: %s", logs)
	}
	ledger, err := native.readLedger(request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.Acknowledged || ledger.OutcomeDocument.Classification != "success" {
		t.Fatalf("production broker closure: acknowledged=%t classification=%s; %s", ledger.Acknowledged, ledger.OutcomeDocument.Classification, logs)
	}
	ledgerJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	ledgerPath := filepath.Join(mount, "native-ledger.json")
	if err := os.WriteFile(ledgerPath, ledgerJSON, 0600); err != nil {
		t.Fatal(err)
	}
	strictPath := filepath.Join(mount, "sdo_production_receipt_strict.json")
	strict := exec.CommandContext(ctx, python, "run", "python", "-c", menuStrictPython, ledgerPath, strictPath, jobName, string(job.UID))
	strict.Dir = root
	strict.Env = append(os.Environ(), "PYTHONPATH="+root)
	strictOutput, err := strict.CombinedOutput()
	if err != nil || !strings.Contains(string(strictOutput), "strict receipt PASS: sdo_mitigated") {
		t.Fatalf("strict factory receipt: %v %s", err, strictOutput)
	}
	if destination := os.Getenv("SDO_MENU_E2E_ARTIFACTS"); destination != "" {
		if err := os.MkdirAll(destination, 0700); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{ledgerPath, strictPath, filepath.Join(sessionDir, "factory-report.json")} {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destination, filepath.Base(path)), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("real RunWithOptions/LoopFactory: same-session UNRESOLVED then SUCCESS; %s", strings.TrimSpace(string(strictOutput)))
}

const productionMenuScript = `import io,json,os,shlex,sys
from pathlib import Path
from benchmarks.sregym.assurance.scripted_codex.turn import MAX_STRUCTURED_OUTPUT_CHARS,Turn
from benchmarks.sregym.assurance.scripted_codex.wire import EventStream,Rollout
from sdo.agents.responder.session_recovery import open_recovery
incident,directory=sys.argv[1],Path(sys.argv[2])
recovery=open_recovery(incident,"codex")
assert recovery is not None
rollout=Rollout.create(Path(os.environ["CODEX_HOME"]),cwd=str(Path.cwd()))
recovery.checkpoint(rollout.session_id)
turn=Turn("responder",175,Path.cwd(),rollout,EventStream(io.StringIO()))
def select(program):
 code,text=turn.run(shlex.join([sys.executable,"-m","sdo.agents.responder.tools_cli","--json","menu"]),max_output_chars=MAX_STRUCTURED_OUTPUT_CHARS)
 assert code==0,text
 menu=json.loads(text.splitlines()[0])["menu"]
 option=next(o["option_id"] for o in menu["options"] if o.get("program_id")==program)
 code,text=turn.run(shlex.join([sys.executable,"-m","sdo.agents.responder.tools_cli","--json","mitigate",option,"--rounds","1"]))
 assert code==0,text
 envelope=json.loads(text.splitlines()[0])
 full=json.loads(Path(envelope["details_file"]).read_text())
 assert len(full["actions"])==1,full
 return full["actions"][0]["report"]
first=select("try-wrong-port")
assert first["status"]=="STATUS_UNRESOLVED",first
second=select("restore-route")
assert second["status"]=="STATUS_SUCCESS",second
assert first["session_id"]==second["session_id"]
assert first["attempt_id"]!=second["attempt_id"]
turn.finish("read failed mitigation report and selected successful alternate in the same session")
(directory/"factory-report.json").write_text(json.dumps(dict(first=first,second=second)))
print(json.dumps(dict(session_id=rollout.session_id)))
`
