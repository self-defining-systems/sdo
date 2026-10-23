package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type menuHealthDetector struct{}

func (menuHealthDetector) Spec() sdk.DetectorSpec {
	spec := (fastloopDetector{}).Spec()
	spec.ID = "menu-health"
	return spec
}
func (menuHealthDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if len(view.Services()) == 1 && len(view.Deployments()) == 1 && view.Services()[0].Spec.Ports[0].TargetPort.IntVal == 8080 && view.Deployments()[0].Status.ReadyReplicas == 1 {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: "menu-health", RuleID: "bootstrap", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: menuService, Summary: "synthetic backend bootstrap is incomplete", Evidence: "routing and readiness disagree with intended behavior", Playbooks: []string{"base"}, ParameterBindings: map[string]sdk.ObjectRef{"service": menuService, "deployment": menuDeployment}}}, nil
}

// Hermetic scripted e2e: real controller, KubernetesJobDispatcher, file tools,
// Python CLI, assurance Codex Turn/Rollout, broker/outcome derivation and strict
// benchmark receipt checker. Only the Kubernetes API is synthetic. No cluster,
// model, benchmark verdict or production-to-benchmark import is involved.
func TestResponderMenuScriptedControllerStrictReceiptRoundThree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	template, cluster := menuThirdRound(t)
	// An indexed env slot intentionally exercises the historical spelling bypass.
	// Placeholder-only fixture: no Secret object is fetched or credential exists.
	containers, _, _ := unstructured.NestedSlice(cluster.objects[menuDeployment].Object, "spec", "template", "spec", "containers")
	containers[0].(map[string]any)["env"] = []any{map[string]any{"name": "SYNTHETIC_FLAG", "value": "placeholder"}}
	_ = unstructured.SetNestedSlice(cluster.objects[menuDeployment].Object, containers, "spec", "template", "spec", "containers")
	configureSyntheticMenu(t, template, cluster)
	programs := []menuB3Program{{"restart-only"}, {"routing-then-restart"}, {"secret-env-index"}}
	var registryErr error
	template.Programs, registryErr = program.NewRegistry([]program.Diagnoser{programs[0], programs[1], programs[2]}, []program.Mitigator{programs[0], programs[1]})
	if registryErr != nil {
		t.Fatal(registryErr)
	}
	template.MenuConfig.Plans = []ResponderMenuPlan{{ID: "secret-env-index", ProgramID: "secret-env-index", Bindings: map[string]sdk.ObjectRef{"service": menuService, "deployment": menuDeployment}, Plan: program.Plan{Actions: []program.Action{program.SetField{Ref: menuDeployment, Path: "spec.template.spec.containers[0].env[0]", Value: map[string]any{"name": "SYNTHETIC_FLAG", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "placeholder", "key": "placeholder"}}}}}}}}
	mount := t.TempDir()
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(mount, "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(mount, "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none", Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "synthetic menu fixture needs no learned artifact"}, nil
	})}
	revision := nativeGit(t, accepted, "rev-parse", "HEAD")
	client := fake.NewSimpleClientset()
	loops := make(chan *ResponderLoop, 4)
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "synthetic/responder:scripted"), AgentID: testIncidentAgentID(t), ServiceAccount: "synthetic-worker", RepositoryPVC: "synthetic-repository", RepositoryMountPath: mount, PollInterval: time.Millisecond, Catalog: fixedMemoryCatalog{commit: revision}, RequireMemorySnapshot: true, LoopFactory: func(request IncidentRequest) *ResponderLoop {
		loop := NewResponderLoop(request, template.Programs, cluster, []sdk.Detector{menuHealthDetector{}}, template.Executor, 0)
		loop.ReadView = template.ReadView
		loop.MenuConfig = template.MenuConfig
		loop.DiagnosisSettleDelay = time.Nanosecond
		loops <- loop
		return loop
	}}
	start := time.Now().UTC()
	config := ControllerConfig{Application: "app", Namespace: "app", SourceCommit: revision, DeployedCommit: revision, RepositoryWorktree: accepted, CorpusSessionRoot: mount, ResponseTimeout: 10 * time.Minute, VerificationTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions"}
	controller, err := NewController(config, []sdk.Detector{menuHealthDetector{}}, cluster, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	controller.OnError = func(err error) { t.Logf("controller error: %v", err) }
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	workspaceEffect, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("missing native workspace effect")
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, workspaceEffect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	if err := controller.Step(ctx, start.Add(time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, controller)
	job := awaitJob(t, client, IncidentJobName(workspaceEffect.IncidentID))
	job.UID = types.UID("synthetic-worker-uid")
	if _, err := client.BatchV1().Jobs("demo").Update(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !containsEnvironment(job.Spec.Template.Spec.Containers[0].Env, "SDO_MITIGATION_SESSION", "1") {
		t.Fatal("dispatcher did not enable original-session recovery")
	}
	var loop *ResponderLoop
	for len(loops) > 0 {
		loop = <-loops
	}
	if loop == nil {
		t.Fatal("dispatcher did not create responder loop")
	}
	sessionDir := ""
	for _, volume := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if volume.MountPath == "/sdo/session" {
			sessionDir = filepath.Join(mount, volume.SubPath)
		}
	}
	if sessionDir == "" {
		t.Fatal("missing isolated worker session")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(mount, "scripted-menu.py")
	if err := os.WriteFile(script, []byte(menuScriptedPython), 0600); err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("uv")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, python, "run", "python", script, loop.Request.IncidentID, sessionDir)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONPATH="+root, "SDO_TOOLS_DIR="+filepath.Join(sessionDir, "tools"), "CODEX_HOME="+filepath.Join(sessionDir, "codex"), "SDO_SESSION_SCRATCH="+filepath.Join(sessionDir, "scratch"), "SDO_MITIGATION_SESSION=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scripted menu failed: %v\n%s", err, output)
	}
	var scripted struct {
		SessionID string `json:"session_id"`
		Refused   string `json:"refused"`
		First     string `json:"first"`
		Second    string `json:"second"`
		Rounds    int    `json:"rounds"`
	}
	if err := json.Unmarshal(output, &scripted); err != nil {
		t.Fatalf("script result: %v %s", err, output)
	}
	if scripted.Refused != "STATUS_REFUSED" || scripted.First != "STATUS_UNRESOLVED" || scripted.Second != "STATUS_SUCCESS" || scripted.Rounds != 3 {
		t.Fatalf("menu did not traverse failed report to third-round success: %s", output)
	}
	if cluster.forbiddenReads != 0 || len(cluster.patches) != 5 {
		t.Fatal("Secret negative case read opaque content or performed an extra mutation")
	}
	result := completedResult(loop.Request.IncidentID)
	result.ResponderSessionID = scripted.SessionID
	result.Timing.StartedAt = start
	result.Timing.CompletedAt = time.Now().UTC()
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "synthetic routing and bootstrap readiness mismatch", Resources: []sdk.ObjectRef{menuService, menuDeployment}, Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "synthetic-cluster", Observation: "routing and readiness recovered after menu rounds"}}, ExplainedDetectors: []string{"menu-health"}}}
	result.VerificationEvidence = []VerificationEvidence{{Name: "fresh independent menu health", Passed: true, Details: "bound cause and health clear", ObservedAt: result.Timing.CompletedAt}}
	payload, err := testAgentResultPayload(t, loop.Request, result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result"}, Data: map[string]string{jobResultDataKey: string(payload)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(ctx, time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatalf("controller did not independently close menu repair: dispatch=%s, done=%t, error=%s", controller.dispatchState, controller.responderDone, controller.dispatchError)
	}
	if closure.Result == nil || len(closure.Result.PlanApplications) != 0 || len(closure.Result.RepairActions) == 0 || closure.Result.ResponseLoopsUsed != 5 {
		t.Fatalf("menu repair evidence crossed trust path or lost incident budget: %+v", closure.Result)
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(loop.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.Acknowledged || ledger.OutcomeDocument.Classification != "success" {
		t.Fatal("broker did not independently derive/acknowledge success")
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
	strict := exec.CommandContext(ctx, python, "run", "python", "-c", menuStrictPython, ledgerPath, strictPath, job.Name, string(job.UID))
	strict.Dir = root
	strict.Env = append(os.Environ(), "PYTHONPATH="+root)
	strictOutput, err := strict.CombinedOutput()
	if err != nil {
		t.Fatalf("strict receipt rejected: %v\n%s", err, strictOutput)
	}
	if !strings.Contains(string(strictOutput), "strict receipt PASS: sdo_mitigated") {
		t.Fatalf("strict receipt missing: %s", strictOutput)
	}
	// Opt-in artifact export keeps run evidence outside the repository.
	if destination := os.Getenv("SDO_MENU_E2E_ARTIFACTS"); destination != "" {
		if err := os.MkdirAll(destination, 0700); err != nil {
			t.Fatal(err)
		}
		for _, file := range []string{ledgerPath, strictPath, filepath.Join(sessionDir, "scripted-report.json")} {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destination, filepath.Base(file)), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("scripted e2e PASS: indexed-env=%s, first=%s, second=%s, second-option rounds=%d, incident rounds=%d, %s", scripted.Refused, scripted.First, scripted.Second, scripted.Rounds, closure.Result.ResponseLoopsUsed, strings.TrimSpace(string(strictOutput)))
}

const menuScriptedPython = `import io, json, os, shlex, sys
from pathlib import Path
from benchmarks.sregym.assurance.scripted_codex.turn import Turn
from benchmarks.sregym.assurance.scripted_codex.wire import EventStream, Rollout
from sdo.agents.responder.session_recovery import open_recovery
incident, directory = sys.argv[1], Path(sys.argv[2])
recovery = open_recovery(incident, "codex")
assert recovery is not None and recovery.resume_session_id() is None
rollout = Rollout.create(Path(os.environ["CODEX_HOME"]), cwd=str(Path.cwd()))
recovery.checkpoint(rollout.session_id)
turn = Turn("responder", 175, Path.cwd(), rollout, EventStream(io.StringIO()))
def command(option, rounds):
    code, text = turn.run(shlex.join([sys.executable, "-m", "sdo.agents.responder.tools_cli", "mitigate", option, "--rounds", str(rounds)]))
    assert code == 0, text
    response = json.loads(text.splitlines()[0])
    return response["report"]
refused = command("secret-env-index", 1)
assert refused["status"] == "STATUS_REFUSED" and refused["failed_phase"] == "planning", refused
assert refused["reason_code"] == "plan_preflight_refused", refused
assert not any(r.get("actions") for r in refused.get("rounds", [])), refused
assert refused["remaining_batches"] == 4, refused
first = command("restart-only", 1)
assert first["status"] == "STATUS_UNRESOLVED" and first["remaining_batches"] == 3, first
second = command("routing-then-restart", 3)
assert second["status"] == "STATUS_SUCCESS" and len(second["rounds"]) == 3 and second.get("remaining_batches", 0) == 0, second
assert refused["session_id"] == first["session_id"] == second["session_id"]
assert first["attempt_id"] != second["attempt_id"]
assert [r["round_index"] for r in second["rounds"]] == [1,2,3]
assert all(r["barrier_ids"] for r in second["rounds"])
turn.finish("same responder session observed first failure and verified the alternate option")
summary = dict(session_id=rollout.session_id, refused=refused["status"], first=first["status"], second=second["status"], rounds=len(second["rounds"]))
(directory / "scripted-report.json").write_text(json.dumps(dict(summary=summary, refused=refused, first=first, second=second)))
print(json.dumps(summary))
`
const menuStrictPython = `import json, sys
from pathlib import Path
from benchmarks.sregym.protocol.native_receipt import native_production_receipt
from benchmarks.sregym.protocol.receipt import validate_production_receipt
ledger=json.loads(Path(sys.argv[1]).read_text())
receipt=native_production_receipt(ledger, namespace="app", control_namespace="demo", controller_image="synthetic/controller:hermetic", responder_image="synthetic/responder:scripted", validator_image="synthetic/validator:hermetic", controller_pod_uid="synthetic-controller-uid", responder_job_evidence=[dict(incident_id=ledger["incident_id"], job_name=sys.argv[3], job_uid=sys.argv[4], succeeded=True)])
validate_production_receipt(receipt)
Path(sys.argv[2]).write_text(json.dumps(receipt,indent=2))
print("strict receipt PASS: "+receipt["resolution"])
`
