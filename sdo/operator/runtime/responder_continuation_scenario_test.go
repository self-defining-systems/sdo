package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Gate 1 B uses actual detector reads, program execution, session filesystem
// transport, the Python CLI, persisted continuation and the controller closure.
func TestHermeticResponderThreeFaultContinuation(t *testing.T) {
	t.Run("plan-apply", func(t *testing.T) { testHermeticResponderThreeFaultContinuation(t, false) })
	t.Run("mitigate", func(t *testing.T) { testHermeticResponderThreeFaultContinuation(t, true) })
}
func testHermeticResponderThreeFaultContinuation(t *testing.T, mitigate bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := fake.NewSimpleClientset()
	for _, name := range []string{"first", "second", "third"} {
		_, err := client.CoreV1().Services("app").Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	provider := fastloopObjects{client}
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.Namespace = "app"
	request.RepairPolicy = "recorded-actions"
	request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
	request.ResponseDeadline = time.Now().Add(time.Minute)
	request.MaxResponseLoops = 3
	snapshot, _ := provider.Snapshot(ctx)
	request.Findings, _ = (fastloopDetector{}).Detect(ctx, snapshot)
	for i := range request.Findings {
		request.Findings[i].Playbooks = []string{"service-targetport-mismatch"}
	}
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider}}, 3)
	if mitigate {
		loop.MenuConfig = productionResponderMenu()
	}
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, RequireMemorySnapshot: true, Catalog: fixedMemoryCatalog{request.RepositoryBaseCommit}, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	done := make(chan IncidentResult, 1)
	errs := make(chan error, 1)
	go func() { result, err := dispatcher.Dispatch(ctx, request); done <- result; errs <- err }()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	sessionPath := ""
	for _, mount := range first.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/sdo/session" {
			sessionPath = mount.SubPath
		}
	}
	tools := filepath.Join(dispatcher.RepositoryMountPath, sessionPath, "tools")
	detected := fastloopTool(t, tools, "detect", "--detector", "availability")
	if len(detected.Findings) != 3 {
		t.Fatalf("initial self-check: %+v", detected)
	}
	for _, name := range []string{"first", "second"} {
		if mitigate {
			fastloopTool(t, tools, "mitigate", "service-targetport-mismatch", "--service", "Service/app/"+name)
		} else {
			plan := fastloopTool(t, tools, "plan", "service-targetport-mismatch", "--bind", "service=Service/app/"+name)
			fastloopTool(t, tools, "apply", plan.PlanID)
		}
	}
	verify := fastloopTool(t, tools, "verify")
	if verify.Healthy || len(verify.Findings) != 1 || verify.Findings[0].PrimaryResource.Name != "third" {
		t.Fatalf("partial self-check: %+v", verify)
	}
	result := completedResult(request.IncidentID)
	result.ResponderSessionID = "same-scripted-session"
	publishContinuationResult(t, client, first, result)
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	feedback := ""
	for _, env := range second.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "SDO_RESPONSE_CONTINUATION" {
			feedback = env.Value
		}
	}
	var parsed responderFeedback
	if err = json.Unmarshal([]byte(feedback), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.StillBroken) != 1 || parsed.StillBroken[0].Resource.Name != "third" || parsed.StillBroken[0].Fingerprint == "" {
		t.Fatalf("remaining finding: %s", feedback)
	}
	if len(parsed.AlreadyDone) != 2 {
		t.Fatalf("both repairs must have receipts: %s", feedback)
	}
	for _, repair := range parsed.AlreadyDone {
		if repair.Kind != "program-application" || repair.ApplicationID == "" || len(repair.ProgramIDs) != 1 || repair.ProgramIDs[0] != "service-targetport-mismatch" || !repair.Receipt.Success {
			t.Fatalf("missing repair receipt: %+v", repair)
		}
	}
	for _, mount := range second.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/sdo/session" && mount.SubPath != sessionPath {
			t.Fatal("continuation replaced provider session")
		}
	}
	plan := fastloopTool(t, tools, "plan", "service-targetport-mismatch", "--bind", "service=Service/app/third")
	fastloopTool(t, tools, "apply", plan.PlanID)
	if verified := fastloopTool(t, tools, "verify"); !verified.Healthy {
		t.Fatalf("final self-check: %+v", verified)
	}
	publishContinuationResult(t, client, second, result)
	if err = <-errs; err != nil {
		t.Fatal(err)
	}
	final := <-done
	if final.Status != IncidentCompleted || final.ResponderSessionID != "same-scripted-session" {
		t.Fatalf("continuation failed: %+v", final)
	}
	// Run the actual independent closure path on the repaired live fixtures.
	now := time.Now().UTC()
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, []sdk.Detector{fastloopDetector{}}, provider, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	controller.incidentOpen = true
	controller.currentIncidentRequest = &request
	controller.currentIncidentResult = &final
	controller.responderDone = true
	controller.responderCompletedAt = now.Add(-time.Second)
	controller.history = []DetectorEvaluation{{DetectorID: "availability", Status: DetectorEvaluationClear, EvaluatedAt: now}}
	controller.maybeCloseIncident(ctx, now, nil)
	if controller.IncidentOpen() {
		t.Fatal("repaired incident did not close")
	}
}

func TestContinuationRepairsSurviveRestartAndDeduplicateReceipts(t *testing.T) {
	request := goldenRequest(t)
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	result := completedResult(request.IncidentID)
	result.RepairActions = []RepairActionReceipt{{ActionID: "live-action", Kind: "kubectl", Summary: "recorded repair", Success: true}}
	repairs := loop.continuationRepairs(result)
	if len(repairs) != 1 || repairs[0].Kind != "recorded-action" || repairs[0].Receipt.ActionID != "live-action" {
		t.Fatalf("missing live receipt: %+v", repairs)
	}
	encoded, err := continuationFeedback(1, ToolResponse{Summary: "third remains"}, repairs...)
	if err != nil {
		t.Fatal(err)
	}
	message, err := incidentResultContract(result)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { persisted = append([]byte(nil), payload...); return nil }
	if err = loop.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Feedback: string(encoded), Result: message}); err != nil {
		t.Fatal(err)
	}
	restored := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	if err = restored.restoreToolSnapshot(persisted); err != nil {
		t.Fatal(err)
	}
	// A resumed provider may omit old receipts; the continuation still retains them.
	next := restored.continuationRepairs(IncidentResult{})
	if len(next) != 1 || next[0].ActionID != "live-action" {
		t.Fatalf("restart lost completed repair: %+v", next)
	}
	next = restored.continuationRepairs(result)
	if len(next) != 1 {
		t.Fatalf("duplicate resumed receipt: %+v", next)
	}
}

func TestResponderContinuationWaitsForClearingWithoutNewJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := goldenRequest(t)
	request.ResponseDeadline = time.Now().Add(time.Minute)
	request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	checks := 0
	loop.VerifyClosure = func(context.Context) ToolResponse {
		checks++
		return ToolResponse{Command: "verify", Healthy: checks > 1, Summary: "waiting for clearing confirmation"}
	}
	client := fake.NewSimpleClientset()
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	done := make(chan error, 1)
	go func() { _, err := dispatcher.Dispatch(ctx, request); done <- err }()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	publishContinuationResult(t, client, first, completedResult(request.IncidentID))
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || loop.continuation.Attempts != 1 {
		t.Fatalf("clearing consumed another turn: jobs=%d attempts=%d", len(jobs.Items), loop.continuation.Attempts)
	}
}
