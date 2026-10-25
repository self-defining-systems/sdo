package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/outcome_derivation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type coverageThreeFaultDetector struct{ fastloopDetector }

func (coverageThreeFaultDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (fastloopDetector{}).Detect(ctx, view)
	for _, service := range view.Services() {
		if service.Spec.Ports[0].TargetPort.IntVal == 7777 {
			findings = append(findings, sdk.Finding{DetectorID: "availability", RuleID: "unlearned-port", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Service", "v1", &service), Summary: "unlearned fault", Evidence: "third service has wrong port"})
		}
	}
	return findings, err
}

// Gate 1 A uses the real dispatcher, worker task, Python tool transport, menu,
// executor, health verifier and outcome derivation. The worker is scripted.
func TestHermeticAgentRepairsTwoCoveredFaultsAndOneUnexplained(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := fastloopClient("good:v1")
	_, err := client.CoreV1().Services("app").Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "third", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(7777)}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fastloopPodWaiting(t, ctx, client, "api-pod", "ImagePullBackOff")
	provider := fastloopObjects{client: client}
	view, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	detector := coverageThreeFaultDetector{}
	findings, err := detector.Detect(ctx, view)
	if err != nil || len(findings) != 3 {
		t.Fatalf("three faults: %+v %v", findings, err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}, fastloopImageProgram{}}, []program.Mitigator{fastloopPortProgram{}, fastloopImageProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	request := goldenRequest(t)
	request.Namespace = "app"
	request.Findings = findings
	request.RepairPolicy = "recorded-actions"
	request.RepositoryWorktree = filepath.Join(root, "application")
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider}}, 5)
	loop.MenuConfig = &ResponderMenuConfig{}
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "scripted:test"), AgentID: testIncidentAgentID(t), ServiceAccount: "scripted", RepositoryPVC: "repository", RepositoryMountPath: root, PollInterval: time.Millisecond, LoopFactory: func(r IncidentRequest) *ResponderLoop { loop.Request = r; return loop }}
	prepared, err := dispatcher.PrepareDispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	task := new(contracts.AgentTask)
	if err := protojson.Unmarshal([]byte(prepared.PreparedAgentTask), task); err != nil {
		t.Fatal(err)
	}
	typed := new(contracts.IncidentRequest)
	if err := task.Request.UnmarshalTo(typed); err != nil {
		t.Fatal(err)
	}
	if len(typed.GetPlaybookCoverage().GetMatches()) != 2 || len(typed.PlaybookCoverage.UnexplainedFindingFingerprints) != 1 {
		t.Fatalf("worker missed partial coverage: %v", typed.PlaybookCoverage)
	}
	if typed.PlaybookCoverage.UnexplainedFindingFingerprints[0] != FindingFingerprint(findings[2]) {
		t.Fatalf("wrong uncovered fault: %v", typed.PlaybookCoverage)
	}
	type dispatchResult struct {
		result IncidentResult
		err    error
	}
	done := make(chan dispatchResult, 1)
	go func() { result, err := dispatcher.Dispatch(ctx, prepared); done <- dispatchResult{result, err} }()
	var job *batchv1.Job
	for i := 0; i < 1000; i++ {
		select {
		case ended := <-done:
			t.Fatalf("dispatch ended before agent Job: %+v", ended)
		default:
		}
		job, err = client.BatchV1().Jobs("app").Get(ctx, IncidentJobName(request.IncidentID), metav1.GetOptions{})
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(loop.PlanApplications()) != 0 {
		t.Fatal("Go applied a program before starting the agent")
	}
	live, _ := provider.Snapshot(ctx)
	still, _ := detector.Detect(ctx, live)
	if len(still) != 3 {
		t.Fatalf("fault changed before agent: %v", still)
	}
	tools := filepath.Join(t.TempDir(), "tools")
	server := SessionToolServer{Directory: tools, Handle: loop.Handle, PollInterval: time.Millisecond}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	serverCtx, stop := context.WithCancel(ctx)
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(serverCtx) }()
	t.Cleanup(func() { stop(); <-served })
	// Every write is a scripted agent choice through the real menu transport.
	for _, match := range typed.PlaybookCoverage.Matches {
		response := fastloopTool(t, tools, "mitigate", match.MitigationSelection)
		if response.ApplicationID == "" {
			t.Fatalf("program not applied: %+v", response)
		}
	}
	third := fastloopTool(t, tools, "mitigate", "SetField", "--param", "ref=Service/app/third", "--param", "path=spec.ports[0].targetPort", "--param", `value=8080`, "--param", "reason=repair uncovered finding")
	if third.ApplicationID == "" {
		t.Fatalf("agent action not recorded: %+v", third)
	}
	checked := fastloopTool(t, tools, "verify")
	if !checked.Healthy || len(checked.Findings) != 0 {
		t.Fatalf("three faults did not close: %+v", checked)
	}
	result := completedResult(request.IncidentID)
	result.RepairActions = nil
	result.FinalDetectorStates = checked.DetectorStates
	payload, err := testAgentResultPayload(t, prepared, result)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CoreV1().ConfigMaps("app").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result"}, Data: map[string]string{jobResultDataKey: string(payload)}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	if _, err := client.BatchV1().Jobs("app").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	dispatched := <-done
	if dispatched.err != nil || dispatched.result.Status != IncidentCompleted {
		t.Fatalf("dispatch: %+v, error: %v", dispatched.result, dispatched.err)
	}
	closure := &contracts.IncidentClosure{Request: mustProject(t, incidentRequestContract, prepared), Result: mustProject(t, incidentResultContract, dispatched.result), FinalDetectorStates: evaluationsContract(checked.DetectorStates)}
	// The same independently verified classification used by normal closure.
	now := timestamppb.Now()
	closure.DetectedAt = now
	closure.DispatchedAt = now
	closure.ResponderCompletedAt = now
	closure.VerifiedAt = now
	derived, err := outcome_derivation.Derive(ctx, closure, "", "scripted", "scripted")
	if err != nil {
		t.Fatal(err)
	}
	coverage := derived.Record.GetPlaybookCoverage()
	if len(coverage.GetMatches()) != 2 || len(coverage.UnexplainedFindingFingerprints) != 1 || len(derived.Record.RepairActions) < 3 {
		t.Fatalf("lost attribution: %v", derived.Record)
	}
	data, err := json.Marshal(derived.Record)
	if err != nil || len(data) == 0 {
		t.Fatalf("outcome: %v", err)
	}
}
