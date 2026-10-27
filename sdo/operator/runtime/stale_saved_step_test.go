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

// staleFaultProvider reports the Service fault back after any write: the saved
// step treats a symptom, and the live cause reasserts it.
type staleFaultProvider struct{ fastloopObjects }

func (p staleFaultProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := p.fastloopObjects.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := view.(sdktest.Snapshot)
	for i := range snapshot.ServiceList {
		snapshot.ServiceList[i].Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	}
	return snapshot, nil
}

// The safety net for steps saved from any verified-healthy closure: on a repeat
// the step is stale, the real verify reports the finding still broken, the
// real continuation re-prompts with only that finding, and nothing is recorded
// as success. The program is a hand-written stand-in for the saved step; the
// compiled saved program meets the same verify in TestCompiledStaleStepStaysBroken.
func TestHermeticStaleSavedStepRepromptsWithOnlyTheStillBrokenFinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := fake.NewSimpleClientset()
	if _, err := client.CoreV1().Services("app").Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	provider := staleFaultProvider{fastloopObjects{client}}
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
	if len(request.Findings) != 1 {
		t.Fatalf("setup: %+v", request.Findings)
	}
	request.Findings[0].Playbooks = []string{"service-targetport-mismatch"}
	detectors := []sdk.Detector{fastloopDetector{}}
	loop := NewResponderLoop(request, registry, provider, detectors, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: provider}}, 3)
	loop.MenuConfig = productionResponderMenu()
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
	fastloopTool(t, tools, "mitigate", "service-targetport-mismatch", "--service", "Service/app/first")
	if verify := fastloopTool(t, tools, "verify"); verify.Healthy || len(verify.Findings) != 1 {
		t.Fatalf("stale step reported health: %+v", verify)
	}
	// The agent reports done; the controller's own verify decides.
	publishContinuationResult(t, client, first, completedResult(request.IncidentID))
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	feedback := ""
	for _, env := range second.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "SDO_RESPONSE_CONTINUATION" {
			feedback = env.Value
		}
	}
	var parsed responderFeedback
	if err := json.Unmarshal([]byte(feedback), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Healthy || len(parsed.StillBroken) != 1 || parsed.StillBroken[0].Resource.Name != "first" || parsed.StillBroken[0].Fingerprint != FindingFingerprint(request.Findings[0]) {
		t.Fatalf("re-prompt must carry exactly the still-broken finding: %s", feedback)
	}
	// A turn that changes nothing is re-prompted again with the same finding until
	// the loop budget ends the incident, never with success.
	publishContinuationResult(t, client, second, completedResult(request.IncidentID))
	third := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-2"))
	publishContinuationResult(t, client, third, completedResult(request.IncidentID))
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	final := <-done
	if final.Status != IncidentFailed || !final.ResponseLoopExhausted || len(final.UnresolvedFindings) != 1 {
		t.Fatalf("stale step recorded success: %+v", final)
	}
	for _, application := range loop.PlanApplications() {
		if application.Result.VerificationPassed {
			t.Fatalf("stale step earned credit: %+v", application.Result)
		}
	}
}
