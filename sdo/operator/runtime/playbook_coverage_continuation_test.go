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

// Gate 1 B joins incident-open partial coverage with real CLI writes and one
// continuation. The third finding has no learned mitigation, including at retry.
func TestHermeticCoverageContinuationLeavesOnlyUnexplainedFinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := fake.NewSimpleClientset()
	for _, name := range []string{"first", "second", "third"} {
		port := int32(9999)
		if name == "third" {
			port = 7777
		}
		_, err := client.CoreV1().Services("app").Create(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(port)}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	provider := fastloopObjects{client: client}
	detector := coverageThreeFaultDetector{}
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
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request.Findings, err = detector.Detect(ctx, snapshot)
	if err != nil || len(request.Findings) != 3 {
		t.Fatalf("initial findings: %+v %v", request.Findings, err)
	}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{detector}, provider, &verificationRetryDispatcher{}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	request.ObservationGeneration = observationGeneration(snapshot)
	controller.currentIncidentRequest = &request
	controller.incidentOpen = true
	observe := func() {
		if err := controller.StepEvents(ctx, time.Now().UTC(), []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}); err != nil {
			t.Fatal(err)
		}
	}
	observe()
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, PlanExecutor{
		Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client},
		Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider},
	}, 3)
	loop.MenuConfig = productionResponderMenu()
	loop.VerifyClosure = func(ctx context.Context) ToolResponse {
		return controller.responderVerification(ctx, request.IncidentID)
	}
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"),
		AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree),
		ServiceAccount: "responder", PollInterval: time.Millisecond,
		RequireMemorySnapshot: true, Catalog: fixedMemoryCatalog{request.RepositoryBaseCommit},
		LoopFactory: func(r IncidentRequest) *ResponderLoop { loop.Request = r; return loop },
	}
	prepared, err := dispatcher.PrepareDispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	coverage := prepared.PlaybookCoverage
	if len(coverage.GetMatches()) != 2 || len(coverage.GetUnexplainedFindingFingerprints()) != 1 {
		t.Fatalf("must offer two matches and one unexplained finding: %+v", coverage)
	}
	unexplained := FindingFingerprint(request.Findings[2])
	if coverage.UnexplainedFindingFingerprints[0] != unexplained {
		t.Fatalf("wrong unexplained finding: %+v", coverage)
	}
	type dispatchResult struct {
		result IncidentResult
		err    error
	}
	done := make(chan dispatchResult, 1)
	go func() { result, err := dispatcher.Dispatch(ctx, prepared); done <- dispatchResult{result, err} }()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	sessionPath := ""
	for _, mount := range first.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/sdo/session" {
			sessionPath = mount.SubPath
		}
	}
	if sessionPath == "" {
		t.Fatal("missing real worker session")
	}
	tools := filepath.Join(dispatcher.RepositoryMountPath, sessionPath, "tools")
	// Exercise the public CLI for both all-detector and selected-detector reads.
	for _, args := range [][]string{{"detect"}, {"detect", "--detector", "availability"}} {
		response := fastloopTool(t, tools, args...)
		if response.Error != "" || len(response.Findings) != 3 {
			t.Fatalf("CLI self-check %v: %+v", args, response)
		}
	}
	for _, match := range coverage.Matches {
		response := fastloopTool(t, tools, "mitigate", match.MitigationSelection)
		if response.Error != "" || response.ApplicationID == "" {
			t.Fatalf("covered mitigation: %+v", response)
		}
	}
	observe()
	partial := fastloopTool(t, tools, "verify")
	if partial.Healthy || len(partial.Findings) != 1 || FindingFingerprint(partial.Findings[0]) != unexplained {
		t.Fatalf("partial verification must retain only unexplained finding: %+v", partial)
	}
	result := completedResult(request.IncidentID)
	result.RepairActions = nil
	result.ResponderSessionID = "coverage-continuation-session"
	publishContinuationResult(t, client, first, result)
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	var feedback responderFeedback
	for _, entry := range second.Spec.Template.Spec.Containers[0].Env {
		if entry.Name == "SDO_RESPONSE_CONTINUATION" {
			if err := json.Unmarshal([]byte(entry.Value), &feedback); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(feedback.StillBroken) != 1 || feedback.StillBroken[0].Fingerprint != unexplained || feedback.StillBroken[0].Resource.Name != "third" {
		t.Fatalf("re-prompt leaked covered findings or lost unexplained fault: %+v", feedback)
	}
	if len(feedback.AlreadyDone) != 2 {
		t.Fatalf("must retain exactly two learned repairs: %+v", feedback)
	}
	resources := map[string]bool{}
	for _, repair := range feedback.AlreadyDone {
		if repair.Kind != "program-application" || repair.ApplicationID == "" || !repair.Receipt.Success || len(repair.ProgramIDs) != 1 || repair.ProgramIDs[0] != "service-targetport-mismatch" {
			t.Fatalf("lost learned repair receipt: %+v", repair)
		}
		for _, resource := range repair.Receipt.Resources {
			resources[resource.Name] = true
		}
	}
	if !resources["first"] || !resources["second"] || resources["third"] {
		t.Fatalf("wrong already-done resources: %+v", resources)
	}
	for _, mount := range second.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/sdo/session" && mount.SubPath != sessionPath {
			t.Fatal("continuation changed session")
		}
	}
	repaired := fastloopTool(t, tools, "mitigate", "SetField", "--param", "ref=Service/app/third", "--param", "path=spec.ports[0].targetPort", "--param", "value=8080", "--param", "reason=repair unexplained finding")
	if repaired.Error != "" || repaired.ApplicationID == "" {
		t.Fatalf("scripted repair: %+v", repaired)
	}
	observe()
	if final := fastloopTool(t, tools, "verify"); !final.Healthy || len(final.Findings) != 0 {
		t.Fatalf("final verification: %+v", final)
	}
	publishContinuationResult(t, client, second, result)
	final := <-done
	if final.err != nil || final.result.Status != IncidentCompleted || final.result.ResponderSessionID != result.ResponderSessionID {
		t.Fatalf("continuation result: %+v", final)
	}
	jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 2 {
		t.Fatalf("expected initial job and exactly one re-prompt, got %d", len(jobs.Items))
	}
}
