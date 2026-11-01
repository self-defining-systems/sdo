//go:build integration

package runtime

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/routingport"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestHermeticRoutingCaptureCompilesReplayableMitigator(t *testing.T) {
	ctx := context.Background()
	snapshot := routingCaptureSnapshot()
	peer := *snapshot.ServiceList[0].DeepCopy()
	peer.Name = "peer"
	peer.Spec.Selector = map[string]string{"app": "peer"}
	peer.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	peerPod := *snapshot.PodList[0].DeepCopy()
	peerPod.Name = "peer-pod"
	peerPod.Status.PodIP = "10.0.0.6"
	peerPod.Labels = map[string]string{"app": "peer"}
	snapshot.ServiceList = append(snapshot.ServiceList, peer)
	snapshot.PodList = append(snapshot.PodList, peerPod)
	provider := &changingProvider{snapshot: snapshot}
	probe := inProcessProberClient(noTrafficProberHandler(t))
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{DetectorID: "health", RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, PrimaryResource: ref, Summary: "routing unavailable", Evidence: "no Service connection"}
	detector := controllerDetector("health", time.Second, finding, finding, sdk.Finding{}, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	captureRoot := t.TempDir()
	config := testControllerConfig()
	config.Namespace = "app"
	config.CorpusSessionRoot = captureRoot
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{View: func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(s), Prober: probe}
	}})
	for i := 0; i < 2; i++ {
		if err := controller.Step(ctx, time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	captureDir, err := memory.IncidentCorpusDir(captureRoot, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	provider.snapshot = snapshot
	awaitDispatchCompletionQueued(t, controller)
	for i := 2; i < 4; i++ {
		if err := controller.Step(ctx, time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	programDir := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch")
	if err := os.MkdirAll(programDir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"diagnose.go", "mitigate.go"} {
		source, err := os.ReadFile(filepath.Join("../../../examples/playbook-programs/playbooks/service-targetport-mismatch", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(programDir, name), source, 0644); err != nil {
			t.Fatal(err)
		}
	}
	readme := []byte("---\nschema_version: 1\nowner: responder\nfault_class: service-targetport-mismatch\noriginating_incident: " + request.IncidentID + "\n---\n\nRestore `<SERVICE>` target port only after observed selected-Pod listener evidence.\n")
	if err := os.WriteFile(filepath.Join(programDir, "README.md"), readme, 0644); err != nil {
		t.Fatal(err)
	}
	writeCapturedProgramCorpus(t, root, captureDir, request.IncidentID, "service-targetport-mismatch", "service", "Service", "app", "api", "peer", "detect")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	var notes []sdktest.MitigationReplayResult
	diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot, MitigationReplayNotes: &notes})
	if err != nil || len(diagnostics) != 0 {
		t.Fatalf("captured routing diagnosis/generalization/mitigation gate: %+v %v", diagnostics, err)
	}
	if len(notes) != 1 || notes[0].Status != sdktest.MitigationReplayPassed {
		t.Fatalf("SetField was not replayed: %+v", notes)
	}

	// A trusted generalized routing program can cover both targetPort faults.
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "accepted recorded routing fixture")
	commit := nativeGit(t, root, "rev-parse", "HEAD")
	revision, err := programDependencyRevision(ctx, root, commit, "service-targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.ServiceList {
		snapshot.ServiceList[i].Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	}
	client := fake.NewSimpleClientset(&snapshot.ServiceList[0], &snapshot.ServiceList[1], &snapshot.PodList[0], &snapshot.PodList[1])
	liveProvider := fastloopObjects{client: client}
	p := routingport.Program{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	repeated := goldenRequest(t)
	repeated.IncidentID = "two-routing-faults"
	repeated.Namespace = "app"
	repeated.SourceCommit = commit
	repeated.RepositoryBaseCommit = commit
	repeated.RepositoryWorktree = root
	repeated.RepairPolicy = "recorded-actions"
	repeated.Findings, err = (fastloopDetector{}).Detect(ctx, snapshot)
	if err != nil || len(repeated.Findings) != 2 {
		t.Fatalf("two routing findings: %+v %v", repeated.Findings, err)
	}
	outcomes := []memory.Outcome{}
	for i := 0; i < 3; i++ {
		outcomes = append(outcomes, memory.Outcome{IncidentID: fmt.Sprintf("routing-success-%d", i), Record: verifiedTrustOutcome(t, p.Spec().ID, revision)})
	}
	warm := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), ProgramFastPath: true, ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root, Outcomes: priorOutcomeReader{outcomes: outcomes}, LoopFactory: func(request IncidentRequest) *ResponderLoop {
		loop := NewResponderLoop(request, registry, liveProvider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: liveProvider}}, 1)
		loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
			return LiveReadView{Carrier: inputs.New(s), Prober: probe}
		}
		loop.CorpusReplayPassed = true
		loop.DiagnosisSettleDelay = time.Millisecond
		return loop
	}}
	repeated, err = warm.PrepareDispatch(ctx, repeated)
	if err != nil {
		t.Fatal(err)
	}
	result, err := warm.Dispatch(ctx, repeated)
	if err != nil || result.ResolutionPath != "fast_path" || len(result.PlanApplications) != 1 || len(result.PlanApplications[0].Result.ActionsApplied) != 2 || result.Usage.GetInputTokens() != 0 {
		t.Fatalf("two-targetPort fast path: %+v %v", result, err)
	}
	jobs, err := client.BatchV1().Jobs("app").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("routing responder dispatched: %+v %v", jobs, err)
	}
	t.Log("captured TCP evidence passes fixed/healthy peer, rename and SetField repair replay; at three prior credits both targetPort faults resolve with fresh live probes and no responder Job")
}
