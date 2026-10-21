package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/worker_registry"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

type fixedMemoryCatalog struct{ commit string }

func (catalog fixedMemoryCatalog) Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error) {
	return &contracts.OperationalMemorySnapshot{AcceptedCommit: catalog.commit}, nil
}

func TestNativeResponderSessionsAreIsolatedPerIncident(t *testing.T) {
	root := t.TempDir()
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	request.RepositoryWorktree = root + "/proposals/incident-one"
	request.IncidentID = "incident-one"
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root,
		Environment: map[string]string{"CODEX_HOME": "/workspace/.sdo-runtime/codex"},
		Catalog:     fixedMemoryCatalog{commit: request.RepositoryBaseCommit}, RequireMemorySnapshot: true,
		PollInterval: time.Millisecond,
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	go func() { _, _ = dispatcher.Dispatch(firstCtx, request) }()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	cancelFirst()
	request.IncidentID = "incident-two"
	request.RepositoryWorktree = root + "/proposals/incident-two"
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	go func() { _, _ = dispatcher.Dispatch(secondCtx, request) }()
	second := awaitJob(t, client, IncidentJobName(request.IncidentID))
	session := func(job *batchv1.Job) string {
		container := job.Spec.Template.Spec.Containers[0]
		if !containsEnvironment(container.Env, "CODEX_HOME", "/sdo/session/codex") || !containsEnvironment(container.Env, "CLAUDE_CONFIG_DIR", "/sdo/session/claude") {
			t.Fatalf("worker session home escaped narrow mount: %+v", container.Env)
		}
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == root || mount.SubPath == "application" {
				t.Fatalf("worker gained accepted checkout: %+v", mount)
			}
			if mount.MountPath == "/sdo/session" {
				if mount.ReadOnly {
					t.Fatalf("session mount is not writable and narrow: %+v", mount)
				}
				return mount.SubPath
			}
		}
		t.Fatal("missing per-incident session mount")
		return ""
	}
	if left, right := session(first), session(second); left == right {
		t.Fatalf("separate incidents share agent session volume: %q", left)
	}
}

type liveShapePortDetector struct{}

func (liveShapePortDetector) Spec() sdk.DetectorSpec { return (fastloopDetector{}).Spec() }

func (liveShapePortDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (fastloopDetector{}).Detect(ctx, view)
	if err != nil || len(findings) == 0 {
		return findings, err
	}
	drift := findings[0]
	drift.DetectorID = "drift/service-targetport-mismatch"
	drift.ParameterBindings = map[string]sdk.ObjectRef{"service": drift.PrimaryResource}
	return []sdk.Finding{drift, findings[0]}, nil
}

func TestHermeticFourIncidentTrustedFastPathCompletesWithoutResponderJob(t *testing.T) {
	loop, client := trustedPortLoop(t, false, false, true)
	loop.Detectors = []sdk.Detector{liveShapePortDetector{}}
	drift := loop.Request.Findings[0]
	drift.DetectorID = "drift/service-targetport-mismatch"
	drift.ParameterBindings = map[string]sdk.ObjectRef{"service": drift.PrimaryResource}
	request := goldenRequest(t)
	request.Namespace = "app"
	request.RepairPolicy = "recorded-actions"
	request.SourceCommit = "tree-a"
	request.Findings = []sdk.Finding{drift, loop.Request.Findings[0]}
	request.RepositoryWorktree = "/workspace/worktrees/" + request.IncidentID
	outcomes := []memory.Outcome{}
	for index := 0; index < 3; index++ {
		outcomes = append(outcomes, memory.Outcome{IncidentID: fmt.Sprintf("prior-%d", index), Record: &contracts.OutcomeRecord{
			SourceCommit: "tree-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "service-targetport-mismatch", ProgramRevision: "tree-a"}},
			PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"service-targetport-mismatch"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}},
		}})
	}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Image: "responder:test", Command: []string{"/responder"}, ProgramFastPath: true,
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: "/workspace",
		Outcomes: priorOutcomeReader{outcomes: outcomes}, LoopFactory: func(request IncidentRequest) *ResponderLoop {
			loop.Request = request
			return loop
		}}
	var err error
	request, err = dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), request)
	if err != nil || result.ResolutionPath != "fast_path" || len(result.FastPathPrograms) != 1 || result.Usage.GetInputTokens() != 0 {
		t.Fatalf("fast path result: %+v %v", result, err)
	}
	matchedSources := map[string]bool{}
	for _, run := range result.ProgramRuns {
		if run.Verdict.Result == program.Match {
			matchedSources[run.BindingSource] = true
		}
	}
	if !matchedSources["hint"] || !matchedSources["finding"] {
		t.Fatalf("live-shaped hint and finding runs were lost: %+v", result.ProgramRuns)
	}
	replayed, err := dispatcher.Dispatch(context.Background(), request)
	if err != nil || replayed.ResolutionPath != "fast_path" || len(replayed.PlanApplications) != 1 || len(replayed.RepairActions) != 1 {
		t.Fatalf("fast path was not durable across dispatch replay: %+v %v", replayed, err)
	}
	jobs, err := client.BatchV1().Jobs("app").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("responder Job dispatched: %+v %v", jobs, err)
	}
}

func TestHermeticTrustedLatentDriftFastPathSkipsResponderAndReflection(t *testing.T) {
	loop, client := trustedPortLoop(t, false, false, true)
	loop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	finding := loop.Request.Findings[0]
	finding.DetectorID = "drift/service-targetport-mismatch"
	finding.Severity = sdk.SeverityLatent
	finding.ParameterBindings = map[string]sdk.ObjectRef{"service": finding.PrimaryResource}
	request := goldenRequest(t)
	request.Namespace = "app"
	request.SourceCommit = "tree-a"
	request.RepairPolicy = "recorded-actions"
	request.Findings = []sdk.Finding{finding}
	request.DriftEvents = []sdk.DriftEvent{{Ref: finding.PrimaryResource, Classification: sdk.DriftFault, ProgramID: "service-targetport-mismatch", ObservedAt: time.Now().UTC()}}
	request.RepositoryWorktree = "/workspace/worktrees/" + request.IncidentID
	outcomes := []memory.Outcome{{IncidentID: "earlier-drift", Record: &contracts.OutcomeRecord{
		SourceCommit: "tree-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "service-targetport-mismatch", ProgramRevision: "tree-a"}},
		PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"service-targetport-mismatch"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}},
	}}}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Image: "responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: "/workspace",
		ProgramFastPath: true, ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: outcomes},
		LoopFactory: func(request IncidentRequest) *ResponderLoop { loop.Request = request; return loop }}
	prepared, err := dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), prepared)
	if err != nil || result.ResolutionPath != "fast_path" || len(result.FastPathPrograms) != 1 {
		t.Fatalf("trusted latent drift did not resolve: %+v %v", result, err)
	}
	jobs, err := client.BatchV1().Jobs("app").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("responder Job dispatched: %+v %v", jobs, err)
	}
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	reflections := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"),
		ResponderBackend: "test", ResponderModel: "test", ForceReflection: true,
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			derived := testDerivedOutcome(closure.Request.IncidentID, "success", true)
			derived.Record.ResolutionPath = closure.Result.ResolutionPath
			derived.Record.FastPathPrograms = closure.Result.FastPathPrograms
			return derived, nil
		}),
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string) (ReflectionDecision, error) {
			reflections++
			return ReflectionDecision{}, nil
		})}
	workspace, err := native.PrepareIncident(context.Background(), "trusted-drift")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	result.IncidentID = closure.Request.IncidentID
	closure.Result = &result
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("trusted-drift")
	if err != nil || reflections != 0 || ledger.ReflectionSkippedReason != "trusted program fast path" {
		t.Fatalf("trusted drift dispatched reflection: ledger=%+v calls=%d err=%v", ledger, reflections, err)
	}
}

func TestFailedFastPathRollsBackAndDispatchesResponderWithSummary(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, false)
	request := goldenRequest(t)
	request.Namespace = "app"
	request.RepairPolicy = "recorded-actions"
	request.SourceCommit = "tree-a"
	request.Findings = loop.Request.Findings
	request.RepositoryWorktree = "/workspace/worktrees/" + request.IncidentID
	outcomes := make([]memory.Outcome, 3)
	for index := range outcomes {
		outcomes[index] = memory.Outcome{IncidentID: fmt.Sprintf("prior-%d", index), Record: &contracts.OutcomeRecord{
			SourceCommit: "tree-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "service-targetport-mismatch", ProgramRevision: "tree-a"}},
			PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"service-targetport-mismatch"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}},
		}}
	}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Image: "responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: "/workspace",
		ProgramFastPath: true, Outcomes: priorOutcomeReader{outcomes: outcomes}, PollInterval: time.Millisecond,
		LoopFactory: func(request IncidentRequest) *ResponderLoop { loop.Request = request; return loop }}
	var err error
	request, err = dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { _, err := dispatcher.Dispatch(ctx, request); finished <- err }()
	select {
	case err := <-finished:
		t.Fatalf("responder dispatch ended before Job: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.BatchV1().Jobs("app").Get(context.Background(), IncidentJobName(request.IncidentID), metav1.GetOptions{}); err == nil {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("responder dispatch ended before Job: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := client.BatchV1().Jobs("app").Get(context.Background(), IncidentJobName(request.IncidentID), metav1.GetOptions{}); err != nil {
		t.Fatalf("responder Job absent: %v", err)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("failed fast path left mutation: %+v %v", service, err)
	}
	taskMap, err := client.CoreV1().ConfigMaps("app").Get(context.Background(), IncidentJobName(request.IncidentID)+"-request", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	task := new(contracts.AgentTask)
	if err := protojson.Unmarshal([]byte(taskMap.Data[jobRequestDataKey]), task); err != nil {
		t.Fatal(err)
	}
	incident := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(incident); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(incident.GetFastPathSummary(), "service-targetport-mismatch") || !strings.Contains(incident.GetFastPathSummary(), "rollback true") {
		t.Fatalf("responder missed failed attempt: %q", incident.GetFastPathSummary())
	}
	cancel()
	<-finished
}

func TestInterruptedFastPathRestoresPersistedPreImages(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	request := loop.Request
	request.IncidentID = "interrupted"
	ref := request.Findings[0].PrimaryResource
	before, err := (TypedPlanClient{Client: client}).Get(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	preImages, err := json.Marshal([]PlanPreImage{{Ref: ref, Object: before}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CoreV1().ConfigMaps("app").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: IncidentJobName(request.IncidentID) + "-fast-path"},
		Data:       map[string]string{"state": "attempting", "preimages": string(preImages), "program_ids": `["service-targetport-mismatch"]`},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app"}
	_, summary, resolved, err := dispatcher.tryFastPath(context.Background(), request, loop)
	if err != nil || resolved || !strings.Contains(summary, "rollback true") {
		t.Fatalf("interrupted attempt: summary=%q resolved=%t err=%v", summary, resolved, err)
	}
	service, err = client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("interrupted mutation not restored: %+v %v", service, err)
	}
}

func TestResponderJobStartsBeforeSettleCapture(t *testing.T) {
	root := t.TempDir()
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	request.RepositoryWorktree = filepath.Join(root, "proposals", "incident")
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root,
		Catalog: fixedMemoryCatalog{commit: request.RepositoryBaseCommit}, RequireMemorySnapshot: true,
		PollInterval: time.Millisecond,
		LoopFactory: func(request IncidentRequest) *ResponderLoop {
			loop := NewResponderLoop(request, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: request.Namespace}}, nil, PlanExecutor{}, 5)
			loop.CorpusSessionRoot = root
			loop.DiagnosisSettleDelay = time.Second
			loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
			}
			return loop
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = dispatcher.Dispatch(ctx, request) }()
	_ = awaitJob(t, client, IncidentJobName(request.IncidentID))
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "open@settle.json")); !os.IsNotExist(err) {
		t.Fatalf("settle capture preceded responder Job: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "open@settle.json")); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("settle capture did not arrive while responder Job was active")
}

func TestKubernetesJobDispatcherCrashReplayReattachesExactlyOnce(t *testing.T) {
	// TODO: Add a hermetic e2e case for settle update, restart, and result
	// collection once the hermetic e2e suite lands in this worktree.
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", RepositoryPVCSubPath: "shared", PollInterval: time.Millisecond,
		Environment: map[string]string{"SDO_SREGYM_API_BASE": "http://host.docker.internal:8123"},
	}
	var err error
	request, err = dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatalf("prepare durable dispatch: %v", err)
	}
	if request.PreparedAgentTask == "" {
		t.Fatal("controller state has no exact agent task")
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := dispatcher.Dispatch(firstCtx, request)
		firstResult <- err
	}()
	jobName := IncidentJobName(request.IncidentID)
	job := awaitJob(t, client, jobName)
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished < 1800 {
		t.Fatalf("responder Job expires before the closure receipt can inspect it: %#v", job.Spec.TTLSecondsAfterFinished)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := container.EnvFrom[0].SecretRef.Name; got != "sdo-codex" {
		t.Fatalf("responder did not receive isolated credentials Secret: %q", got)
	}
	if got := container.WorkingDir; got != request.RepositoryWorktree {
		t.Fatalf("responder working directory %q does not match broker worktree", got)
	}
	if !containsEnvironment(container.Env, "SDO_SREGYM_API_BASE", "http://host.docker.internal:8123") {
		t.Fatalf("responder bridge environment was not propagated: %#v", container.Env)
	}
	if got := container.VolumeMounts[1]; got.MountPath != request.RepositoryWorktree || got.SubPath != "shared/"+request.RepositoryWorktree[len("/workspace/"):] {
		t.Fatalf("only the proposal checkout should be mounted: %#v", got)
	}
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil ||
		*container.SecurityContext.AllowPrivilegeEscalation || container.SecurityContext.ReadOnlyRootFilesystem == nil ||
		!*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("responder container is not hardened: %#v", container.SecurityContext)
	}
	if job.Spec.Template.Spec.SecurityContext == nil || job.Spec.Template.Spec.SecurityContext.RunAsNonRoot == nil ||
		!*job.Spec.Template.Spec.SecurityContext.RunAsNonRoot {
		t.Fatalf("responder pod is not non-root: %#v", job.Spec.Template.Spec.SecurityContext)
	}
	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected first controller crash cancellation, got %v", err)
	}
	stateJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var restored IncidentRequest
	if err := json.Unmarshal(stateJSON, &restored); err != nil {
		t.Fatal(err)
	}
	request = restored
	// A settle observation changes the controller's current finding. Rejoining
	// the worker must use the task that was dispatched before this update.
	request.Findings[0].Evidence = "settle-time evidence"
	preparedTask := request.PreparedAgentTask
	request.PreparedAgentTask = "" // controller state written before task persistence
	request, err = dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatalf("recover legacy dispatch: %v", err)
	}
	if request.PreparedAgentTask != preparedTask {
		t.Fatal("recovery rebuilt the dispatched task")
	}

	secondResult := make(chan IncidentResult, 1)
	secondError := make(chan error, 1)
	go func() {
		result, err := dispatcher.Dispatch(context.Background(), request)
		secondResult <- result
		secondError <- err
	}()
	result := completedResult(request.IncidentID)
	payload, err := testAgentResultPayload(request, result)
	if err != nil {
		t.Fatalf("encode result: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: jobName + "-result"},
		Data:       map[string]string{jobResultDataKey: string(payload)},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create result ConfigMap: %v", err)
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("demo").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark replayed responder Job complete: %v", err)
	}
	if err := <-secondError; err != nil {
		t.Fatalf("reattach dispatch: %v", err)
	}
	if got := (<-secondResult).IncidentID; got != request.IncidentID {
		t.Fatalf("unexpected result incident %q", got)
	}
	jobs, err := client.BatchV1().Jobs("demo").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list Jobs: %v", err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Name != jobName {
		t.Fatalf("dispatch replay created duplicate Jobs: %#v", jobs.Items)
	}
}

func TestKubernetesJobDispatcherDoesNotCompleteUntilResponderJobCompletes(t *testing.T) {
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", PollInterval: time.Millisecond,
	}
	resultChannel := make(chan IncidentResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, err := dispatcher.Dispatch(context.Background(), request)
		resultChannel <- result
		errorChannel <- err
	}()
	jobName := IncidentJobName(request.IncidentID)
	job := awaitJob(t, client, jobName)
	result := completedResult(request.IncidentID)
	payload, err := testAgentResultPayload(request, result)
	if err != nil {
		t.Fatalf("encode result: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: jobName + "-result"},
		Data:       map[string]string{jobResultDataKey: string(payload)},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create result ConfigMap: %v", err)
	}
	select {
	case err := <-errorChannel:
		t.Fatalf("dispatcher released the incident lock while responder Job was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("demo").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark responder Job complete: %v", err)
	}
	select {
	case err := <-errorChannel:
		if err != nil {
			t.Fatalf("completed responder Job returned error: %v", err)
		}
		if got := (<-resultChannel).IncidentID; got != request.IncidentID {
			t.Fatalf("unexpected result incident %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not release completed responder Job result")
	}
}

func containsEnvironment(environment []corev1.EnvVar, name string, value string) bool {
	for _, variable := range environment {
		if variable.Name == name && variable.Value == value {
			return true
		}
	}
	return false
}

func testAgentResultPayload(request IncidentRequest, result IncidentResult) ([]byte, error) {
	task, _, err := encodeIncidentAgentTask(request, builtInResponderID)
	if err != nil {
		return nil, err
	}
	typed, err := incidentResultToProto(result)
	if err != nil {
		return nil, err
	}
	packed, err := anypb.New(typed)
	if err != nil {
		return nil, err
	}
	return canonicalProtoJSON(&contracts.AgentResult{
		SchemaVersion: ProtocolSchemaVersion, TaskId: task.GetTaskId(), Status: string(result.Status),
		ProposalId: task.GetWorkspace().GetProposalId(), Response: packed,
	})
}

func TestKubernetesJobDispatcherUsesRegisteredWorker(t *testing.T) {
	registration := &contracts.AgentRegistration{
		SchemaVersion: ProtocolSchemaVersion, AgentId: "custom-responder",
		Capabilities: []*contracts.AgentCapability{{TaskType: incidentTaskType, RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult"}},
		Launch:       &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{Image: "example/custom:1", Command: []string{"/custom-worker"}}},
	}
	registry, err := worker_registry.New(registration)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: registry, AgentID: "custom-responder",
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", PollInterval: time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = dispatcher.Dispatch(ctx, request) }()
	job := awaitJob(t, client, IncidentJobName(request.IncidentID))
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "example/custom:1" || len(container.Command) != 1 || container.Command[0] != "/custom-worker" {
		t.Fatalf("worker launch ignored registration: %#v", container)
	}
	requestMap, err := client.CoreV1().ConfigMaps("demo").Get(ctx, IncidentJobName(request.IncidentID)+"-request", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var task contracts.AgentTask
	if err := protojson.Unmarshal([]byte(requestMap.Data[jobRequestDataKey]), &task); err != nil {
		t.Fatal(err)
	}
	if task.GetAgentId() != "custom-responder" {
		t.Fatalf("wrong agent selected: %q", task.GetAgentId())
	}
}

func TestKubernetesJobDispatcherAddsDispatchTimeEnvironment(t *testing.T) {
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", PollInterval: time.Millisecond,
		Environment: map[string]string{"SDO_SREGYM_API_BASE": "http://bridge:18000"},
		DispatchEnvironment: func(context.Context) map[string]string {
			return map[string]string{"SDO_PROBER_URL": "http://10.0.0.7:8080", "SDO_NAMESPACE": "hijacked"}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = dispatcher.Dispatch(ctx, request) }()
	container := awaitJob(t, client, IncidentJobName(request.IncidentID)).Spec.Template.Spec.Containers[0]
	if !containsEnvironment(container.Env, "SDO_PROBER_URL", "http://10.0.0.7:8080") {
		t.Fatalf("dispatch-time environment was not propagated: %#v", container.Env)
	}
	if !containsEnvironment(container.Env, "SDO_SREGYM_API_BASE", "http://bridge:18000") {
		t.Fatalf("static environment was lost: %#v", container.Env)
	}
	if containsEnvironment(container.Env, "SDO_NAMESPACE", "hijacked") {
		t.Fatalf("dispatch-time environment must not override controller-managed names: %#v", container.Env)
	}
}

func TestKubernetesJobDispatcherReturnsFailedJob(t *testing.T) {
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", PollInterval: time.Millisecond,
	}
	result := make(chan error, 1)
	go func() {
		_, err := dispatcher.Dispatch(context.Background(), request)
		result <- err
	}()
	jobName := IncidentJobName(request.IncidentID)
	job := awaitJob(t, client, jobName)
	job.Status.Failed = 1
	if _, err := client.BatchV1().Jobs("demo").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark Job failed: %v", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected failed Job error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed Job result")
	}
}

func TestKubernetesJobDispatcherRejectsWorktreeOutsideSharedMount(t *testing.T) {
	request := goldenRequest(t)
	request.RepositoryWorktree = "/private/host/worktree"
	dispatcher := KubernetesJobDispatcher{
		Client: fake.NewSimpleClientset(), Namespace: "demo", Image: "sdo-responder:test", Command: []string{"/responder"},
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", PollInterval: time.Millisecond,
	}

	if _, err := dispatcher.Dispatch(context.Background(), request); err == nil {
		t.Fatal("dispatcher accepted a broker worktree outside the shared PVC mount")
	}
}

func awaitJob(t *testing.T, client *fake.Clientset, name string) *batchv1.Job {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if job, err := client.BatchV1().Jobs("demo").Get(context.Background(), name, metav1.GetOptions{}); err == nil {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for Job %q", name)
	return nil
}
