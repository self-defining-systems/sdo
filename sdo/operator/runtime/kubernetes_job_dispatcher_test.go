package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
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
				if mount.Name != "repository" || mount.ReadOnly {
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

func TestKubernetesJobDispatcherCrashReplayReattachesExactlyOnce(t *testing.T) {
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", RepositoryPVCSubPath: "shared", PollInterval: time.Millisecond,
		Environment: map[string]string{"SDO_SREGYM_API_BASE": "http://host.docker.internal:8123"},
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

	secondResult := make(chan IncidentResult, 1)
	secondError := make(chan error, 1)
	go func() {
		result, err := dispatcher.Dispatch(context.Background(), request)
		secondResult <- result
		secondError <- err
	}()
	result := completedResult(request.IncidentID)
	payload, err := testAgentResultPayload(t, request, result)
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
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
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
	payload, err := testAgentResultPayload(t, request, result)
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

func testAgentResultPayload(t testing.TB, request IncidentRequest, result IncidentResult) ([]byte, error) {
	task, _, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t))
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
	return wire.Canonical(&contracts.AgentResult{
		SchemaVersion: ProtocolSchemaVersion, TaskId: task.GetTaskId(), Status: string(result.Status),
		ProposalId: task.GetWorkspace().GetProposalId(), Response: packed,
	})
}

func TestKubernetesJobDispatcherUsesRegisteredWorker(t *testing.T) {
	registration := &contracts.AgentRegistration{
		SchemaVersion: ProtocolSchemaVersion, AgentId: "custom-responder",
		Capabilities: []*contracts.AgentCapability{{TaskType: testIncidentTaskType(t), RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult"}},
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
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
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
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
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
		Client: fake.NewSimpleClientset(), Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
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

// testIncidentRegistry is the checked-in built-in manifests; a non-empty image
// replaces their launch image the way a deployment's responder image does.
func testIncidentRegistry(t testing.TB, image string) *worker_registry.Registry {
	t.Helper()
	registry := registrytest.BuiltIn(t)
	if image == "" {
		return registry
	}
	registry, err := registry.WithLaunchImage(image)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

// testIncidentAgentID and testIncidentTaskType name what the manifests register
// for incident requests, so no test restates them.
func testIncidentAgentID(t testing.TB) string {
	t.Helper()
	id, err := registrytest.BuiltIn(t).AgentFor(&contracts.IncidentRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func testIncidentTaskType(t testing.TB) string {
	t.Helper()
	return registrytest.TaskType(t, testIncidentAgentID(t), &contracts.IncidentRequest{})
}
