package worker_registry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sdo.dev/operator/contracts/wire"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func reflectionTestRegistry(t *testing.T) *Registry {
	t.Helper()
	registry, err := New(&contracts.AgentRegistration{
		SchemaVersion: SchemaVersion, AgentId: "external-reflector",
		Capabilities: []*contracts.AgentCapability{{
			TaskType:       "custom-reflection",
			RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionRequest",
			ResultTypeUrl:  "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionResult",
		}},
		Launch: &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{Image: "example/reflector:1", Command: []string{"/reflector"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func reflectionTestTask(t *testing.T) *contracts.AgentTask {
	t.Helper()
	payload, err := anypb.New(&contracts.ReflectionRequest{OutcomeCommit: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.AgentTask{
		SchemaVersion: SchemaVersion, TaskId: "reflection:incident-1:abc", AgentId: "external-reflector", TaskType: "custom-reflection", Request: payload,
		Workspace: &contracts.ProposalWorkspace{ProposalId: "reflection-1", CheckoutPath: "/workspace/proposals/reflection-1", BaseCommit: "abc"},
	}
}

func TestKubernetesExecutorRunsRegisteredReflectionInPrivateCheckout(t *testing.T) {
	t.Setenv("SDO_DEV_MOUNT", "1")
	client := fake.NewSimpleClientset()
	task := reflectionTestTask(t)
	executor := KubernetesExecutor{
		ToolchainImage: "controller:compiler", Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), ServiceAccount: "sdo-worker",
		RepositoryPVC: "repo", RepositoryMountPath: "/workspace", RepositoryPVCSubPath: "tenant", PollInterval: time.Millisecond,
		SessionPVCSubPath:     "tenant/.sdo-runtime/sessions/incident-hash",
		CredentialsSecret:     "agent-provider-credentials",
		HealthyBaseline:       true,
		LateFindingsConfigMap: "sdo-late-incident",
		Environment:           map[string]string{"SDO_ACTOR": "human", "SDO_PHASE": "initial", "SDO_BASE_COMMIT": "forged"},
	}
	results := make(chan *contracts.AgentResult, 1)
	errs := make(chan error, 1)
	go func() { result, err := executor.Execute(context.Background(), task); results <- result; errs <- err }()
	jobName := JobName(task.TaskId)
	var job *batchv1.Job
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		found, err := client.BatchV1().Jobs("demo").Get(context.Background(), jobName, metav1.GetOptions{})
		if err == nil {
			job = found
			break
		}
		time.Sleep(time.Millisecond)
	}
	if job == nil {
		t.Fatal("agent Job was not created")
	}
	if len(job.Spec.Template.Spec.InitContainers) != 1 || job.Spec.Template.Spec.InitContainers[0].Image != "controller:compiler" {
		t.Fatal("reflection job missing controller compiler")
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "example/reflector:1" || container.Command[0] != "/reflector" {
		t.Fatalf("ignored registration: %#v", container)
	}
	if container.WorkingDir != task.Workspace.CheckoutPath {
		t.Fatalf("wrong working directory: %q", container.WorkingDir)
	}
	for name, want := range map[string]string{"SDO_ACTOR": "responder", "SDO_PHASE": "reflection", "SDO_BASE_COMMIT": "abc", "SDO_HEALTHY_BASELINE": "/sdo/healthy-baseline"} {
		found := 0
		for _, env := range container.Env {
			if env.Name == name {
				found++
				if env.Value != want {
					t.Fatalf("%s = %s, want %s", name, env.Value, want)
				}
			}
		}
		if found != 1 {
			t.Fatalf("environment %s has %d entries", name, found)
		}
	}
	foundSession := false
	repositoryVolumes := 0
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == "repo" {
			repositoryVolumes++
		}
	}
	if repositoryVolumes != 1 {
		t.Fatalf("repository PVC must be declared once even when sessions are enabled: %#v", job.Spec.Template.Spec.Volumes)
	}
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == "/sdo/session" {
			foundSession = true
			if mount.Name != "repository" || mount.SubPath != "tenant/.sdo-runtime/sessions/incident-hash" || mount.ReadOnly {
				t.Fatalf("session mount is not a narrow writable incident subPath: %#v", mount)
			}
		}
		if mount.Name == "late-findings" && (mount.MountPath != "/sdo/late-findings" || !mount.ReadOnly) {
			t.Fatalf("late findings projection is writable or misplaced: %#v", mount)
		}
		if mount.MountPath == "/workspace" || mount.SubPath == "tenant/application" {
			t.Fatalf("worker can access accepted repository: %#v", mount)
		}
		if mount.MountPath == "/sdo/healthy-baseline" {
			if !mount.ReadOnly || mount.SubPath != "tenant/.sdo-baseline/healthy" {
				t.Fatalf("baseline must be narrow and read-only: %+v", mount)
			}
			continue
		}
		if mount.Name == "repository" && mount.MountPath != "/sdo/session" &&
			(mount.MountPath != task.Workspace.CheckoutPath || mount.SubPath != "tenant/proposals/reflection-1") {
			t.Fatalf("repository mount leaks outside proposal checkout: %#v", mount)
		}
	}
	if !foundSession {
		t.Fatal("isolated incident session mount missing")
	}
	if len(container.EnvFrom) != 1 || container.EnvFrom[0].SecretRef.Name != "agent-provider-credentials" {
		t.Fatalf("worker received an unexpected Secret: %#v", container.EnvFrom)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "late-findings" && (volume.ConfigMap == nil || volume.ConfigMap.Name != "sdo-late-incident") {
			t.Fatalf("late findings volume is not a ConfigMap projection: %#v", volume)
		}
		if volume.Secret != nil && volume.Secret.SecretName != "agent-provider-credentials" {
			t.Fatalf("worker mounted broker-only Secret %q", volume.Secret.SecretName)
		}
	}
	response, err := anypb.New(&contracts.ReflectionResult{Summary: "learned", LearningDecision: "no_change", NoChangeReason: "already covered"})
	if err != nil {
		t.Fatal(err)
	}
	wireResult := &contracts.AgentResult{SchemaVersion: SchemaVersion, TaskId: task.TaskId, Status: "completed", ProposalId: task.Workspace.ProposalId, Response: response}
	data, err := wire.Canonical(wireResult)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: jobName + "-result", Namespace: "demo"}, Data: map[string]string{ResultDataKey: string(data)},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	if _, err := client.BatchV1().Jobs("demo").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if got := <-results; got.GetTaskId() != task.TaskId {
		t.Fatalf("wrong result: %#v", got)
	}
}

func TestKubernetesExecutorRejectsCheckoutOutsideMount(t *testing.T) {
	task := reflectionTestTask(t)
	task.Workspace.CheckoutPath = "/accepted/application"
	executor := KubernetesExecutor{Client: fake.NewSimpleClientset(), Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace"}
	if _, err := executor.Execute(context.Background(), task); err == nil {
		t.Fatal("accepted checkout outside proposal mount")
	}
}

func TestJudgeJobPinsMemoryCheckOwnership(t *testing.T) {
	client := fake.NewSimpleClientset()
	executor := KubernetesExecutor{Client: client, Namespace: "demo", RepositoryPVC: "repo", Environment: map[string]string{"SDO_ACTOR": "human", "SDO_PHASE": "reflection", "SDO_BASE_COMMIT": "forged"}}
	request, err := anypb.New(&contracts.HealthJudgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/workspace/judge", BaseCommit: "accepted-architecture"}}
	if err := executor.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "judge:1"}, "judge", requestSource{ConfigMap: "request"}, "result", "judge"); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("demo").Get(context.Background(), "judge", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"SDO_ACTOR": "health_judge", "SDO_PHASE": "initial", "SDO_BASE_COMMIT": "accepted-architecture"} {
		count := 0
		for _, env := range job.Spec.Template.Spec.Containers[0].Env {
			if env.Name == name {
				count++
				if env.Value != want {
					t.Fatalf("%s = %s, want %s", name, env.Value, want)
				}
			}
		}
		if count != 1 {
			t.Fatalf("%s has %d values", name, count)
		}
	}
}

func TestKubernetesExecutorCanMountSourceReadOnly(t *testing.T) {
	client := fake.NewSimpleClientset()
	task := reflectionTestTask(t)
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace", RepositoryReadOnly: true}
	registration, err := executor.Registry.Resolve(task.AgentId, task.TaskType)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ensureJob(context.Background(), task, registration.GetKubernetesJob(), "read-only-test", requestSource{ConfigMap: "request"}, "result", "proposals/reflection-1"); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("demo").Get(context.Background(), "read-only-test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == task.Workspace.CheckoutPath {
			if !mount.ReadOnly {
				t.Fatal("worker source mount is writable")
			}
			return
		}
	}
	t.Fatal("worker source mount is absent")
}

func TestWorkerJobKeepsToolAccessWithReadOnlySource(t *testing.T) {
	client := fake.NewSimpleClientset()
	task := reflectionTestTask(t)
	registration := reflectionTestRegistry(t)
	launch, err := registration.Resolve("external-reflector", task.TaskType)
	if err != nil {
		t.Fatal(err)
	}
	base := KubernetesExecutor{Client: client, Namespace: "demo", Registry: registration,
		RepositoryPVC: "repo", RepositoryMountPath: "/workspace", SessionPVCSubPath: "sessions/incident",
		Environment: map[string]string{"SDO_TOOLS_DIR": "/sdo/session/tools"}}
	for _, tc := range []struct {
		name, account string
		readOnly      bool
	}{
		{"responder", "sdo-responder", false},
		{"reflector", "sdo-reflector", true},
	} {
		executor := base
		executor.ServiceAccount = tc.account
		executor.RepositoryReadOnly = tc.readOnly
		if err := executor.ensureJob(context.Background(), task, launch.GetKubernetesJob(), tc.name, requestSource{ConfigMap: "request"}, "result", "proposals/reflection-1"); err != nil {
			t.Fatal(err)
		}
		job, err := client.BatchV1().Jobs("demo").Get(context.Background(), tc.name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if job.Spec.Template.Spec.ServiceAccountName != tc.account {
			t.Fatalf("%s service account: %q", tc.name, job.Spec.Template.Spec.ServiceAccountName)
		}
		mounts := map[string]corev1.VolumeMount{}
		for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
			mounts[mount.MountPath] = mount
		}
		sessionDir := ""
		for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
			if variable.Name == "SDO_SESSION_DIR" {
				sessionDir = variable.Value
			}
		}
		if sessionDir != "/sdo/session" {
			t.Errorf("%s Job does not tell the worker where the incident session is mounted: %q", tc.name, sessionDir)
		}
		for _, surface := range []struct {
			name     string
			readOnly bool
		}{{"tools", true}, {"tools/requests", false}, {"tools/responses", false}} {
			mount, ok := mounts["/sdo/session/"+surface.name]
			if !ok || mount.Name != "repository" || mount.SubPath != "sessions/incident/"+surface.name || mount.ReadOnly != surface.readOnly {
				t.Errorf("tool directory can be replaced or has wrong access: %+v", mount)
			}
		}
		if !mounts["/sdo/request"].ReadOnly || mounts["/sdo/session"].ReadOnly || mounts["/tmp"].ReadOnly || mounts[task.Workspace.CheckoutPath].ReadOnly != tc.readOnly {
			t.Fatalf("%s lost request, session, scratch, or source mount contract: %+v", tc.name, mounts)
		}
	}
}

func largeReflectionTask(t *testing.T, checkout string, snapshotBytes int) *contracts.AgentTask {
	t.Helper()
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: "abc", Playbooks: []*contracts.MemoryPlaybook{{Id: "big", Markdown: strings.Repeat("playbook line\n", snapshotBytes/14)}}}
	payload, err := anypb.New(&contracts.ReflectionRequest{OutcomeCommit: "abc", MemorySnapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.AgentTask{
		SchemaVersion: SchemaVersion, TaskId: "reflection:incident-big:abc", AgentId: "external-reflector", TaskType: "custom-reflection", Request: payload,
		Workspace: &contracts.ProposalWorkspace{ProposalId: "reflection-big", CheckoutPath: checkout, BaseCommit: "abc"},
	}
}

func TestKubernetesExecutorStagesRequestsAboveConfigMapBudgetOnRepositoryVolume(t *testing.T) {
	mount := t.TempDir()
	client := fake.NewSimpleClientset()
	task := largeReflectionTask(t, mount+"/proposals/reflection-big", 2<<20)
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: mount, RepositoryPVCSubPath: "tenant", PollInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := executor.Execute(ctx, task); done <- err }()
	jobName := JobName(task.TaskId)
	var job *batchv1.Job
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) && job == nil; time.Sleep(time.Millisecond) {
		job, _ = client.BatchV1().Jobs("demo").Get(ctx, jobName, metav1.GetOptions{})
	}
	if job == nil {
		t.Fatal("agent Job was not created")
	}
	if maps, _ := client.CoreV1().ConfigMaps("demo").List(ctx, metav1.ListOptions{}); len(maps.Items) != 0 {
		t.Fatalf("an oversized request must not use a ConfigMap: %d created", len(maps.Items))
	}
	var request *corev1.VolumeMount
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/sdo/request" {
			request = &mount
		}
	}
	wantSubPath := "tenant/.sdo-runtime/requests/" + jobName
	if request == nil || request.Name != "repository" || request.SubPath != wantSubPath || !request.ReadOnly {
		t.Fatalf("worker request is not a read-only repository subPath: %+v", request)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.ConfigMap != nil {
			t.Fatalf("job mounts a ConfigMap request: %+v", volume)
		}
	}
	staged, err := os.ReadFile(filepath.Join(mount, ".sdo-runtime/requests", jobName, TaskDataKey))
	if err != nil {
		t.Fatal(err)
	}
	want, err := wire.Canonical(task)
	if err != nil || string(staged) != string(want) || len(staged) <= 1<<20 {
		t.Fatalf("staged request differs from the canonical task (%d bytes, err %v)", len(staged), err)
	}
	cancel()
	<-done
}

func TestKubernetesExecutorRejectsRequestOverStagedBudgetNamingTheComponent(t *testing.T) {
	mount := t.TempDir()
	client := fake.NewSimpleClientset()
	task := largeReflectionTask(t, mount+"/proposals/reflection-big", MaxStagedRequestBytes+1<<20)
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: mount, PollInterval: time.Millisecond}
	_, err := executor.Execute(context.Background(), task)
	var tooLarge *RequestTooLargeError
	if !errors.As(err, &tooLarge) || !strings.Contains(err.Error(), "memory_snapshot=") {
		t.Fatalf("want RequestTooLargeError naming memory_snapshot, got %v", err)
	}
	if jobs, _ := client.BatchV1().Jobs("demo").List(context.Background(), metav1.ListOptions{}); len(jobs.Items) != 0 {
		t.Fatal("a job was created for an over-budget request")
	}
}

func TestKubernetesExecutorReportsUnwritableRequestStaging(t *testing.T) {
	mount := t.TempDir()
	task := largeReflectionTask(t, mount+"/proposals/reflection-big", 2<<20)
	if err := os.WriteFile(filepath.Join(mount, ".sdo-runtime"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	executor := KubernetesExecutor{Client: fake.NewSimpleClientset(), Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: mount}
	var staging *RequestStagingError
	if _, err := executor.Execute(context.Background(), task); !errors.As(err, &staging) {
		t.Fatalf("want RequestStagingError, got %v", err)
	}
}
