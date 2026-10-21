package worker_registry

import (
	"context"
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
			TaskType:       "outcome-reflection",
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
		SchemaVersion: SchemaVersion, TaskId: "reflection:incident-1:abc", AgentId: "external-reflector", TaskType: "outcome-reflection", Request: payload,
		Workspace: &contracts.ProposalWorkspace{ProposalId: "reflection-1", CheckoutPath: "/workspace/proposals/reflection-1", BaseCommit: "abc"},
	}
}

func TestKubernetesExecutorRunsRegisteredReflectionInPrivateCheckout(t *testing.T) {
	client := fake.NewSimpleClientset()
	task := reflectionTestTask(t)
	var phases []string
	executor := KubernetesExecutor{
		Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), ServiceAccount: "sdo-worker",
		RepositoryPVC: "repo", RepositoryMountPath: "/workspace", RepositoryPVCSubPath: "tenant", PollInterval: time.Millisecond,
		SessionPVCSubPath:      "tenant/.sdo-runtime/sessions/incident-hash",
		CapturedCorpusReadOnly: true,
		CredentialsSecret:      "agent-provider-credentials",
		LateFindingsConfigMap:  "sdo-late-incident",
		OnPhase:                func(_ context.Context, phase string) error { phases = append(phases, phase); return nil },
	}
	results := make(chan *contracts.AgentResult, 1)
	errors := make(chan error, 1)
	go func() { result, err := executor.Execute(context.Background(), task); results <- result; errors <- err }()
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
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "example/reflector:1" || container.Command[0] != "/reflector" {
		t.Fatalf("ignored registration: %#v", container)
	}
	if container.WorkingDir != task.Workspace.CheckoutPath {
		t.Fatalf("wrong working directory: %q", container.WorkingDir)
	}
	foundSession := false
	foundCorpus := false
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == "/sdo/session/corpus" {
			foundCorpus = true
			if !mount.ReadOnly || mount.SubPath != "tenant/.sdo-runtime/sessions/incident-hash/corpus" {
				t.Fatalf("capture mount is not read-only: %#v", mount)
			}
			continue
		}
		if mount.MountPath == "/sdo/session" {
			foundSession = true
			if mount.Name != "repository" {
				t.Fatalf("a second volume for the same PVC hangs the kubelet; mount the repository volume with another subPath: %#v", mount)
			}
			if mount.MountPath != "/sdo/session" || mount.SubPath != "tenant/.sdo-runtime/sessions/incident-hash" || mount.ReadOnly {
				t.Fatalf("session mount is not a narrow writable incident subPath: %#v", mount)
			}
		}
		if mount.Name == "late-findings" && (mount.MountPath != "/sdo/late-findings" || !mount.ReadOnly) {
			t.Fatalf("late findings projection is writable or misplaced: %#v", mount)
		}
		if mount.MountPath == "/workspace" || mount.SubPath == "tenant/application" {
			t.Fatalf("worker can access accepted repository: %#v", mount)
		}
		if mount.Name == "repository" && mount.MountPath != "/sdo/session" && (mount.MountPath != task.Workspace.CheckoutPath || mount.SubPath != "tenant/proposals/reflection-1") {
			t.Fatalf("repository mount leaks outside proposal checkout: %#v", mount)
		}
	}
	if !foundSession {
		t.Fatal("isolated incident session mount missing")
	}
	if !foundCorpus {
		t.Fatal("read-only captured corpus mount missing")
	}
	if len(container.EnvFrom) != 1 || container.EnvFrom[0].SecretRef.Name != "agent-provider-credentials" {
		t.Fatalf("worker received an unexpected Secret: %#v", container.EnvFrom)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "session" {
			t.Fatalf("session must reuse the repository volume, not declare a second PVC volume: %#v", volume)
		}
		if volume.Name == "late-findings" && (volume.ConfigMap == nil || volume.ConfigMap.Name != "sdo-late-incident") {
			t.Fatalf("late findings volume is not a ConfigMap projection: %#v", volume)
		}
		if volume.Secret != nil && volume.Secret.SecretName != "agent-provider-credentials" {
			t.Fatalf("worker mounted broker-only Secret %q", volume.Secret.SecretName)
		}
	}
	if profile := job.Spec.Template.Spec.SecurityContext.SeccompProfile; profile == nil || profile.Type != corev1.SeccompProfileTypeUnconfined {
		t.Fatalf("Codex's bubblewrap sandbox needs user namespaces, which RuntimeDefault seccomp denies: %#v", profile)
	}
	foundHomes := false
	for _, mount := range container.VolumeMounts {
		if mount.Name == "codex-homes" {
			foundHomes = mount.MountPath == "/sdo/codex-homes" && !mount.ReadOnly
		}
	}
	homesEnv := ""
	for _, variable := range container.Env {
		if variable.Name == "SDO_CODEX_HOME_ROOT" {
			homesEnv = variable.Value
		}
	}
	homesVolume := false
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "codex-homes" {
			homesVolume = volume.EmptyDir != nil
		}
	}
	if !foundHomes || !homesVolume || homesEnv != "/sdo/codex-homes" {
		t.Fatalf("Codex turns need a private writable home root outside /tmp and the checkout: mount=%v volume=%v env=%q", foundHomes, homesVolume, homesEnv)
	}
	response, err := anypb.New(&contracts.ReflectionResult{Summary: "learned", LearningDecision: "no_change", NoChangeReason: "already covered"})
	if err != nil {
		t.Fatal(err)
	}
	wireResult := &contracts.AgentResult{SchemaVersion: SchemaVersion, TaskId: task.TaskId, Status: "completed", ProposalId: task.Workspace.ProposalId, Response: response}
	data, err := canonicalJSON(wireResult)
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
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	if got := <-results; got.GetTaskId() != task.TaskId {
		t.Fatalf("wrong result: %#v", got)
	}
	if len(phases) < 2 || phases[0] != "reflection_job_created" || phases[len(phases)-1] != "worker_result_written" {
		t.Fatalf("worker transport phases: %v", phases)
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

func TestKubernetesExecutorCanMountInvestigationSourceReadOnly(t *testing.T) {
	client := fake.NewSimpleClientset()
	task := reflectionTestTask(t)
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace", RepositoryReadOnly: true}
	registration, err := executor.Registry.Resolve(task.AgentId, task.TaskType)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ensureJob(context.Background(), task, registration.GetKubernetesJob(), "read-only-test", "request", "result", "proposals/reflection-1"); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("demo").Get(context.Background(), "read-only-test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == task.Workspace.CheckoutPath {
			if !mount.ReadOnly {
				t.Fatal("investigator source mount is writable")
			}
			return
		}
	}
	t.Fatal("investigator source mount is absent")
}
