package worker_registry

import (
	"context"
	"sdo.dev/operator/contracts/wire"
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
			TaskType:       "outcome-reflection-direct-v1",
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
		SchemaVersion: SchemaVersion, TaskId: "reflection:incident-1:abc", AgentId: "external-reflector", TaskType: "outcome-reflection-direct-v1", Request: payload,
		Workspace: &contracts.ProposalWorkspace{ProposalId: "reflection-1", CheckoutPath: "/workspace/proposals/reflection-1", BaseCommit: "abc"},
	}
}

func TestKubernetesExecutorRunsRegisteredReflectionInPrivateCheckout(t *testing.T) {
	client := fake.NewSimpleClientset()
	task := reflectionTestTask(t)
	executor := KubernetesExecutor{
		Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), ServiceAccount: "sdo-worker",
		RepositoryPVC: "repo", RepositoryMountPath: "/workspace", RepositoryPVCSubPath: "tenant", PollInterval: time.Millisecond,
		SessionPVCSubPath:     "tenant/.sdo-runtime/sessions/incident-hash",
		CredentialsSecret:     "agent-provider-credentials",
		LateFindingsConfigMap: "sdo-late-incident",
		Environment:           map[string]string{"SDO_ACTOR": "human", "SDO_PHASE": "initial", "SDO_BASE_COMMIT": "forged"},
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
	for name, want := range map[string]string{"SDO_ACTOR": "responder", "SDO_PHASE": "reflection", "SDO_BASE_COMMIT": "abc"} {
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
	if err := <-errors; err != nil {
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
