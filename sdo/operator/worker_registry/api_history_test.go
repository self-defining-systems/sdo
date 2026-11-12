package worker_registry

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func launchedWithSession(t *testing.T, message proto.Message, taskID string) (*batchv1.Job, string) {
	t.Helper()
	request, err := anypb.New(message)
	if err != nil {
		t.Fatal(err)
	}
	mount := t.TempDir()
	client := fake.NewSimpleClientset()
	executor := KubernetesExecutor{Client: client, Namespace: "sdo", RepositoryPVC: "repo", RepositoryMountPath: mount, SessionPVCSubPath: "sessions/s1", APIProxyImage: "operator-image"}
	task := &contracts.AgentTask{TaskId: taskID, Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/work", BaseCommit: "abc"}}
	if err := executor.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "worker"}, "job-1", requestSource{ConfigMap: "req"}, "job-1-result", ""); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("sdo").Get(context.Background(), "job-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return job, mount
}

func sidecar(job *batchv1.Job) *corev1.Container {
	for i := range job.Spec.Template.Spec.InitContainers {
		if job.Spec.Template.Spec.InitContainers[i].Name == "kube-api-history" {
			return &job.Spec.Template.Spec.InitContainers[i]
		}
	}
	return nil
}

// Every agent Job with a session reaches the API through the recording proxy: the responder with a writable
// incident scope, every other agent with a read-only task scope.
func TestEveryAgentJobGetsTheRecordingProxy(t *testing.T) {
	cases := []struct {
		name    string
		message proto.Message
		scope   string
	}{
		{"responder", &contracts.IncidentRequest{IncidentId: "inc-1"}, "incident:inc-1"},
		{"reflection", &contracts.ReflectionRequest{}, "task:task-1"},
		{"deployer", &contracts.SourceDeploymentRequest{}, "task:task-1"},
		{"health judge", &contracts.HealthJudgeRequest{}, "task:task-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job, _ := launchedWithSession(t, tc.message, "task-1")
			side := sidecar(job)
			if side == nil {
				t.Fatal("no kube-api-history sidecar")
			}
			if side.Image != "operator-image" {
				t.Fatalf("sidecar image = %q", side.Image)
			}
			index := slices.Index(side.Command, "--scope")
			if index < 0 || side.Command[index+1] != tc.scope {
				t.Fatalf("sidecar command = %v, want --scope %s", side.Command, tc.scope)
			}
			if jobEnvironment(job)["SDO_API_PROXY_URL"] != "http://127.0.0.1:18080" {
				t.Fatalf("agent does not point at the proxy: %v", jobEnvironment(job))
			}
		})
	}
}

// The reflection agent reads the incident's recording, so its own recording goes to a separate directory, with
// the generation boundary prepared before launch.
func TestReflectionRecordsToItsOwnHistoryDirectory(t *testing.T) {
	job, mount := launchedWithSession(t, &contracts.ReflectionRequest{}, "task-1")
	side := sidecar(job)
	var sub string
	for _, m := range side.VolumeMounts {
		if m.MountPath == "/sdo/history" {
			sub = m.SubPath
		}
	}
	if sub != "sessions/s1/api-history-reflection" {
		t.Fatalf("reflection sidecar history subpath = %q", sub)
	}
	if _, err := os.Stat(filepath.Join(mount, "sessions/s1/api-history-reflection.generation")); err != nil {
		t.Fatalf("generation boundary not prepared before launch: %v", err)
	}
	if got := jobEnvironment(job)["SDO_API_HISTORY_DIR"]; got != "/sdo/session/api-history" {
		t.Fatalf("reflection must keep reading the incident recording: %q", got)
	}
}

func TestLifecycleTaskRecordsUnderItsOwnSession(t *testing.T) {
	job, mount := launchedWithSession(t, &contracts.SourceDeploymentRequest{}, "task-1")
	if _, err := os.Stat(filepath.Join(mount, "sessions/s1/api-history.generation")); err != nil {
		t.Fatalf("generation boundary not prepared before launch: %v", err)
	}
	if sidecar(job) == nil {
		t.Fatal("no sidecar")
	}
}
