package worker_registry

import (
	"bytes"
	"context"
	"encoding/json"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestIncidentJobUsesControllerSeedWithoutCopy(t *testing.T) {
	client := fake.NewSimpleClientset()
	request, _ := anypb.New(&contracts.IncidentRequest{IncidentId: "incident"})
	task := &contracts.AgentTask{TaskId: "incident", Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/workspace/proposal"}}
	e := KubernetesExecutor{Client: client, Namespace: "control", RepositoryPVC: "repo", RepositoryPVCSubPath: "tenant", ToolchainImage: "controller:compiler", SessionPVCSubPath: "tenant/session"}
	if err := e.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "worker", Command: []string{"python3", "-m", "worker"}}, "incident", requestSource{ConfigMap: "request"}, "result", "tenant/proposal"); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("control").Get(context.Background(), "incident", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, init := range job.Spec.Template.Spec.InitContainers {
		if init.Name == "prepare-toolchain" {
			t.Fatal("incident still copies the toolchain")
		}
	}
	mounts := map[string]bool{}
	for _, m := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if m.MountPath == "/usr/local/go" || m.MountPath == "/go" {
			if m.Name != "repository" || !m.ReadOnly {
				t.Fatalf("compiler authority: %+v", m)
			}
			mounts[m.MountPath] = true
		}
	}
	if len(mounts) != 2 {
		t.Fatalf("missing offline compiler/cache: %+v", mounts)
	}
}

func TestResponderStartupRecordContainsEveryMeasuredBoundary(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC))
	always := corev1.ContainerRestartPolicyAlways
	record := &ResponderStartup{IncidentID: "i", JobName: "j", InitContainers: map[string]ContainerStartup{}, Missing: []string{}}
	record.observe(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: now}}, []corev1.Pod{{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "history", RestartPolicy: &always}, {Name: "legacy-init"}}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "history", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}}, {Name: "legacy-init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{StartedAt: now, FinishedAt: now}}}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}}}}}})
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "session"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "session/startup-worker.json"), []byte(`{"worker_ready":"2026-10-25T01:00:01Z","agent_process_started":"2026-10-25T01:00:02Z","first_model_request_sent":"2026-10-25T01:00:03Z","first_model_request_source":"codex.otel.api_request.timestamp-minus-duration"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	e := KubernetesExecutor{Client: fake.NewSimpleClientset(), Namespace: "control", RepositoryMountPath: root, SessionPVCSubPath: "tenant/session", RepositoryPVCSubPath: "tenant"}
	e.emitStartup(record)
	var row struct {
		Message string `json:"msg"`
		Startup string `json:"startup_json"`
	}
	if err := json.Unmarshal(logs.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	var decoded ResponderStartup
	if err := json.Unmarshal([]byte(row.Startup), &decoded); err != nil {
		t.Fatal(err)
	}
	if row.Message != "responder_startup" || len(decoded.Missing) != 0 || decoded.FirstModelRequestSent == nil || len(decoded.InitContainers) != 2 {
		t.Fatalf("incomplete telemetry: %s", logs.String())
	}
}
