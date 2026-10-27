package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/worker_registry"
)

// withRetiredStageReports rewrites an agent task payload the way a build that
// still had the automatic response stage persisted it: the packed incident
// request carries prior_stages.
func withRetiredStageReports(t *testing.T, payload string) string {
	t.Helper()
	var task map[string]any
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		t.Fatal(err)
	}
	request, ok := task["request"].(map[string]any)
	if !ok {
		t.Fatalf("task has no packed request: %s", payload)
	}
	request["prior_stages"] = []any{map[string]any{"stage": "deterministic", "outcome": "verified"}}
	old, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return string(old)
}

func restartDispatcher(t *testing.T, client *fake.Clientset) KubernetesJobDispatcher {
	t.Helper()
	return KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "application-repository", CredentialsSecret: "sdo-codex",
		RepositoryMountPath: "/workspace", RepositoryPVCSubPath: "shared", PollInterval: time.Millisecond,
	}
}

// A controller that restarts on a new build with an incident in flight reads the
// prepared task and the "-request" ConfigMap the old build wrote. Both still
// carry the retired stage reports and must dispatch.
func TestRestartAfterStageDeletionDispatchesStoredTask(t *testing.T) {
	request := goldenRequest(t)
	prepared, err := restartDispatcher(t, fake.NewSimpleClientset()).PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	old := withRetiredStageReports(t, prepared.PreparedAgentTask)

	t.Run("existing request ConfigMap", func(t *testing.T) {
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: worker_registry.JobName(request.IncidentID) + "-request", Namespace: "demo"},
			Data:       map[string]string{jobRequestDataKey: old},
		}
		dispatcher := restartDispatcher(t, fake.NewSimpleClientset(configMap))
		resumed, err := dispatcher.PrepareDispatch(context.Background(), request)
		if err != nil {
			t.Fatalf("restart cannot reuse the stored request: %v", err)
		}
		if resumed.PreparedAgentTask != old {
			t.Fatal("restart did not keep the stored task bytes")
		}
	})

	t.Run("prepared task in controller state", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		dispatcher := restartDispatcher(t, client)
		restored := request
		restored.PreparedAgentTask = old
		ctx, cancel := context.WithCancel(context.Background())
		var dispatchErr error
		goUntilCleanup(t, cancel, func() { _, dispatchErr = dispatcher.Dispatch(ctx, restored) })
		var job *batchv1.Job = awaitJob(t, client, IncidentJobName(request.IncidentID))
		if job == nil {
			t.Fatalf("stored task did not dispatch: %v", dispatchErr)
		}
	})
}
