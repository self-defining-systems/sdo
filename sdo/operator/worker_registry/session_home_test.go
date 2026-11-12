package worker_registry

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry/internal/workerio"
)

// Every agent Job that has a session mount records its CLI session (the Codex rollout, the Claude project log) on
// that mount, whatever home the caller configured: a home on the pod's own filesystem dies with the pod, so the
// evidence sink and the runtime-artifacts export would never see the transcript. A fresh-session reflection after
// a stage-only closure used /tmp/codex and lost every transcript this way.
func TestEveryAgentWithASessionRecordsItsTranscriptOnTheSessionMount(t *testing.T) {
	cases := []struct {
		name    string
		message proto.Message
	}{
		{"incident responder", &contracts.IncidentRequest{}},
		{"reflection", &contracts.ReflectionRequest{}},
		{"deployer", &contracts.SourceDeploymentRequest{}},
		{"health judge", &contracts.HealthJudgeRequest{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request, err := anypb.New(tc.message)
			if err != nil {
				t.Fatal(err)
			}
			client := fake.NewSimpleClientset()
			executor := KubernetesExecutor{
				Client: client, Namespace: "sdo", RepositoryPVC: "repo", SessionPVCSubPath: ".sdo-runtime/sessions/abc",
				Environment: map[string]string{"CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude"},
			}
			task := &contracts.AgentTask{TaskId: "task-1", Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/work", BaseCommit: "abc"}}
			if err := executor.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "worker"}, "job-1", requestSource{ConfigMap: "req"}, "job-1-result", ""); err != nil {
				t.Fatal(err)
			}
			job, err := client.BatchV1().Jobs("sdo").Get(context.Background(), "job-1", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			homes := map[string][]string{}
			for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
				homes[variable.Name] = append(homes[variable.Name], variable.Value)
			}
			for name, want := range map[string]string{"CODEX_HOME": SessionCodexHome, "CLAUDE_CONFIG_DIR": SessionClaudeHome} {
				if got := homes[name]; len(got) != 1 || got[0] != want {
					t.Errorf("%s = %v, want exactly [%s] on the session mount", name, got, want)
				}
			}
		})
	}
}

// A responder handed to a warm standby pod gets the same session homes as a cold Job: the standby reads its launch
// environment from the published environment.json, not from the Job spec.
func TestWarmStandbyResponderRecordsItsTranscriptOnTheSessionMount(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"ready/agent", "ready/history"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(directory, name)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(directory, "control"), 0o700); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-standby-1", Namespace: "sdo"}})
	request, err := anypb.New(&contracts.IncidentRequest{IncidentId: "incident-1"})
	if err != nil {
		t.Fatal(err)
	}
	executor := KubernetesExecutor{
		Client: client, Namespace: "sdo", RepositoryPVC: "repo", SessionPVCSubPath: ".sdo-runtime/sessions/abc",
		Environment: map[string]string{"CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude"},
		WarmBinding: &WarmBinding{JobName: "sdo-standby-1", Directory: directory, ClaimedAt: workerio.Now().UTC(), CreatedAt: workerio.Now().UTC().Add(-time.Second)},
	}
	task := &contracts.AgentTask{SchemaVersion: SchemaVersion, TaskId: "task-1", AgentId: "responder", TaskType: "incident", Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/work"}}
	if err := executor.publishWarmTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(directory, "control", "environment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var environment map[string]string
	if err := json.Unmarshal(raw, &environment); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"CODEX_HOME": SessionCodexHome, "CLAUDE_CONFIG_DIR": SessionClaudeHome} {
		if environment[name] != want {
			t.Errorf("warm standby %s = %q, want %q on the session mount", name, environment[name], want)
		}
	}
}
