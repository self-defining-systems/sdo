package worker_registry

import (
	"context"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"strings"
	"testing"
	"time"
)

func TestKubernetesExecutorHealthJudgeRerunIsIdempotent(t *testing.T) {
	for _, scenario := range []string{"same-request", "request-payload", "workspace-base", "result-binding"} {
		t.Run(scenario, func(t *testing.T) {
			mismatch := scenario != "same-request"
			registry, err := LoadDirectory("../../agents/registrations")
			if err != nil {
				t.Fatal(err)
			}
			mount := t.TempDir()
			request, _ := anypb.New(&contracts.HealthJudgeRequest{Application: "app", Namespace: "demo", HealthObjective: "healthy"})
			taskType, err := registry.TaskType("sdo-health-judge", &contracts.HealthJudgeRequest{})
			if err != nil {
				t.Fatal(err)
			}
			task := &contracts.AgentTask{SchemaVersion: SchemaVersion, TaskId: "health-refresh-aborted", AgentId: "sdo-health-judge", TaskType: taskType, Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "refresh", CheckoutPath: mount + "/proposals/refresh", BaseCommit: "abc"}}
			prior := proto.Clone(task).(*contracts.AgentTask)
			if scenario == "workspace-base" {
				prior.Workspace.BaseCommit = "different-base"
			} else if scenario == "request-payload" {
				prior.Request, _ = anypb.New(&contracts.HealthJudgeRequest{Application: "app", Namespace: "demo", HealthObjective: "different objective"})
			}
			payload, _ := wire.Canonical(prior)
			response, _ := anypb.New(&contracts.HealthJudgeResult{Authored: true, AgentSessionId: "original-session", DetectorIds: []string{"health"}})
			result := &contracts.AgentResult{SchemaVersion: SchemaVersion, TaskId: task.TaskId, ProposalId: "refresh", Status: "completed", Response: response}
			data, _ := wire.Canonical(result)
			name := JobName(task.TaskId)
			oldRequest := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-request", Namespace: "demo"}, Data: map[string]string{TaskDataKey: string(payload)}}
			oldResult := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-result", Namespace: "demo", CreationTimestamp: metav1.Now()}, Data: map[string]string{ResultDataKey: string(data)}}
			if scenario == "result-binding" {
				oldResult.Annotations = map[string]string{requestDigestAnnotation: "different-request"}
			}
			client := fake.NewSimpleClientset(oldRequest, oldResult)
			dispatched := 0
			client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
				dispatched++
				job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				if job.Name == name {
					t.Error("mismatching result reused stale Job identity")
				}
				nextResponse, _ := anypb.New(&contracts.HealthJudgeResult{Authored: true, AgentSessionId: "new-session", DetectorIds: []string{"health"}})
				next := proto.Clone(result).(*contracts.AgentResult)
				next.Response = nextResponse
				nextData, _ := wire.Canonical(next)
				if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result", Namespace: "demo"}, Data: map[string]string{ResultDataKey: string(nextData)}}, "demo"); err != nil {
					t.Fatal(err)
				}
				job.Status.Succeeded = 1
				job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
				return true, job, client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "demo")
			})
			executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: mount, PollInterval: time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := executor.Execute(ctx, task)
			if err != nil {
				t.Fatal(err)
			}
			var judge contracts.HealthJudgeResult
			if err := got.Response.UnmarshalTo(&judge); err != nil {
				t.Fatal(err)
			}
			wantCalls, wantSession := 0, "original-session"
			if mismatch {
				wantCalls, wantSession = 1, "new-session"
			}
			if dispatched != wantCalls || judge.AgentSessionId != wantSession {
				t.Fatalf("dispatches=%d session=%s, want %d %s", dispatched, judge.AgentSessionId, wantCalls, wantSession)
			}
			if _, err := executor.Execute(ctx, task); err != nil {
				t.Fatal(err)
			}
			if dispatched != wantCalls {
				t.Fatalf("restart dispatched again: %d", dispatched)
			}
			preserved, err := client.CoreV1().ConfigMaps("demo").Get(ctx, oldResult.Name, metav1.GetOptions{})
			if err != nil || preserved.Data[ResultDataKey] != oldResult.Data[ResultDataKey] {
				t.Fatalf("stale result overwritten: %v", err)
			}
			frozen, err := client.CoreV1().ConfigMaps("demo").Get(ctx, oldRequest.Name, metav1.GetOptions{})
			if err != nil || frozen.Data[TaskDataKey] != oldRequest.Data[TaskDataKey] {
				t.Fatalf("stale request overwritten: %v", err)
			}
			if mismatch {
				// Independent retention of the old evidence must not change
				// the new request's already-established execution identity.
				for _, name := range []string{oldRequest.Name, oldResult.Name} {
					if err := client.CoreV1().ConfigMaps("demo").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := executor.Execute(ctx, task); err != nil {
					t.Fatal(err)
				}
				if dispatched != wantCalls {
					t.Fatalf("retiring unrelated evidence redispatched task: %d", dispatched)
				}
			}
		})
	}
}

func TestKubernetesExecutorHealthJudgeRecordsSetupUsageBeforeResult(t *testing.T) {
	mount := t.TempDir()
	registry, err := LoadDirectory("../../agents/registrations")
	if err != nil {
		t.Fatal(err)
	}
	request, _ := anypb.New(&contracts.HealthJudgeRequest{Application: "app", Namespace: "demo", HealthObjective: "healthy"})
	taskType, err := registry.TaskType("sdo-health-judge", &contracts.HealthJudgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{SchemaVersion: SchemaVersion, TaskId: "setup-judge", AgentId: "sdo-health-judge", TaskType: taskType, Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "setup", CheckoutPath: mount + "/proposal", BaseCommit: "abc"}}
	client := fake.NewSimpleClientset()
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: mount, RepositoryPVCSubPath: "tenant", PollInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = executor.Execute(ctx, task)
	if err == nil {
		t.Fatal("unfinished worker unexpectedly returned a result")
	}
	job, err := client.BatchV1().Jobs("demo").Get(context.Background(), TaskJobName(task), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var usage string
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "SDO_TURN_USAGE_LOG" {
			usage = env.Value
		}
	}
	if usage != "/sdo/session/setup-usage.jsonl" {
		t.Fatalf("setup usage is not durable before result publication: %q", usage)
	}
	found := false
	for _, m := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if m.MountPath == "/sdo/session" {
			found = true
			if !strings.HasPrefix(m.SubPath, "tenant/.sdo-runtime/setup/") || m.ReadOnly {
				t.Fatalf("setup is not separate from incident usage: %+v", m)
			}
			if _, err := os.Stat(filepath.Join(mount, strings.TrimPrefix(m.SubPath, "tenant/"))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !found {
		t.Fatal("missing setup accounting mount")
	}
}

func TestKubernetesExecutorRecoversRequestBoundStagedResult(t *testing.T) {
	for _, matches := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "bound"}[matches], func(t *testing.T) {
			task := largeReflectionTask(t, t.TempDir()+"/proposal", 2<<20)
			response, _ := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
			result := &contracts.AgentResult{SchemaVersion: SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}
			data, _ := wire.Canonical(result)
			digest := "different-request"
			if matches {
				digest = taskRequestDigest(task)
			}
			client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: JobName(task.TaskId) + "-result", Namespace: "demo", Annotations: map[string]string{requestDigestAnnotation: digest}}, Data: map[string]string{ResultDataKey: string(data)}})
			executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t)}
			got, recovered, err := executor.RecoverCompleted(context.Background(), task)
			if err != nil || recovered != matches || (got != nil) != matches {
				t.Fatalf("recovered=%v result=%v err=%v; want bound=%v", recovered, got, err, matches)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("read-only recovery mutated %v", action)
				}
			}
		})
	}
}
