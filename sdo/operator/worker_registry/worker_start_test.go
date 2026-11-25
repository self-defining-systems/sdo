package worker_registry

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func startFixture(t *testing.T) (KubernetesExecutor, *contracts.AgentTask) {
	t.Helper()
	mount := t.TempDir()
	request, err := anypb.New(&contracts.IncidentRequest{IncidentId: "inc-1"})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{TaskId: "inc-1", Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: mount + "/proposals/inc-1", BaseCommit: "abc"}}
	e := KubernetesExecutor{Client: fake.NewSimpleClientset(), Namespace: "sdo", RepositoryPVC: "repo", RepositoryMountPath: mount}
	session, err := e.sessionDirectory(task)
	if err != nil {
		t.Fatal(err)
	}
	if e.SessionPVCSubPath, err = filepath.Rel(mount, session); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{session, task.Workspace.CheckoutPath} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return e, task
}

// A standby whose Job already failed is never claimed.
func TestADeadStandbyIsNeverClaimed(t *testing.T) {
	e, task := startFixture(t)
	pool := NewStandbyPool(e, "agent", "app")
	directory := filepath.Join(pool.root(), "sdo-standby-dead")
	for _, name := range []string{"agent", "history"} {
		if err := os.MkdirAll(filepath.Join(directory, "ready"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "ready", name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-standby-dead", Namespace: "sdo"}, Status: batchv1.JobStatus{Failed: 1}}
	if _, err := e.Client.BatchV1().Jobs("sdo").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pool.current = &WarmBinding{JobName: job.Name, Directory: directory, CreatedAt: time.Unix(1, 0)}
	if bound, err := pool.TryBind(context.Background(), e, task); err != nil || bound.WarmBinding != nil {
		t.Fatalf("TryBind = %v, %v; want the cold path", bound.WarmBinding, err)
	}
}

// A claimed standby that died leaves its binding and identity behind. Retiring
// it drops both, so the next worker has its own name and session.
func TestRetiringAClaimedStandbyFreesTheTask(t *testing.T) {
	ctx := context.Background()
	e, task := startFixture(t)
	binding := &WarmBinding{JobName: "sdo-standby-one", IncidentID: "inc-1", Digest: taskRequestDigest(task), Directory: filepath.Join(e.RepositoryMountPath, standbyRoot, "app", "sdo-standby-one")}
	if err := os.MkdirAll(binding.Directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeStandbyJSON(e.warmBindingPath(task), binding); err != nil {
		t.Fatal(err)
	}
	if err := e.ensureIncidentExecution(ctx, task, binding.JobName, ""); err != nil {
		t.Fatal(err)
	}
	if retired, err := e.RetireNeverStarted(ctx, task, binding.JobName, "synthetic"); err != nil || !retired {
		t.Fatalf("RetireNeverStarted = %v, %v", retired, err)
	}
	if bound, err := e.HasBoundStandby(task); err != nil || bound {
		t.Fatalf("still bound: %v, %v", bound, err)
	}
	name, err := e.taskJobName(ctx, task)
	if err != nil || name != replacementJobName("inc-1") {
		t.Fatalf("next worker = %q, %v", name, err)
	}
	if err := e.ensureIncidentExecution(ctx, task, name, ""); err != nil {
		t.Fatalf("identity of the replacement: %v", err)
	}
	if names, err := IncidentExecutionNames(ctx, e.Client, "sdo", e.RepositoryMountPath, "inc-1", true); err != nil || len(names) != 2 {
		t.Fatalf("execution names = %v, %v", names, err)
	}
}
