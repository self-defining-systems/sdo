package operatorcli

import (
	"context"
	"errors"
	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
	"strings"
	"testing"
	"time"
)

func TestHealthRefreshPendingJobReturnsUnverifiedAndCanReattach(t *testing.T) {
	client := fake.NewSimpleClientset()
	registry := registrytest.BuiltIn(t)
	request := &contracts.HealthJudgeRequest{Application: "demo", Namespace: "demo", HealthObjective: "keep healthy", AcceptedSourceCommit: "abc"}
	payload, err := anypb.New(request)
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{SchemaVersion: worker_registry.SchemaVersion, TaskId: "health-refresh-pending", AgentId: "sdo-health-judge", TaskType: registrytest.TaskType(t, "sdo-health-judge", request), Request: payload, Workspace: &contracts.ProposalWorkspace{ProposalId: "refresh", CheckoutPath: "/workspace/proposal", BaseCommit: "abc"}}
	executor := worker_registry.KubernetesExecutor{ToolchainImage: "compiler:test", Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: "/workspace", PollInterval: time.Millisecond}
	for attempt := 0; attempt < 2; attempt++ {
		err := withHealthRefreshDeadline(context.Background(), 30*time.Millisecond, func(ctx context.Context) error { _, err := executor.Execute(ctx, task); return err })
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "health unverified") {
			t.Fatalf("pending Job did not fail visibly: %v", err)
		}
	}
	jobs, err := client.BatchV1().Jobs("demo").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 1 {
		t.Fatalf("reattachment replaced retained Job: %v %v", jobs, err)
	}
	initializers := jobs.Items[0].Spec.Template.Spec.InitContainers
	if len(initializers) != 1 || initializers[0].Image != "compiler:test" {
		t.Fatalf("health judge did not receive its configured compiler: %+v", initializers)
	}
}

func TestHealthRefreshDefaultAndParentDeadlines(t *testing.T) {
	for _, budget := range []time.Duration{0, time.Hour, time.Millisecond} {
		err := withHealthRefreshDeadline(context.Background(), budget, func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > defaultHealthRefreshTimeout {
				t.Fatal("missing finite judge budget")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := withHealthRefreshDeadline(ctx, time.Hour, func(ctx context.Context) error { return ctx.Err() }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
