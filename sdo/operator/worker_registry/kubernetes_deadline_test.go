package worker_registry

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestKubernetesExecutorEnforcesFrozenReflectionDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewSimpleClientset()
		task := reflectionTestTask(t)
		start := time.Now()
		task.Deadline = timestamppb.New(start.Add(time.Minute))
		executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace", PollInterval: time.Second}
		// The transport itself must honor the frozen task even when its caller's
		// context permits waiting longer. No scripted executor supplies the result.
		parent, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, err := executor.Execute(parent, task)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("pending reflection did not time out: %v", err)
		}
		if elapsed := time.Since(start); elapsed != time.Minute {
			t.Errorf("wait escaped frozen deadline: %s", elapsed)
		}
		job, err := client.BatchV1().Jobs("demo").Get(context.Background(), TaskJobName(task), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 60 {
			t.Errorf("Job active deadline not bound to task: %v", job.Spec.ActiveDeadlineSeconds)
		}
		// TTL can remove the old Job. An already-expired logical task must not
		// create another Job, even when the controller supplies a new wait context.
		if err := client.BatchV1().Jobs("demo").Delete(context.Background(), job.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		next, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		_, err = executor.Execute(next, task)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired retry did not time out: %v", err)
		}
		creates := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
				creates++
			}
		}
		if creates != 1 {
			t.Errorf("expired task recreated a Job after TTL: creates=%d", creates)
		}
	})
}
