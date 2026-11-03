//go:build integration && controller_e2e

package e2e

import (
	"context"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestDeletingJobRetiresDependentsWithoutReplacement(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "control", UID: "job", DeletionTimestamp: &now, Finalizers: []string{metav1.FinalizerDeleteDependents}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "control", UID: "pod", OwnerReferences: controllerRef("Job", "batch/v1", job.Name, job.UID)}}
	client := fake.NewSimpleClientset(job, pod)
	k := &Kubelet{cluster: &Cluster{Client: client}}
	for range 2 {
		if err := k.reconcileJobs(ctx); err != nil {
			t.Fatal(err)
		}
		pods, err := client.CoreV1().Pods("control").List(ctx, metav1.ListOptions{})
		if err != nil || len(pods.Items) != 0 {
			t.Fatalf("deleting Job kept or replaced workers: pods=%v err=%v", pods, err)
		}
	}
	current, err := client.BatchV1().Jobs("control").Get(ctx, job.Name, metav1.GetOptions{})
	if err == nil && len(current.Finalizers) != 0 {
		t.Fatal("foreground deletion never finishes")
	}
}
