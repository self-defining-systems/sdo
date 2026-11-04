package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
)

func proposalCheckout(t *testing.T) (root, checkout string) {
	t.Helper()
	root = t.TempDir()
	checkout = filepath.Join(root, "proposal")
	if err := os.MkdirAll(filepath.Join(checkout, "svc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "svc", "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, checkout)
	testutil.Git(t, checkout, "add", "-A")
	testutil.Git(t, checkout, "commit", "-qm", "synthetic")
	return root, checkout
}

func testBackend(root string, client *fake.Clientset, timeout time.Duration) KubernetesBuildBackend {
	return KubernetesBuildBackend{
		Client: client, Namespace: "sdo-control", RepositoryPVC: "repo", ProposalRoot: root,
		BuildKitImage:  "reg.example/buildkit@" + synthDigest,
		RegistrySecret: "push", AllowedRegistryHost: "reg.example", ServiceAccount: "builder",
		PollInterval: time.Millisecond, Timeout: timeout, JobSuffix: func() string { return "t" },
	}
}

func testBuild(checkout string) *contracts.ImageBuild {
	return &contracts.ImageBuild{Image: "reg.example/app/api:1", ContextPath: filepath.Join(checkout, "svc"), DockerfilePath: filepath.Join(checkout, "svc", "Dockerfile")}
}

func TestBuildDeadlineIsATimeoutNotAPlanError(t *testing.T) {
	root, checkout := proposalCheckout(t)
	_, err := testBackend(root, fake.NewSimpleClientset(), 30*time.Millisecond).BuildImage(context.Background(), checkout, testBuild(checkout))
	var timeout *BuildTimeoutError
	if !errors.As(err, &timeout) || timeout.Budget {
		t.Fatalf("a build that outlives its deadline must report BuildTimeoutError, got %v", err)
	}
}

func TestBuildJobDeadlineFailureIsATimeout(t *testing.T) {
	root, checkout := proposalCheckout(t)
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-image-build-t"}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline"}}}}, nil
	})
	_, err := testBackend(root, client, time.Minute).BuildImage(context.Background(), checkout, testBuild(checkout))
	var timeout *BuildTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("Kubernetes DeadlineExceeded must report BuildTimeoutError, got %v", err)
	}
}

// A timed-out build is deleted, so it stops consuming the node that the
// retry needs.
func TestTimedOutBuildJobIsDeleted(t *testing.T) {
	root, checkout := proposalCheckout(t)
	client := fake.NewSimpleClientset()
	if _, err := testBackend(root, client, 30*time.Millisecond).BuildImage(context.Background(), checkout, testBuild(checkout)); err == nil {
		t.Fatal("expected a timeout")
	}
	jobs, err := client.BatchV1().Jobs("sdo-control").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("abandoned build Job must be deleted: %v, err %v", jobs.Items, err)
	}
}
