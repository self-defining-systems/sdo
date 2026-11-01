//go:build integration

package deployment

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestKubernetesBuildUsesOnlyProposalSubPathAndTrustedDigest(t *testing.T) {
	client := fake.NewSimpleClientset()
	root := t.TempDir()
	checkout := filepath.Join(root, "incident-1")
	if err := os.MkdirAll(filepath.Join(checkout, "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "src", "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = checkout
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	backend := KubernetesBuildBackend{
		Client: client, Namespace: "operator", RepositoryPVC: "repository", ProposalRoot: root,
		RepositoryPVCSubPath: "shared/proposals", BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64),
		RegistrySecret: "registry-push", ServiceAccount: "image-builder", PollInterval: time.Millisecond,
		AllowedRegistryHost: "registry.example", JobSuffix: func() string { return "test" },
		ReadJobLogs: func(context.Context, string, string) ([]byte, error) {
			return []byte("build log\nSDO_BUILD_METADATA={\"containerimage.digest\":\"sha256:" + strings.Repeat("a", 64) + "\"}\n"), nil
		},
	}
	var job *batchv1.Job
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job = action.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
		return false, nil, nil
	})
	image, err := backend.BuildImage(context.Background(), checkout, &contracts.ImageBuild{Image: "registry.example/app:latest", ContextPath: filepath.Join(checkout, "src"), DockerfilePath: filepath.Join(checkout, "src", "Dockerfile")})
	if err != nil {
		t.Fatal(err)
	}
	if image != "registry.example/app@sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("untrusted image receipt %q", image)
	}
	mounts := job.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) == 0 || mounts[0].SubPath != "shared/proposals/incident-1" || !mounts[0].ReadOnly {
		t.Fatalf("proposal-only mount missing: %+v", mounts)
	}
	if job.Spec.Template.Spec.ServiceAccountName != "image-builder" || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("build Job gained controller credentials: %+v", job.Spec.Template.Spec)
	}
}

func TestKubernetesBuildFailedJobCannotReturnDigest(t *testing.T) {
	client := fake.NewSimpleClientset()
	root := t.TempDir()
	checkout := filepath.Join(root, "incident")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = checkout
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	backend := KubernetesBuildBackend{Client: client, Namespace: "operator", RepositoryPVC: "repository", ProposalRoot: root, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64), RegistrySecret: "registry-push", AllowedRegistryHost: "registry.example", JobSuffix: func() string { return "test" }, ServiceAccount: "image-builder", PollInterval: time.Millisecond, ReadJobLogs: func(context.Context, string, string) ([]byte, error) {
		return []byte("SDO_BUILD_METADATA={\"containerimage.digest\":\"sha256:" + strings.Repeat("a", 64) + "\"}\n"), nil
	}}
	waits := 0
	backend.JobSuffix = func() string { return "failed-test" }
	backend.Wait = func(ctx context.Context, interval time.Duration) error {
		waits++
		if interval != time.Millisecond {
			t.Fatalf("poll interval %v", interval)
		}
		job, err := client.BatchV1().Jobs("operator").Get(ctx, "sdo-image-build-failed-test", metav1.GetOptions{})
		if err != nil {
			return err
		}
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: "True", Message: "image build failed"}}
		_, err = client.BatchV1().Jobs("operator").UpdateStatus(ctx, job, metav1.UpdateOptions{})
		return err
	}

	image, err := backend.BuildImage(context.Background(), checkout, &contracts.ImageBuild{Image: "registry.example/app:latest", ContextPath: checkout, DockerfilePath: filepath.Join(checkout, "Dockerfile")})
	if err == nil || image != "" || !strings.Contains(err.Error(), "image build failed") || waits != 1 {
		t.Fatalf("failed Job produced receipt %q: %v", image, err)
	}
}
