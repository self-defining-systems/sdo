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
	"k8s.io/client-go/kubernetes/fake"
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
		AllowedRegistryHost: "registry.example",
		ReadJobLogs: func(context.Context, string, string) ([]byte, error) {
			return []byte("build log\nSDO_BUILD_METADATA={\"containerimage.digest\":\"sha256:" + strings.Repeat("a", 64) + "\"}\n"), nil
		},
	}
	type buildResult struct {
		image string
		err   error
	}
	result := make(chan buildResult, 1)
	go func() {
		image, err := backend.BuildImage(context.Background(), checkout, &contracts.ImageBuild{Image: "registry.example/app:latest", ContextPath: filepath.Join(checkout, "src"), DockerfilePath: filepath.Join(checkout, "src", "Dockerfile")})
		result <- buildResult{image, err}
	}()
	// Wait for creation and mark the deterministic Job complete.
	var job *batchv1.Job
	deadline := time.After(time.Second)
	for job == nil {
		jobs, _ := client.BatchV1().Jobs("operator").List(context.Background(), metav1.ListOptions{})
		if len(jobs.Items) > 0 {
			job = &jobs.Items[0]
			break
		}
		select {
		case <-deadline:
			t.Fatal("build Job not created")
		case <-time.After(time.Millisecond):
		}
	}
	mounts := job.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) == 0 || mounts[0].SubPath != "shared/proposals/incident-1" || !mounts[0].ReadOnly {
		t.Fatalf("proposal-only mount missing: %+v", mounts)
	}
	if job.Spec.Template.Spec.ServiceAccountName != "image-builder" || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("build Job gained controller credentials: %+v", job.Spec.Template.Spec)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
	if _, err := client.BatchV1().Jobs("operator").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-result:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.image != "registry.example/app@sha256:"+strings.Repeat("a", 64) {
			t.Fatalf("untrusted image receipt %q", result.image)
		}
	case <-time.After(time.Second):
		t.Fatal("build did not complete")
	}
}

func TestKubernetesBuildRejectsEscapingProposalAndMutableMetadata(t *testing.T) {
	backend := KubernetesBuildBackend{Client: fake.NewSimpleClientset(), Namespace: "operator", RepositoryPVC: "repository", ProposalRoot: "/repo/proposals", BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64), RegistrySecret: "registry-push", ServiceAccount: "image-builder"}
	_, err := backend.BuildImage(context.Background(), "/repo/accepted", &contracts.ImageBuild{Image: "registry.example/app:latest", ContextPath: "/repo/accepted", DockerfilePath: "/repo/accepted/Dockerfile"})
	if err == nil {
		t.Fatal("accepted checkout was mounted for image build")
	}
}

func TestKubernetesBuildRejectsUntrustedRegistryBeforeCreatingJob(t *testing.T) {
	client := fake.NewSimpleClientset()
	backend := KubernetesBuildBackend{Client: client, Namespace: "operator", RepositoryPVC: "repository", ProposalRoot: "/repo/proposals", BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64), RegistrySecret: "registry-push", ServiceAccount: "image-builder", AllowedRegistryHost: "registry.example:5000"}
	for _, image := range []string{"registry.other:5000/app:latest", "registry.example/app:latest", "registry.example:5000.evil/app:latest", "https://registry.example:5000/app:latest", "app:latest"} {
		if _, err := backend.BuildImage(context.Background(), "/repo/proposals/incident-1", &contracts.ImageBuild{Image: image, ContextPath: ".", DockerfilePath: "Dockerfile"}); err == nil {
			t.Fatalf("untrusted image %q accepted", image)
		}
	}
	jobs, err := client.BatchV1().Jobs("operator").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("untrusted target created BuildKit Job: %+v %v", jobs, err)
	}
	backend.AllowedRegistryHost = ""
	if _, err := backend.BuildImage(context.Background(), "/repo/proposals/incident-1", &contracts.ImageBuild{Image: "registry.example:5000/app:latest", ContextPath: ".", DockerfilePath: "Dockerfile"}); err == nil {
		t.Fatal("missing trusted registry accepted")
	}
}

func TestKubernetesBuildRejectsAgentOutputOptionInjection(t *testing.T) {
	backend := KubernetesBuildBackend{Client: fake.NewSimpleClientset(), Namespace: "operator", RepositoryPVC: "repository", ProposalRoot: "/repo/proposals", BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64), RegistrySecret: "registry-push", ServiceAccount: "image-builder"}
	_, err := backend.BuildImage(context.Background(), "/repo/proposals/incident-1", &contracts.ImageBuild{Image: "registry.example/app:tag,registry.insecure=true"})
	if err == nil || !strings.Contains(err.Error(), "invalid characters") {
		t.Fatalf("agent controlled BuildKit option was accepted: %v", err)
	}
}

func TestKubernetesBuildRegistryInsecureIsExplicitOperatorChoice(t *testing.T) {
	backend := KubernetesBuildBackend{Namespace: "operator", BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64)}
	secure := backend.job("build-secure", "proposal", ".", "Dockerfile", "registry.example/app:latest", time.Minute)
	assertOutput := func(job *batchv1.Job, expected string) {
		t.Helper()
		for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
			if variable.Name == "SDO_OUTPUT" {
				if variable.Value != expected {
					t.Fatalf("BuildKit output = %q, want %q", variable.Value, expected)
				}
				return
			}
		}
		t.Fatal("BuildKit output option missing")
	}
	assertOutput(secure, "type=image,name=registry.example/app:latest,push=true")
	backend.RegistryInsecure = true
	insecure := backend.job("build-insecure", "proposal", ".", "Dockerfile", "registry.example/app:latest", time.Minute)
	assertOutput(insecure, "type=image,name=registry.example/app:latest,push=true,registry.insecure=true")
}

func TestKubernetesBuildMetadataFailsClosed(t *testing.T) {
	for _, logs := range []string{
		"build done\n", "SDO_BUILD_METADATA={\"containerimage.digest\":\"latest\"}\n",
		"SDO_BUILD_METADATA={\"containerimage.digest\":\"sha256:" + strings.Repeat("a", 64) + "\"}\nSDO_BUILD_METADATA={\"containerimage.digest\":\"sha256:" + strings.Repeat("b", 64) + "\"}\n",
	} {
		if _, err := parseBuildMetadata([]byte(logs)); err == nil {
			t.Fatalf("accepted invalid metadata %q", logs)
		}
	}
}

func TestKubernetesBuildParsesBuildKitMultilineMetadata(t *testing.T) {
	logs := "#5 exporting layers done\nSDO_BUILD_METADATA={\n  \"containerimage.digest\": \"sha256:" + strings.Repeat("d", 64) + "\"\n}\n"
	digest, err := parseBuildMetadata([]byte(logs))
	if err != nil || digest != "sha256:"+strings.Repeat("d", 64) {
		t.Fatalf("metadata digest=%q err=%v", digest, err)
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
	backend := KubernetesBuildBackend{Client: client, Namespace: "operator", RepositoryPVC: "repository", ProposalRoot: root, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("c", 64), RegistrySecret: "registry-push", AllowedRegistryHost: "registry.example", ServiceAccount: "image-builder", PollInterval: time.Millisecond, ReadJobLogs: func(context.Context, string, string) ([]byte, error) {
		return []byte("SDO_BUILD_METADATA={\"containerimage.digest\":\"sha256:" + strings.Repeat("a", 64) + "\"}\n"), nil
	}}
	type result struct {
		image string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		image, err := backend.BuildImage(context.Background(), checkout, &contracts.ImageBuild{Image: "registry.example/app:latest", ContextPath: checkout, DockerfilePath: filepath.Join(checkout, "Dockerfile")})
		done <- result{image, err}
	}()
	deadline := time.After(time.Second)
	for {
		jobs, _ := client.BatchV1().Jobs("operator").List(context.Background(), metav1.ListOptions{})
		if len(jobs.Items) > 0 {
			job := jobs.Items[0]
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: "True", Message: "image build failed"}}
			if _, err := client.BatchV1().Jobs("operator").UpdateStatus(context.Background(), &job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("build Job not created")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case result := <-done:
		if result.err == nil || result.image != "" {
			t.Fatalf("failed Job produced receipt: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("failed Job did not return")
	}
}

func TestKubernetesBuildRejectsSymlinkToAcceptedCheckout(t *testing.T) {
	root := t.TempDir()
	accepted := t.TempDir()
	if err := os.Symlink(accepted, filepath.Join(root, "incident-1")); err != nil {
		t.Fatal(err)
	}
	backend := KubernetesBuildBackend{ProposalRoot: root}
	_, _, _, err := backend.proposalPaths(filepath.Join(root, "incident-1"), &contracts.ImageBuild{ContextPath: accepted, DockerfilePath: filepath.Join(accepted, "Dockerfile")})
	if err == nil {
		t.Fatal("proposal symlink escaped into accepted checkout")
	}
}
