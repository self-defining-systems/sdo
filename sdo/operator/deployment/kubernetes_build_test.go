package deployment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

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
