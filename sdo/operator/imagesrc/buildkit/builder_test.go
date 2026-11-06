package buildkit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
)

const synthDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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
	return root, checkout
}

func testBackend(root string, client *fake.Clientset, timeout time.Duration) Builder {
	return Builder{
		Client:     client, Namespace: "sdo-control", RepositoryPVC: "repo", CheckoutRoot: root,
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
	_, err := testBackend(root, fake.NewSimpleClientset(), 30*time.Millisecond).buildImage(context.Background(), checkout, testBuild(checkout))
	var timeout *imagesrc.TimeoutError
	if !errors.As(err, &timeout) || timeout.Budget {
		t.Fatalf("a build that outlives its deadline must report imagesrc.TimeoutError, got %v", err)
	}
}

func TestBuildJobDeadlineFailureIsATimeout(t *testing.T) {
	root, checkout := proposalCheckout(t)
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-image-build-t"}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline"}}}}, nil
	})
	_, err := testBackend(root, client, time.Minute).buildImage(context.Background(), checkout, testBuild(checkout))
	var timeout *imagesrc.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("Kubernetes DeadlineExceeded must report imagesrc.TimeoutError, got %v", err)
	}
}

// A timed-out build is deleted, so it stops consuming the node that the
// retry needs.
func TestTimedOutBuildJobIsDeleted(t *testing.T) {
	root, checkout := proposalCheckout(t)
	client := fake.NewSimpleClientset()
	if _, err := testBackend(root, client, 30*time.Millisecond).buildImage(context.Background(), checkout, testBuild(checkout)); err == nil {
		t.Fatal("expected a timeout")
	}
	jobs, err := client.BatchV1().Jobs("sdo-control").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("abandoned build Job must be deleted: %v, err %v", jobs.Items, err)
	}
}

func jobEnvironment(t *testing.T, backend Builder, image string) (map[string]string, string) {
	t.Helper()
	job := backend.job("j", "proposal", ".", "svc/Dockerfile", image, time.Minute)
	container := job.Spec.Template.Spec.Containers[0]
	environment := map[string]string{}
	for _, variable := range container.Env {
		environment[variable.Name] = variable.Value
	}
	return environment, container.Args[0]
}

// Layers survive across build Jobs through the app's own registry repository,
// so a retry or the next proposal does not recompile from scratch.
func TestBuildJobSharesLayerCacheThroughTheImageRepository(t *testing.T) {
	backend := testBackend(t.TempDir(), fake.NewSimpleClientset(), time.Minute)
	environment, script := jobEnvironment(t, backend, "reg.example/app/api:1")
	if environment["SDO_CACHE_REF"] != "reg.example/app/api:sdo-buildcache" {
		t.Fatalf("cache ref = %q", environment["SDO_CACHE_REF"])
	}
	for _, want := range []string{`--import-cache "type=registry,ref=$SDO_CACHE_REF"`, `--export-cache "type=registry,ref=$SDO_CACHE_REF,mode=max,ignore-error=true"`} {
		if !strings.Contains(script, want) {
			t.Fatalf("build script lacks %s:\n%s", want, script)
		}
	}
	if strings.Contains(environment["BUILDKITD_FLAGS"], "--config") {
		t.Fatal("a TLS registry needs no buildkitd registry config")
	}
}

func TestBuildJobCacheReachesAnInsecureLaneRegistry(t *testing.T) {
	backend := testBackend(t.TempDir(), fake.NewSimpleClientset(), time.Minute)
	backend.RegistryInsecure = true
	environment, script := jobEnvironment(t, backend, "reg.example/app/api:1")
	if environment["SDO_REGISTRY_HOST"] != "reg.example" || !strings.Contains(environment["BUILDKITD_FLAGS"], "--config /tmp/buildkitd.toml") {
		t.Fatalf("insecure registry must be configured for the cache client too: %v", environment)
	}
	if !strings.Contains(script, "http = true") {
		t.Fatalf("script must write the registry config:\n%s", script)
	}
}

const buildMetadata = buildMetadataPrefix + `{"containerimage.digest":"` + synthDigest + `"}` + "\n"

func completedBuild(root string) (Builder, *fake.Clientset) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-image-build-t"}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}, nil
	})
	b := testBackend(root, client, time.Minute)
	b.ReadJobLogs = func(context.Context, string, string) ([]byte, error) { return []byte(buildMetadata), nil }
	return b, client
}

// The build input is an immutable materialized effective tree, so a finished
// Job is accepted without re-reading the directory.
func TestBuildOfAMaterializedTreeNeedsNoTreeGuard(t *testing.T) {
	root, checkout := proposalCheckout(t)
	b, _ := completedBuild(root)
	ref, err := b.buildImage(context.Background(), checkout, testBuild(checkout))
	if err != nil || ref != "reg.example/app/api@"+synthDigest {
		t.Fatalf("ref %q err %v", ref, err)
	}
}
