package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type recordingBuilds struct {
	fakeRawBackend
	contexts []string
	build    func(context.Context) (string, error)
}

func (b *recordingBuilds) BuildImage(ctx context.Context, _ string, build *contracts.ImageBuild) (string, error) {
	b.contexts = append(b.contexts, build.ContextPath)
	if b.build != nil {
		return b.build(ctx)
	}
	return "reg.example/" + filepath.Base(build.Image) + "@" + synthDigest, nil
}

func sourceCheckout(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, path := range []string{"services/api/Dockerfile", "services/web/Dockerfile", "docker/openresty/Dockerfile", "manifests/app.yaml"} {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("FROM scratch\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func buildPlan(builds ...*contracts.ImageBuild) *contracts.RawManifestStrategy {
	plan := rawPlan()
	plan.ImageBuilds = builds
	return plan
}

func imageBuild(image, contextPath, dockerfile string) *contracts.ImageBuild {
	return &contracts.ImageBuild{Image: "reg.example/" + image, ContextPath: contextPath, DockerfilePath: dockerfile}
}

// A wrong path anywhere in the plan is reported before the first build starts,
// with the directories the deployer can use instead.
func TestPlanAdmissionRejectsUnknownBuildContextBeforeAnyBuild(t *testing.T) {
	checkout := sourceCheckout(t)
	backend := &recordingBuilds{}
	_, err := (RawStrategy{Namespace: "demo", Backend: backend}).ApplyAndVerify(context.Background(), checkout, "tree", buildPlan(
		imageBuild("api", "services/api", "services/api/Dockerfile"),
		imageBuild("proxy", "checkout/xenial", "docker/openresty/Dockerfile"),
	))
	if err == nil {
		t.Fatal("a build context that is not in the checkout must be rejected")
	}
	if len(backend.contexts) != 0 {
		t.Fatalf("no build may start when any path is wrong; builds: %v", backend.contexts)
	}
	for _, want := range []string{"checkout/xenial", "no build was started", "relative to the checkout root", `"services/api"`, `"docker/openresty"`, "image_builds[1]"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("rejection %q must mention %q", err, want)
		}
	}
}

func TestBuildBudgetTimeoutIsNotAPlanError(t *testing.T) {
	checkout := sourceCheckout(t)
	backend := &recordingBuilds{build: func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}}
	_, err := (RawStrategy{Namespace: "demo", Backend: backend, BuildBudget: 20 * time.Millisecond}).ApplyAndVerify(context.Background(), checkout, "tree",
		buildPlan(imageBuild("api", "services/api", "services/api/Dockerfile")))
	var timeout *BuildTimeoutError
	if !errors.As(err, &timeout) || !timeout.Budget {
		t.Fatalf("budget expiry must be a BuildTimeoutError, got %v", err)
	}
	if !strings.Contains(err.Error(), "not a plan error") || !strings.Contains(err.Error(), "THE PLAN WAS FINE") || !strings.Contains(err.Error(), "Dockerfile") {
		t.Fatalf("timeout feedback must tell the deployer to keep the plan: %v", err)
	}
}

func TestCallerCancellationIsNotATimeout(t *testing.T) {
	checkout := sourceCheckout(t)
	ctx, cancel := context.WithCancel(context.Background())
	backend := &recordingBuilds{build: func(ctx context.Context) (string, error) { cancel(); return "", ctx.Err() }}
	_, err := (RawStrategy{Namespace: "demo", Backend: backend, BuildBudget: time.Hour}).ApplyAndVerify(ctx, checkout, "tree",
		buildPlan(imageBuild("api", "services/api", "services/api/Dockerfile")))
	var timeout *BuildTimeoutError
	if err == nil || errors.As(err, &timeout) {
		t.Fatalf("cancellation must stay a plain error, got %v", err)
	}
}
