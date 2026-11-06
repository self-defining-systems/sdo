package buildkit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

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

func imageBuild(image, contextPath, dockerfile string) *contracts.ImageBuild {
	return &contracts.ImageBuild{Image: "reg.example/" + image, ContextPath: contextPath, DockerfilePath: dockerfile}
}

func TestPlanAdmissionRejectsMissingOrMisplacedDockerfile(t *testing.T) {
	checkout := sourceCheckout(t)
	for name, dockerfile := range map[string]string{"missing": "services/api/Dockerfile.prod", "directory": "services/api"} {
		t.Run(name, func(t *testing.T) {
			_, err := PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", "services/api", dockerfile)})
			if err == nil || !strings.Contains(err.Error(), "dockerfile_path") || !strings.Contains(err.Error(), `"services/api/Dockerfile"`) {
				t.Fatalf("rejection must name dockerfile_path and list the real Dockerfiles, got %v", err)
			}
		})
	}
}

func TestPlanAdmissionTrimsOnlyUnambiguousWhitespace(t *testing.T) {
	checkout := sourceCheckout(t)
	for _, raw := range []string{" services/api", "services/api\t"} {
		if _, err := PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", raw, "services/api/Dockerfile")}); err != nil {
			t.Fatalf("context %q: %v", raw, err)
		}
	}
	planned, err := PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", " .", "services/api/Dockerfile")})
	if err != nil || planned[0].ContextRel != "." {
		t.Fatalf("leading space before . must normalize to the checkout root: %v %v", planned, err)
	}
	// A literal path that exists wins over its trimmed form.
	if err := os.MkdirAll(filepath.Join(checkout, " odd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(checkout, "odd"), 0o755); err != nil {
		t.Fatal(err)
	}
	planned, err = PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", " odd", "services/api/Dockerfile")})
	if err != nil || planned[0].ContextRel != " odd" {
		t.Fatalf("an existing literal path must not be rewritten: %v %v", planned, err)
	}
	// Trimming never rescues a path that does not exist either way.
	if _, err := PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", " nowhere", "services/api/Dockerfile")}); err == nil || !strings.Contains(err.Error(), "whitespace") {
		t.Fatalf("whitespace in a still-missing path must be called out: %v", err)
	}
}

func TestPlanAdmissionRejectsEscapesAndFileContexts(t *testing.T) {
	checkout := sourceCheckout(t)
	for _, contextPath := range []string{"../outside", "/etc", "services/api/Dockerfile"} {
		if _, err := PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", contextPath, "services/api/Dockerfile")}); err == nil {
			t.Fatalf("context %q must be rejected", contextPath)
		}
	}
}

// dockerfile_path is checkout-relative; the context-relative habit is accepted
// only when it is unambiguous, and the rejection names the right form.
func TestDockerfilePathConvention(t *testing.T) {
	checkout := sourceCheckout(t)
	planned, err := PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", "services/api", "Dockerfile")})
	if err != nil || planned[0].DockerfileRel != "services/api/Dockerfile" {
		t.Fatalf("context-relative Dockerfile must resolve below the context: %v %v", planned, err)
	}
	// Both readings exist: the checkout-root one wins.
	if err := os.WriteFile(filepath.Join(checkout, "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	planned, err = PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", "services/api", "Dockerfile")})
	if err != nil || planned[0].DockerfileRel != "Dockerfile" {
		t.Fatalf("an existing checkout-relative path must not be reinterpreted: %v %v", planned, err)
	}
	_, err = PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", "services/api", "Dockerfile.prod")})
	if err == nil || !strings.Contains(err.Error(), "relative to the checkout root, not to the build context") {
		t.Fatalf("rejection must name the convention: %v", err)
	}
}
