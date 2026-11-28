package rendered_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/rendered"
)

// Memory never feeds a build: admission refuses it before any build starts, so the deployer never learns it
// only from a failed operability check. A Dockerfile committed with the source builds.
func TestPlanAdmissionRefusesMemory(t *testing.T) {
	for name, build := range map[string]*contracts.ImageBuild{
		"dockerfile in memory": imageBuild("api", "services/api", ".sdo/goal/Dockerfile"),
		"context in memory":    imageBuild("api", ".sdo/deployment", ".sdo/deployment/Dockerfile"),
	} {
		t.Run(name, func(t *testing.T) {
			checkout := sourceCheckout(t)
			writeFile(t, checkout, ".sdo/goal/Dockerfile", "FROM scratch\n")
			writeFile(t, checkout, ".sdo/deployment/Dockerfile", "FROM scratch\n")
			_, err := buildkit.PlanBuilds(checkout, []*contracts.ImageBuild{build})
			if err == nil || !strings.Contains(err.Error(), "operational memory") {
				t.Fatalf("admission must refuse memory, got %v", err)
			}
			inputs := rendered.ImageInputs{SourceImage: "reg.example/api", ContextPath: build.ContextPath, DockerfilePath: build.DockerfilePath}
			if inputs.Validate() == nil {
				t.Fatalf("recorded inputs %+v must be rejected too", inputs)
			}
		})
	}
	checkout := sourceCheckout(t)
	writeFile(t, checkout, "deploy/sdo/build/api/Dockerfile", "FROM scratch\nCOPY . /app\n")
	planned, err := buildkit.PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", "services/api", "deploy/sdo/build/api/Dockerfile")})
	if err != nil {
		t.Fatalf("a committed Dockerfile is source: %v", err)
	}
	if err := (rendered.ImageInputs{SourceImage: "reg.example/api", ContextPath: planned[0].ContextRel, DockerfilePath: planned[0].DockerfileRel}).Validate(); err != nil {
		t.Fatalf("recorded inputs rejected: %v", err)
	}
}

func sourceCheckout(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "services/api/main.go", "package main\n")
	return dir
}

func imageBuild(image, contextPath, dockerfile string) *contracts.ImageBuild {
	return &contracts.ImageBuild{Image: "reg.example/" + image, ContextPath: contextPath, DockerfilePath: dockerfile}
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
