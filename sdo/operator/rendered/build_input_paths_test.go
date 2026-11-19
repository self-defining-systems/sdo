package rendered_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/rendered"
)

// The deployer's instructions put an authored Dockerfile under .sdo/deployment/build/ with the upstream source as
// context. Every such example path, in the skill and in the prompt, must pass build admission and record inputs that
// the operability report (O2.inputs, rendered.ImageInputs.Validate) accepts: one rule, two readers.
func TestTheDeployerInstructionsDockerfilePathsBuildAndAttribute(t *testing.T) {
	documents := []string{
		"../../skills/sdo-serving/SKILL.md",
		"../../skills/sdo-serving/references/catalog.md",
		"../../agents/source_deployment/prompts/source_deployment.md",
	}
	example := regexp.MustCompile(regexp.QuoteMeta(imagesrc.AuthoredDockerfileDir+"/") + `[A-Za-z0-9._/-]*Dockerfile`)
	for _, document := range documents {
		text, err := os.ReadFile(document)
		if err != nil {
			t.Fatal(err)
		}
		paths := example.FindAllString(string(text), -1)
		if len(paths) == 0 {
			t.Fatalf("%s names no authored Dockerfile under %s/; this test reads its examples", document, imagesrc.AuthoredDockerfileDir)
		}
		for _, dockerfile := range paths {
			checkout := sourceCheckout(t)
			writeFile(t, checkout, dockerfile, "FROM scratch\nCOPY . /app\n")
			planned, err := buildkit.PlanBuilds(checkout, []*contracts.ImageBuild{imageBuild("api", "services/api", dockerfile)})
			if err != nil {
				t.Fatalf("%s: %s: build admission: %v", document, dockerfile, err)
			}
			inputs := rendered.ImageInputs{SourceImage: "reg.example/api", ContextPath: planned[0].ContextRel, DockerfilePath: planned[0].DockerfileRel}
			if err := inputs.Validate(); err != nil {
				t.Fatalf("%s: %s: recorded inputs rejected: %v", document, dockerfile, err)
			}
		}
	}
}

// Other memory never feeds a build: admission refuses it before any build starts, naming where an authored
// Dockerfile goes, so the deployer never learns it only from a failed operability check.
func TestPlanAdmissionRefusesMemoryOutsideTheBuildDirectory(t *testing.T) {
	for name, build := range map[string]*contracts.ImageBuild{
		"dockerfile in other memory": imageBuild("api", "services/api", ".sdo/goal/Dockerfile"),
		"context in memory":          imageBuild("api", imagesrc.AuthoredDockerfileDir+"/api", imagesrc.AuthoredDockerfileDir+"/api/Dockerfile"),
	} {
		t.Run(name, func(t *testing.T) {
			checkout := sourceCheckout(t)
			writeFile(t, checkout, ".sdo/goal/Dockerfile", "FROM scratch\n")
			writeFile(t, checkout, imagesrc.AuthoredDockerfileDir+"/api/Dockerfile", "FROM scratch\n")
			_, err := buildkit.PlanBuilds(checkout, []*contracts.ImageBuild{build})
			if err == nil || !strings.Contains(err.Error(), imagesrc.AuthoredDockerfileDir+"/") {
				t.Fatalf("admission must refuse and name %s/, got %v", imagesrc.AuthoredDockerfileDir, err)
			}
			inputs := rendered.ImageInputs{SourceImage: "reg.example/api", ContextPath: build.ContextPath, DockerfilePath: build.DockerfilePath}
			if inputs.Validate() == nil {
				t.Fatalf("recorded inputs %+v must be rejected too", inputs)
			}
		})
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
