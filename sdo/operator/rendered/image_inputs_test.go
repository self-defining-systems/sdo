package rendered

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestImageInputsDockerIgnoreAndContainment(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, ignore string
		same                     bool
	}{
		{"ignored-manifest", "app/kubernetes/new.yaml", "changed", "kubernetes\n", true},
		{"negated-ignore", "app/kubernetes/keep.yaml", "changed", "kubernetes\n!kubernetes/keep.yaml\n", false},
		{"globstar", "app/generated/deep/file", "changed", "generated/**\n", true},
		{"dockerfile-always-input", "docker/app.Dockerfile", "FROM scratch\n# edit", "**\n", false},
		{"ignore-edit", "app/.dockerignore", "**\n", "docs\n", false},
		{"specific-ignore-edit", "docker/app.Dockerfile.dockerignore", "**\n", "docs\n", false},
		{"source-deletion", "app/main.go", "", "", false},
		{"literal-git-path", "app/space\nand[glob].go", "changed", "", false},
		{"outside-context", "app-other/main.go", "changed", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				data, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
				if err != nil {
					t.Fatalf("git: %v %s", err, data)
				}
				return strings.TrimSpace(string(data))
			}
			write := func(p, body string) {
				t.Helper()
				p = filepath.Join(root, p)
				if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			git("init", "-q")
			git("config", "user.name", "Test")
			git("config", "user.email", "test@example.com")
			write("app/main.go", "original")
			write("docker/app.Dockerfile", "FROM scratch\n")
			write("app/.dockerignore", tc.ignore)
			git("add", ".")
			git("commit", "-qm", "base")
			base := git("rev-parse", "HEAD")
			if tc.body == "" {
				if err := os.Remove(filepath.Join(root, tc.path)); err != nil {
					t.Fatal(err)
				}
			} else {
				write(tc.path, tc.body)
			}
			git("add", ".")
			git("commit", "-qm", "edit")
			next := git("rev-parse", "HEAD")
			same, err := (Source{Repository: root}).SameImageInputs(context.Background(), base, next, ImageInputs{SourceImage: "app:latest", ContextPath: "app", DockerfilePath: "docker/app.Dockerfile"})
			if err != nil || same != tc.same {
				t.Fatalf("same=%t want=%t err=%v", same, tc.same, err)
			}
		})
	}
}

func TestImageInputsRejectUnsafeDeclarations(t *testing.T) {
	for _, inputs := range []ImageInputs{
		{ContextPath: "src", DockerfilePath: "Dockerfile"},
		{SourceImage: "app", ContextPath: "../src", DockerfilePath: "Dockerfile"},
		{SourceImage: "app", ContextPath: "src", DockerfilePath: "/Dockerfile"},
		{SourceImage: "app", ContextPath: "src", DockerfilePath: "."},
		{SourceImage: "app", ContextPath: "src/../other", DockerfilePath: "Dockerfile"},
		{SourceImage: "app", ContextPath: "src"},
	} {
		if err := inputs.Validate(); err == nil {
			t.Fatalf("accepted unsafe declaration: %+v", inputs)
		}
	}
}
