package rendered

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/sdk"
)

// Synthetic deployment observations: no live or historical seed evidence.
type appScopeImages struct{ DeploymentEvidence }

func (i appScopeImages) BaselineTree(sdk.ObjectRef) string { return i.SourceTree }
func (i appScopeImages) BaselineImage(ref sdk.ObjectRef, name string, init bool) (string, bool) {
	image, ok := i.Image(ref, name, init)
	return image.Digest, ok
}

func TestAstronomyBuildInputScope(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		legacy     bool
		gaps       int
	}{
		{"legacy-manifest", "kubernetes/repair.yaml", true, 26},
		{"manifest", "kubernetes/repair.yaml", false, 0},
		{"shipping-source", "src/shipping/src/main.rs", false, 1},
		{"shared-protobuf", "pb/demo.proto", false, 6},
		{"shipping-dockerfile", "src/shipping/Dockerfile", false, 1},
		{"shipping-ignore", "src/shipping/Dockerfile.dockerignore", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v: %s", args, err, output)
				}
				return strings.TrimSpace(string(output))
			}
			write := func(p string, data []byte) {
				t.Helper()
				p = filepath.Join(root, p)
				if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			app := filepath.Join("..", "..", "..", "apps", "astronomy_shop")
			raw, err := os.ReadFile(filepath.Join(app, "kubernetes", "astronomy-shop.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			write("kubernetes/app.yaml", raw)
			write("pb/demo.proto", []byte("synthetic protobuf"))
			var declaration struct {
				Images []struct{ Name, Context, Dockerfile, Suffix string }
			}
			data, err := os.ReadFile(filepath.Join(app, "build.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			if err := yaml.Unmarshal(data, &declaration); err != nil {
				t.Fatal(err)
			}
			inputs := map[string]ImageInputs{}
			for _, image := range declaration.Images {
				inputs[image.Name+":2.2.0"+image.Suffix] = ImageInputs{SourceImage: image.Name + ":2.2.0" + image.Suffix, ContextPath: image.Context, DockerfilePath: image.Dockerfile}
				write(image.Dockerfile, []byte("FROM scratch\n"))
				write("src/"+strings.TrimPrefix(image.Suffix, "-")+"/source.txt", []byte("synthetic source"))
			}
			overlay := filepath.Join(app, "build-inputs")
			if err := filepath.WalkDir(overlay, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				relative, err := filepath.Rel(overlay, path)
				if err != nil {
					return err
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				write(relative, data)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			write("src/shipping/src/main.rs", []byte("synthetic shipping source"))
			git("init", "-q")
			git("config", "user.name", "Test")
			git("config", "user.email", "test@example.com")
			git("add", ".")
			git("commit", "-qm", "synthetic astronomy source")
			base := git("rev-parse", "HEAD")
			proof := DeploymentEvidence{SourceTree: git("rev-parse", "HEAD^{tree}")}
			source := Source{Repository: root, ManifestPaths: []string{"kubernetes"}}
			objects, err := source.RawManifests(context.Background(), base, "astronomy-shop")
			if err != nil {
				t.Fatal(err)
			}
			workloads := 0
			for _, object := range objects {
				images, err := containerImages(object)
				if err != nil {
					t.Fatal(err)
				}
				if len(images) == 0 {
					continue
				}
				workloads++
				for _, image := range images {
					scope, ok := inputs[image.Digest]
					if !ok && !strings.HasPrefix(image.Digest, "ghcr.io/open-telemetry/demo:") {
						scope = ImageInputs{SourceImage: image.Digest}
					}
					if tc.legacy {
						scope = ImageInputs{}
					}
					digest := fmt.Sprintf("example.test/synthetic@sha256:%064x", len(proof.Images)+1)
					proof.Images = append(proof.Images, DeployedImage{Ref: objectRef(object), Name: image.Name, Init: image.Init, Applied: digest, Digest: digest, Inputs: scope})
				}
			}
			if workloads != 26 || len(proof.Images) != 37 {
				t.Fatalf("fixture workloads=%d images=%d", workloads, len(proof.Images))
			}
			if tc.path == "kubernetes/repair.yaml" {
				write(tc.path, []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: synthetic-repair}\n"))
			} else {
				write(tc.path, []byte("changed synthetic source\n"))
			}
			git("add", ".")
			git("commit", "-qm", "synthetic source repair")
			source.Images = appScopeImages{proof}
			source.Evidence = []DeploymentEvidence{proof}
			revision := git("rev-parse", "HEAD")
			lock, err := source.BuildLock(context.Background(), revision, "astronomy-shop")
			if err != nil {
				t.Fatal(err)
			}
			gaps := 0
			for _, object := range lock.Lock.Objects {
				if object.Error != "" {
					gaps++
					if object.File != "" || len(object.Images) != 0 {
						t.Fatal("gap retained authority")
					}
					if tc.gaps == 1 && object.Ref.Name != "shipping" {
						t.Errorf("unexpected gap: %s", object.Ref.Name)
					}
				}
			}
			t.Logf("workloads=%d named_images=%d gaps=%d", workloads, len(proof.Images), gaps)
			if gaps != tc.gaps {
				t.Fatalf("gaps=%d want=%d", gaps, tc.gaps)
			}
		})
	}
}

func TestSocialBuildInputScope(t *testing.T) {
	for _, tc := range []struct {
		path    string
		changed int
	}{
		{"kubernetes/repair.yaml", 0},
		{"src/synthetic.cpp", 1},
		{"docker/media-frontend/nginx.conf", 1},
	} {
		t.Run(tc.path, func(t *testing.T) {
			root := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v: %s", args, err, output)
				}
				return strings.TrimSpace(string(output))
			}
			write := func(p string, data []byte) {
				t.Helper()
				p = filepath.Join(root, p)
				if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			app := filepath.Join("..", "..", "..", "apps", "social_network")
			data, err := os.ReadFile(filepath.Join(app, "build.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var declaration struct {
				Images []struct{ Name, Context, Dockerfile string }
			}
			if err := yaml.Unmarshal(data, &declaration); err != nil {
				t.Fatal(err)
			}
			for _, image := range declaration.Images {
				write(image.Dockerfile, []byte("FROM scratch\n"))
				ignore, err := os.ReadFile(filepath.Join(app, "build-inputs", image.Dockerfile+".dockerignore"))
				if err != nil {
					t.Fatal(err)
				}
				write(image.Dockerfile+".dockerignore", ignore)
			}
			write("src/synthetic.cpp", []byte("synthetic service"))
			write("docker/media-frontend/nginx.conf", []byte("synthetic frontend"))
			git("init", "-q")
			git("config", "user.name", "Test")
			git("config", "user.email", "test@example.com")
			git("add", ".")
			git("commit", "-qm", "synthetic social source")
			base := git("rev-parse", "HEAD")
			write(tc.path, []byte("synthetic repair"))
			git("add", ".")
			git("commit", "-qm", "source repair")
			revision := git("rev-parse", "HEAD")
			changed := 0
			for _, image := range declaration.Images {
				same, err := (Source{Repository: root}).SameImageInputs(context.Background(), base, revision, ImageInputs{SourceImage: image.Name + ":test", ContextPath: image.Context, DockerfilePath: image.Dockerfile})
				if err != nil {
					t.Fatal(err)
				}
				if !same {
					changed++
				}
			}
			if changed != tc.changed {
				t.Fatalf("changed builds=%d want=%d", changed, tc.changed)
			}
		})
	}
}
