//go:build integration

package lifecycle_test

//ci:scoped renders the apps/toy helm chart as a fixture

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/operatorcli/catalog/renderers"
)

// chartRepository commits the toy application's Helm chart under chart/ in a
// fresh repository. The chart's templates are not YAML, so only a renderer
// can tell what the source declares.
func chartRepository(t *testing.T) (repository, commit string) {
	t.Helper()
	repository = t.TempDir()
	root := filepath.Join("..", "..", "..", "apps", "toy", "variants", "helm", "chart")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		relative, _ := filepath.Rel(root, path)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		target := filepath.Join(repository, "chart", relative)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", repository, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...)
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("add", ".")
	git("commit", "-q", "-m", "chart")
	return repository, git("rev-parse", "HEAD")
}

// A Helm chart declares its workloads only after rendering. The inventory the
// architecture and the judge see must come from the renderer, so the detection
// designer can name them in the application map.
func TestHelmSourceInventoryComesFromTheRenderer(t *testing.T) {
	repository, commit := chartRepository(t)
	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "helm", Config: config}

	facts, err := lifecycle.RenderedSourceFacts(context.Background(), renderers.Catalog(), repository, commit, "sdo-toy", source, nil)
	if err != nil {
		t.Fatal(err)
	}
	inventory := appmap.ParseInventory(memory.ArchitectureInventory(facts))
	for _, workload := range []string{"Deployment/frontend", "Deployment/backend", "Service/frontend"} {
		if !inventory.Workloads[workload] {
			t.Errorf("architecture inventory lacks %s: %v", workload, inventory.Workloads)
		}
	}
	if !inventory.Services["frontend"] {
		t.Errorf("application-map services lack frontend: %v", inventory.Services)
	}
}

func TestSourceInventoryOfAnUnrenderableSourceIsAnErrorNotAnEmptyInventory(t *testing.T) {
	repository, commit := chartRepository(t)
	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "helm", Config: config}
	if _, err := lifecycle.RenderedSourceFacts(context.Background(), renderers.Catalog(), repository, commit, "sdo-toy", source, nil); err == nil {
		t.Fatal("a source that does not render produced an inventory")
	}
}
