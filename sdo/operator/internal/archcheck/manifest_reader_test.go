package archcheck

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// manifestReaders are the production files outside the render packages that
// import a YAML decoder and read Kubernetes object fields (kind, metadata),
// with why none of them reads the deployment source tree. Desired state (the
// objects, inventory and workload names a source declares) comes only from the
// renderer catalog through rendered.Desired, so Helm and Kustomize sources are
// seen exactly as raw ones are. Adding a file here is a design decision: say
// what it reads instead of the source tree.
var manifestReaders = map[string]string{
	"runtime/sim/apiserver.go":         "decodes request bodies of the simulated API server",
	"runtime/replay/cli.go":            "loads captured corpus cases (accepted.yaml), not the source tree",
	"memory/observation/scope.go":      "parses the observation scope of arch.md",
	"memory/layout/manifest.go":        "parses detector manifests",
	"memory/layout/sources.go":         "parses sources.yaml",
	"memory/layout/migrate.go":         "migrates operational-memory files",
	"memory/appmap/map.go":             "parses the application map of arch.md",
	"memory/validate/secret_object.go": "scans changed memory data files for Secret data",
}

var (
	yamlImport = regexp.MustCompile(`"(gopkg\.in/yaml[^"]*|sigs\.k8s\.io/yaml[^"]*|k8s\.io/apimachinery/pkg/util/yaml)"`)
	objectRead = regexp.MustCompile(`"kind"|"metadata"|GetKind\(\)|yaml:"kind"`)
)

// TestOnlyTheRenderPackagesParseManifests fails when production code outside
// render/ and rendered/ starts decoding manifest YAML itself. The earlier
// readers (the memory inventory, the builder's Service-name scan, a benchmark
// helper) each missed Helm sources; the render catalog is the one reader.
//
//ci:whole-tree
func TestOnlyTheRenderPackagesParseManifests(t *testing.T) {
	root := "../.."
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(root, path)
		relative = filepath.ToSlash(relative)
		if entry.IsDir() {
			switch {
			case entry.Name() == "testdata", entry.Name() == "gen", relative == "internal/archcheck", relative == "render", relative == "rendered":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(relative, ".go") || strings.HasSuffix(relative, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if yamlImport.Match(data) && objectRead.Match(data) {
			found[relative] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var unexpected, stale []string
	for file := range found {
		if _, ok := manifestReaders[file]; !ok {
			unexpected = append(unexpected, file)
		}
	}
	for file := range manifestReaders {
		if !found[file] {
			stale = append(stale, file)
		}
	}
	sort.Strings(unexpected)
	sort.Strings(stale)
	if len(unexpected) > 0 {
		t.Errorf("production code outside render/ decodes YAML and reads object kind/metadata: %v\nRoute desired-state reads through rendered.Desired (the renderer catalog); if the file reads something else, list it in manifestReaders with the reason.", unexpected)
	}
	if len(stale) > 0 {
		t.Errorf("manifestReaders lists files that no longer match; delete the entries: %v", stale)
	}
}
