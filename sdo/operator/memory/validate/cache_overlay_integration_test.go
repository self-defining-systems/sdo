//go:build integration

package validate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil/toolio"
)

// maxPrivateEntries bounds what one overlay check may add to its private cache over a seed that already holds the
// tree's dependencies.
const maxPrivateEntries = 500

// cacheEntries counts the Go disk-cache action entries under root.
func cacheEntries(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, "-a") {
			count++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

// A worker's memory check reads the controller's Go build cache through the read-through GOCACHEPROG helper
// (cmd/sdo-go-cache); the broker's check reads its own cache directly. Where the compiled bytes come from must never
// change a verdict: every gate compile row, and a candidate every stage accepts, gives the same diagnostics both ways.
func TestCheckThroughTheCacheOverlayGivesTheBrokersVerdicts(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles every gate row twice through the real toolchain; nightly CI runs it without -short")
	}
	seed := os.Getenv("GOCACHE")
	if seed == "" {
		directory, err := os.UserCacheDir()
		if err != nil {
			t.Fatal(err)
		}
		seed = filepath.Join(directory, "go-build")
	}
	helper := filepath.Join(t.TempDir(), "sdo-go-cache")
	if output, err := toolio.BuildPackage(helper, "sdo.dev/operator/cmd/sdo-go-cache"); err != nil {
		t.Fatalf("build the cache helper: %v: %s", err, output)
	}
	rows := append([]gateCompileRow{{name: "a candidate every stage accepts", real: func(*testing.T, string) {}}}, gateCompileRows...)
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			root := compileTree(t)
			row.real(t, root)
			t.Setenv("GOCACHE", seed)
			t.Setenv("GOCACHEPROG", "")
			broker := gateCompileDiagnostics(t, row, root)
			private := t.TempDir()
			t.Setenv("GOCACHE", private)
			t.Setenv("GOCACHEPROG", helper)
			t.Setenv("SDO_GO_CACHE_SEED", seed)
			worker := gateCompileDiagnostics(t, row, root)
			if !reflect.DeepEqual(worker, broker) {
				t.Fatalf("through the overlay:\n%v\ndirect:\n%v", worker, broker)
			}
			// The check really read the seed: a cold check of this tree writes thousands of entries (every
			// dependency's compile and vet facts); through a warm seed only what differs is written.
			if written := cacheEntries(t, private); written > maxPrivateEntries {
				t.Fatalf("the overlay check wrote %d entries to its private cache (at most %d): it did not read the seed", written, maxPrivateEntries)
			}
		})
	}
}
