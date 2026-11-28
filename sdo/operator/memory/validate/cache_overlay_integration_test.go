//go:build integration

package validate

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"sdo.dev/operator/internal/testutil/toolio"
)

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
			t.Setenv("GOCACHE", t.TempDir())
			t.Setenv("GOCACHEPROG", helper)
			t.Setenv("SDO_GO_CACHE_SEED", seed)
			worker := gateCompileDiagnostics(t, row, root)
			if !reflect.DeepEqual(worker, broker) {
				t.Fatalf("through the overlay:\n%v\ndirect:\n%v", worker, broker)
			}
		})
	}
}
