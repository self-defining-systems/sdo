//go:build integration

package builder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// All configurations must stage. Compile the strict missing-catalog case once:
// the other cases emit the same empty registry, and adoption skips acceptance.
func TestEmptyCatalogControllerBuild(t *testing.T) {
	for _, present := range []bool{false, true} {
		for _, compileOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("directory=%t/compile-only=%t", present, compileOnly), func(t *testing.T) {
				app := t.TempDir()
				diagnostics := filepath.Join(app, ".sdo", "diagnostics")
				if present {
					if err := os.MkdirAll(diagnostics, 0755); err != nil {
						t.Fatal(err)
					}
				}
				workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: "..", CompileOnly: compileOnly, Replay: "off"})
				if err != nil {
					t.Fatal(err)
				}
				defer workspace.Close()
				if !present && !compileOnly {
					if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
