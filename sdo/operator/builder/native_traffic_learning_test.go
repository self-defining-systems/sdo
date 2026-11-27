package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratorImportErrorNamesAllowedAlternatives(t *testing.T) {
	for _, name := range []string{"math/rand", "net/http"} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "generators")
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte("package generators\nimport \""+name+"\"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			err := validateGeneratorSources(base, "app.dev/diagnostics")
			if err == nil || !strings.Contains(err.Error(), "math/rand/v2") || !strings.Contains(err.Error(), "sdo.dev/operator/sdk/traffic") {
				t.Fatalf("import diagnostic: %v", err)
			}
		})
	}
}
