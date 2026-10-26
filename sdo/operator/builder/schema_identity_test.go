package builder

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSchemaIdentityUsesNativeGoBuilderSources(t *testing.T) {
	root := t.TempDir()
	for path, content := range map[string]string{
		"memory/observation/scope.go": "package observation\n",
		"memory/validate/validate.go": "package validate\n",
		"sdk/detector.go":             "package sdk\n",
		"builder/native.go":           "package builder\n",
	} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	first, err := SchemaIdentity(root)
	if err != nil {
		t.Fatalf("native Go schema identity requires no Python builder: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "builder", "native.go"), []byte("package builder\n// changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := SchemaIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("native builder schema change did not alter identity")
	}
}
