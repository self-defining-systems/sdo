package program

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOnlyTestsMayImportApplicationPlaybook(t *testing.T) {
	for _, tc := range []struct {
		dir, file string
		allowed   bool
	}{
		{"detectors", "detector.go", false}, {"lib", "matrix_test.go", true},
		{"playbooks", "diagnose.go", false}, {"lib", "helper.go", false},
	} {
		t.Run(tc.dir+"/"+tc.file, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), tc.dir)
			if err := os.MkdirAll(root, 0755); err != nil {
				t.Fatal(err)
			}
			body := "package example\nimport _ \"app/playbooks/repair-port\"\n"
			if err := os.WriteFile(filepath.Join(root, tc.file), []byte(body), 0644); err != nil {
				t.Fatal(err)
			}
			err := checkProgramImports(root, "app")
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%t: %v", tc.allowed, err)
			}
		})
	}
}

func TestExternalSharedModuleStillRefused(t *testing.T) {
	root := filepath.Join(t.TempDir(), "detectors")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "detector.go"), []byte("package example\nimport _ \"external-shared/evidence\"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := checkProgramImports(root, "app"); err == nil {
		t.Fatal("external module accepted")
	}
}
