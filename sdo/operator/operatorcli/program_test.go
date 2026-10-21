package operatorcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProgramScaffoldWritesProgramLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "scratch")
	config, err := ParseProgramScaffold([]string{"program", "scaffold", "--fault-class", "targetport", "--param", "service=Service", "--mitigate", "--out", dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := RunProgramScaffold(config); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"README.md", "diagnose.go", "mitigate.go", "diagnose_test.go"} {
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			t.Errorf("missing %s: %v", path, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "diagnose.go"))
	if err != nil || !strings.Contains(string(data), `ID: "targetport"`) || !strings.Contains(string(data), `Name: "service", Kind: "Service"`) {
		t.Fatalf("diagnoser skeleton: %s, %v", data, err)
	}
	if err := RunProgramScaffold(config); err == nil {
		t.Fatal("scaffold overwrote existing files")
	}
}
