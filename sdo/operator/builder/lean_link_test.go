package builder

import (
	"context"
	"debug/elf"
	"path/filepath"
	"slices"
	"testing"
)

func TestLeanPutsLinkerFlagsRightAfterTheSubcommand(t *testing.T) {
	got := lean("build", "-buildvcs=false", "-o", "out", "./cmd/x")
	want := []string{"build", "-ldflags=-s -w", "-buildvcs=false", "-o", "out", "./cmd/x"}
	if !slices.Equal(got, want) {
		t.Fatalf("lean() = %q, want %q", got, want)
	}
}

// Validation links only to prove the controller links, so it pays no DWARF
// cost; the controller that ships keeps its debug data.
func TestShippedControllerKeepsItsDebugData(t *testing.T) {
	repository := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	shipped, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: repository, DiagnosticsRoot: filepath.Join(repository, ".sdo", "diagnostics"), OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer shipped.Close()
	binary, err := shipped.ValidateAndBuild(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	file, err := elf.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if file.Section(".debug_info") == nil && file.Section(".zdebug_info") == nil {
		t.Fatal("the controller binary that ships must keep its debug data")
	}
}
