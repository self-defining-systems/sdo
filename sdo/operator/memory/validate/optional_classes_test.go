package validate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryMayOmitDetectorAndPlaybookClasses(t *testing.T) {
	for _, name := range []string{"detector-none", "playbook-none"} {
		t.Run(name, func(t *testing.T) {
			root := goodTree(t)
			applyCase(t, root, name)
			if ds := diagnose(t, Request{Root: root, Layers: Structural | Semantic}); len(ds) != 0 {
				t.Fatalf("valid memory with omitted classes rejected: %v", ds)
			}
		})
	}
}

func TestMemoryMayKeepDetectorSuggestionsWithoutPlaybookClass(t *testing.T) {
	root := goodTree(t)
	if err := os.RemoveAll(filepath.Join(root, ".sdo/playbooks")); err != nil {
		t.Fatal(err)
	}
	if ds := diagnose(t, Request{Root: root, Layers: Structural | Semantic}); len(ds) != 0 {
		t.Fatalf("optional class rejected: %v", ds)
	}
}
