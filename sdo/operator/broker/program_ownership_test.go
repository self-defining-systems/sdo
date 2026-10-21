package broker

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/validate"
	"strings"
	"testing"
)

func TestIgnoredSharedLibraryReachesValidatorTree(t *testing.T) {
	root := repository(t)
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("lib/\ncorpus/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".gitignore")
	git(t, root, "commit", "-qm", "ignore ordinary libraries")
	path := filepath.Join(root, ".sdo", "lib", "ports", "ports.go")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package ports\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	casePath := filepath.Join(root, ".sdo", "corpus", "case-1", "labels.json")
	if err := os.MkdirAll(filepath.Dir(casePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(casePath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := git(t, root, "rev-parse", "HEAD")
	paths, err := validate.ChangedPaths(context.Background(), root, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != ".sdo/corpus/case-1/labels.json" || paths[1] != ".sdo/lib/ports/ports.go" {
		t.Fatalf("library missing from proposal diff: %v", paths)
	}
	validator := JobValidator(validationJobFunc(func(_ context.Context, request ValidationJobRequest) (ValidationJobReceipt, error) {
		files := git(t, root, "ls-tree", "-r", "--name-only", request.CandidateTree)
		if !strings.Contains(files, ".sdo/lib/ports/ports.go") || !strings.Contains(files, ".sdo/corpus/case-1/labels.json") {
			t.Fatalf("library missing from validator tree: %s", files)
		}
		return ValidationJobReceipt{CandidateTree: request.CandidateTree, Accepted: true, DetectorCompiled: true, SourceValidated: true, ReadOnlyMountObserved: true, NoPublishingCredentialsObserved: true, EgressIsolationCanaryPassed: true}, nil
	}))
	if err := validator(context.Background(), root, paths); err != nil {
		t.Fatal(err)
	}
}
