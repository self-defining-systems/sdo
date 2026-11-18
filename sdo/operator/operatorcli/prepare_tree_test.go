package operatorcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/operatorcli/internal/processio"
)

// A pool lane's memory was prepared by an earlier build. Preparing it as an
// install of the new build would regenerates the earlier Go-generated
// detectors to the new template, commits them, and a second pass changes
// nothing.
func TestPrepareInstallTreeRegeneratesAnEarlierBuildsDetectors(t *testing.T) {
	repo := parityRepo(t)
	launch, err := ParseInstallExistingLaunch(existingLaunchArgs("--repository", repo))
	if err != nil {
		t.Fatal(err)
	}
	first, err := PrepareInstallTree(context.Background(), launch)
	if err != nil || first == "" {
		t.Fatalf("first preparation = %q, %v", first, err)
	}
	var stale string
	_ = filepath.WalkDir(filepath.Join(repo, ".sdo", "diagnostics", "detectors"), func(path string, d os.DirEntry, _ error) error {
		if stale == "" && d != nil && d.Name() == "detector.go" {
			if data, err := os.ReadFile(path); err == nil && strings.HasPrefix(string(data), "// sdo:generated sha256=") {
				stale = path
			}
		}
		return nil
	})
	if stale == "" {
		t.Fatal("no Go-generated detector.go in the prepared tree")
	}
	current, err := os.ReadFile(stale)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("package detector\n\n// an earlier template\n")
	sum := sha256.Sum256(body)
	old := append([]byte("// sdo:generated sha256="+hex.EncodeToString(sum[:])+"\n"), body...)
	if err := os.WriteFile(stale, old, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "an earlier build's tree"}} {
		if _, err := processio.Git(context.Background(), append([]string{"-C", repo}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	second, err := PrepareInstallTree(context.Background(), launch)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("the earlier template was not regenerated into a new head")
	}
	got, _ := os.ReadFile(stale)
	if string(got) != string(current) {
		t.Fatalf("regenerated detector differs from the current template:\n%s", got)
	}
	third, err := PrepareInstallTree(context.Background(), launch)
	if err != nil || third != second {
		t.Fatalf("a prepared tree changed: %q -> %q, %v", second, third, err)
	}
}
