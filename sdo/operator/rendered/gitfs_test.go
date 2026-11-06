package rendered

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
)

func TestGitTreeFSClassifiesGitFailureAsUnavailable(t *testing.T) {
	boom := errors.New("boom")
	objects := ObjectStore{Dir: "x", Git: func(context.Context, string, ...string) ([]byte, error) { return nil, boom }}
	id := strings.Repeat("b", 40)
	if _, err := Effective(context.Background(), objects, BaseTree(id)); !errors.Is(err, ErrTreeUnavailable) || !errors.Is(err, boom) {
		t.Fatalf("Effective = %v", err)
	}
	if _, err := GitTreeFS(context.Background(), objects, EffectiveTree(id)); !errors.Is(err, ErrTreeUnavailable) {
		t.Fatalf("GitTreeFS = %v", err)
	}
}

// listing is synthetic `git ls-tree -r -z --full-tree -l` output.
func listing(records ...string) string { return strings.Join(records, "\x00") + "\x00" }

func TestGitTreeFSReadsTheLsTreeListing(t *testing.T) {
	id := strings.Repeat("c", 40)
	blob := strings.Repeat("d", 40)
	out := listing(
		"100644 blob "+blob+"      12\tapp/a.yaml",
		"100755 blob "+blob+"       3\trun.sh",
		"120000 blob "+blob+"       5\tlink",
		"160000 commit "+blob+"       -\tvendor/mod",
		"100644 blob "+blob+"       4\t.sdo/goal.md",
		"100644 blob "+blob+"       6\t.sdo/deployment/p.yaml",
	)
	objects := ObjectStore{Dir: "x", Git: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "cat-file" && args[1] == "-t" {
			return []byte("tree\n"), nil
		}
		return []byte(out), nil
	}}
	fsys, err := GitTreeFS(context.Background(), objects, EffectiveTree(id))
	if err != nil {
		t.Fatal(err)
	}
	info, err := fsys.Stat("app/a.yaml")
	if err != nil || info.Size() != 12 {
		t.Fatalf("size from ls-tree -l: %v, %v", info, err)
	}
	if got, _ := fsys.Blob("run.sh"); got != blob {
		t.Fatalf("Blob = %q", got)
	}
	for _, name := range []string{"link", "vendor", "vendor/mod", ".sdo/goal.md"} {
		if _, err := fsys.Stat(name); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat(%s) = %v, want not exist", name, err)
		}
	}
	if _, err := fsys.Stat(".sdo/deployment/p.yaml"); err != nil {
		t.Fatalf(".sdo/deployment must stay visible: %v", err)
	}
}

func TestGitTreeFSRejectsMalformedListing(t *testing.T) {
	objects := ObjectStore{Dir: "x", Git: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "cat-file" {
			return []byte("tree\n"), nil
		}
		return []byte("garbage\x00"), nil
	}}
	if _, err := GitTreeFS(context.Background(), objects, EffectiveTree(strings.Repeat("c", 40))); !errors.Is(err, ErrTreeUnavailable) {
		t.Fatalf("err = %v", err)
	}
}
