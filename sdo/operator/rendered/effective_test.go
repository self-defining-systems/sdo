package rendered

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// rootRepo answers the root listing Effective makes from synthetic ls-tree
// records and records the mktree input.
type rootRepo struct {
	root    []string
	written [][]string
}

func (f *rootRepo) git(_ context.Context, _ string, args ...string) ([]byte, error) {
	switch {
	case args[0] == "cat-file" && args[1] == "-t":
		return []byte("tree\n"), nil
	case args[0] == "ls-tree":
		return []byte(strings.Join(f.root, "\x00") + "\x00"), nil
	}
	return nil, fmt.Errorf("fake repo: unexpected git %v", args)
}

func (f *rootRepo) mktree(_ context.Context, _ string, records []string) (string, error) {
	f.written = append(f.written, records)
	return strings.Repeat("f", 40), nil
}

func rec(kind, name string) string {
	mode := "100644"
	if kind == "tree" {
		mode = "040000"
	}
	return fmt.Sprintf("%s %s %s\t%s", mode, kind, strings.Repeat("e", 40), name)
}

func tid(c byte) BaseTree { return BaseTree(strings.Repeat(string(c), 40)) }

func TestEffectiveDropsOnlyMemoryAndRuntimeRoots(t *testing.T) {
	repo := &rootRepo{root: []string{rec("tree", ".sdo"), rec("tree", ".sdo-runtime"), rec("tree", "app"), rec("blob", "Dockerfile"), rec("tree", ".sdoextra")}}
	eff, err := Effective(context.Background(), ObjectStore{Dir: t.Name(), Git: repo.git, MkTree: repo.mktree}, tid('a'))
	if err != nil {
		t.Fatal(err)
	}
	if eff.ID != strings.Repeat("f", 40) || len(repo.written) != 1 {
		t.Fatalf("effective = %+v, writes %d", eff, len(repo.written))
	}
	got := strings.Join(repo.written[0], "|")
	want := strings.Join([]string{rec("tree", "app"), rec("blob", "Dockerfile"), rec("tree", ".sdoextra")}, "|")
	if got != want {
		t.Fatalf("projected root = %q, want %q", got, want)
	}
}

func TestEffectiveIsTheBaseTreeWithoutMemory(t *testing.T) {
	repo := &rootRepo{root: []string{rec("tree", "app"), rec("tree", "deploy")}}
	eff, err := Effective(context.Background(), ObjectStore{Dir: t.Name(), Git: repo.git, MkTree: repo.mktree}, tid('b'))
	if err != nil || eff.ID != string(tid('b')) || len(repo.written) != 0 {
		t.Fatalf("a tree that is its own projection needs no write: %+v %v, writes %d", eff, err, len(repo.written))
	}
}

func TestEffectiveFailuresAreUnavailable(t *testing.T) {
	repo := &rootRepo{root: []string{rec("tree", ".sdo")}}
	failing := func(context.Context, string, []string) (string, error) { return "", errors.New("disk full") }
	_, err := Effective(context.Background(), ObjectStore{Dir: t.Name(), Git: repo.git, MkTree: failing}, tid('c'))
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("err = %v", err)
	}
}
