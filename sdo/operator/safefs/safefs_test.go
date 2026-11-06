package safefs_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/safefs"
)

func setup(t *testing.T) (root, outside string) {
	t.Helper()
	base := t.TempDir()
	root = filepath.Join(base, "checkout")
	outside = filepath.Join(base, "outside")
	for _, d := range []string{root, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root, outside
}

func untouched(t *testing.T, file string) {
	t.Helper()
	if data, err := os.ReadFile(file); err != nil || string(data) != "precious" {
		t.Fatalf("outside file changed: %q %v", data, err)
	}
}

func TestWriteFileRefusesSymlinkShapes(t *testing.T) {
	cases := map[string]func(root, outside string){
		"final file": func(root, outside string) {
			os.MkdirAll(filepath.Join(root, "a/b"), 0o755)
			os.Symlink(filepath.Join(outside, "victim"), filepath.Join(root, "a/b/f.yaml"))
		},
		"first component": func(root, outside string) {
			os.Symlink(outside, filepath.Join(root, "a"))
		},
		"middle component": func(root, outside string) {
			os.MkdirAll(filepath.Join(root, "a"), 0o755)
			os.Symlink(outside, filepath.Join(root, "a/b"))
		},
		"dangling final": func(root, outside string) {
			os.MkdirAll(filepath.Join(root, "a/b"), 0o755)
			os.Symlink(filepath.Join(outside, "new"), filepath.Join(root, "a/b/f.yaml"))
		},
		"link inside the root": func(root, outside string) {
			os.MkdirAll(filepath.Join(root, "real"), 0o755)
			os.Symlink("real", filepath.Join(root, "a"))
		},
	}
	for name, plant := range cases {
		t.Run(name, func(t *testing.T) {
			root, outside := setup(t)
			victim := filepath.Join(outside, "victim")
			os.WriteFile(victim, []byte("precious"), 0o644)
			plant(root, outside)
			err := safefs.WriteFile(root, "a/b/f.yaml", []byte("x"), 0o644)
			var se *safefs.SymlinkError
			if !errors.As(err, &se) || !errors.Is(err, safefs.ErrSymlink) {
				t.Fatalf("want SymlinkError, got %v", err)
			}
			untouched(t, victim)
			if _, err := os.Stat(filepath.Join(outside, "new")); err == nil {
				t.Fatal("created a file outside the root")
			}
			if _, err := os.Stat(filepath.Join(outside, "b")); err == nil {
				t.Fatal("created a directory outside the root")
			}
		})
	}
}

func TestWriteFileCreatesAndReplaces(t *testing.T) {
	root, _ := setup(t)
	if err := safefs.WriteFile(root, "x/y/z.txt", []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := safefs.WriteFile(root, "x/y/z.txt", []byte("two"), 0o755); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(root, "x/y/z.txt"))
	info, _ := os.Stat(filepath.Join(root, "x/y/z.txt"))
	if string(data) != "two" || info.Mode().Perm() != 0o755 {
		t.Fatalf("got %q %v", data, info.Mode())
	}
	entries, _ := os.ReadDir(filepath.Join(root, "x/y"))
	if len(entries) != 1 {
		t.Fatalf("temporary file left behind: %v", entries)
	}
}

func TestWriteFileRejectsEscapingPaths(t *testing.T) {
	root, _ := setup(t)
	for _, rel := range []string{"../x", "/abs", "a/../../x", ""} {
		if err := safefs.WriteFile(root, rel, []byte("x"), 0o644); err == nil {
			t.Fatalf("%q accepted", rel)
		}
	}
}

func TestRemoveRefusesLinkedParent(t *testing.T) {
	root, outside := setup(t)
	victim := filepath.Join(outside, "victim")
	os.WriteFile(victim, []byte("precious"), 0o644)
	os.Symlink(outside, filepath.Join(root, "corpus"))
	if err := safefs.Remove(root, "corpus/victim"); !errors.Is(err, safefs.ErrSymlink) {
		t.Fatalf("got %v", err)
	}
	untouched(t, victim)
}

func TestMkdirAllRefusesLink(t *testing.T) {
	root, outside := setup(t)
	os.Symlink(outside, filepath.Join(root, ".sdo"))
	if err := safefs.MkdirAll(root, ".sdo/deployment", 0o755); !errors.Is(err, safefs.ErrSymlink) {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "deployment")); err == nil {
		t.Fatal("created outside")
	}
}
