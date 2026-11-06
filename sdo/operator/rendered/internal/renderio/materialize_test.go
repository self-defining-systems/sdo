package renderio

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tarOf(t *testing.T, entries ...tar.Header) *bytes.Reader {
	t.Helper()
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	for _, h := range entries {
		h := h
		h.Size = int64(len("x"))
		if h.Typeflag != tar.TypeReg {
			h.Size = 0
		}
		if err := w.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			_, _ = w.Write([]byte("x"))
		}
	}
	_ = w.Close()
	return bytes.NewReader(buf.Bytes())
}

func TestExtractTarRejectsEntriesThatEscapeOrWriteThroughLinks(t *testing.T) {
	cases := map[string][]tar.Header{
		"parent":    {{Name: "../evil", Typeflag: tar.TypeReg, Mode: 0o644}},
		"absolute":  {{Name: "/evil", Typeflag: tar.TypeReg, Mode: 0o644}},
		"nested up": {{Name: "a/../../evil", Typeflag: tar.TypeReg, Mode: 0o644}},
		"through symlink": {
			{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/tmp"},
			{Name: "link/evil", Typeflag: tar.TypeReg, Mode: 0o644},
		},
		"device":    {{Name: "dev", Typeflag: tar.TypeChar}},
		"hard link": {{Name: "h", Typeflag: tar.TypeLink, Linkname: "x"}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "out")
			if err := os.Mkdir(dest, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := ExtractTar(tarOf(t, entries...), dest); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestExtractTarWritesFilesDirsAndModes(t *testing.T) {
	dest := t.TempDir()
	err := ExtractTar(tarOf(t,
		tar.Header{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o755},
		tar.Header{Name: "d/f", Typeflag: tar.TypeReg, Mode: 0o755},
		tar.Header{Name: "g", Typeflag: tar.TypeReg, Mode: 0o600},
		tar.Header{Name: "s", Typeflag: tar.TypeSymlink, Linkname: "g"},
	), dest)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(dest, "d/f")); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("d/f: %v %v", info, err)
	}
	if info, err := os.Stat(filepath.Join(dest, "g")); err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("g: %v %v", info, err)
	}
	if target, err := os.Readlink(filepath.Join(dest, "s")); err != nil || target != "g" {
		t.Fatalf("s: %q %v", target, err)
	}
}

func TestPruneKeepsHeldRecentAndNewestAndDropsStalePartials(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-2 * time.Hour)
	mk := func(name string, at time.Time) {
		if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(filepath.Join(root, name), at, at)
	}
	mk("held", old)
	mk("newest", old.Add(time.Minute))
	mk("stale", old)
	mk("recent", time.Now())
	mk(".extract-old", old)
	mk(".extract-live", time.Now())
	if err := Prune(root, map[string]bool{"held": true}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{"held": true, "newest": false, "stale": false, "recent": true, ".extract-old": false, ".extract-live": true} {
		_, err := os.Stat(filepath.Join(root, name))
		// "newest" is kept only if among the newest keep=1: "recent" is newer.
		if (err == nil) != want {
			t.Errorf("%s exists=%v want %v", name, err == nil, want)
		}
	}
}
