//go:build integration

package bench

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const silly = ".nfs000000000cb3147000015832" // what an NFS client leaves for a file deleted while open

// stepDir is a step directory with a result, a kept ".nfs…" look-alike file and directory, and a manifest.
func stepDir(t *testing.T, leftover bool) string {
	t.Helper()
	src := filepath.Join(t.TempDir(), "step")
	must(t, os.MkdirAll(filepath.Join(src, ".nfsdir"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "result.json"), []byte(`{}`), 0o644))
	must(t, os.WriteFile(filepath.Join(src, ".nfs-notes.md"), []byte("kept"), 0o644))
	must(t, os.WriteFile(filepath.Join(src, ".nfsdir", "a.json"), []byte("kept"), 0o644))
	if leftover {
		must(t, os.WriteFile(filepath.Join(src, silly), []byte("deleted while open"), 0o644))
	}
	m, err := StepManifest(src)
	must(t, err)
	must(t, writeJSON(filepath.Join(src, "manifest.json"), m))
	return src
}

func paths(m []ManifestEntry) (out []string) {
	for _, e := range m {
		out = append(out, e.Path)
	}
	return out
}

func wantHome(t *testing.T, dest string) {
	t.Helper()
	for _, f := range []string{"result.json", ".nfs-notes.md", filepath.Join(".nfsdir", "a.json")} {
		if _, err := os.Stat(filepath.Join(dest, f)); err != nil {
			t.Fatalf("%s is not home: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dest, silly)); err == nil {
		t.Fatal("the NFS leftover was copied home")
	}
}

// A file deleted while a process still holds it open stays on NFS as ".nfsXXXX" until the handle closes. It is
// not part of a step: neither the manifest nor the copy carries it, so a step directory is adopted whether or not
// one is there at the moment of the copy (it used to be listed, vanish, and fail every adopt with "missing").
// Only that name, as a regular file, is skipped: a file called ".nfs-notes.md" or a directory ".nfsdir" is output.
func TestFetchIgnoresOnlyTheNFSLeftoversOfAStepDirectory(t *testing.T) {
	src := stepDir(t, true)
	m, err := StepManifest(src)
	must(t, err)
	for _, p := range paths(m) {
		if p == silly {
			t.Fatalf("manifest %v lists the NFS leftover", paths(m))
		}
	}
	dest := filepath.Join(t.TempDir(), "home")
	must(t, fetchDir(src, dest))
	wantHome(t, dest)
	// The same, with the leftover gone between the manifest and the copy.
	must(t, os.Remove(filepath.Join(src, silly)))
	dest2 := filepath.Join(t.TempDir(), "home")
	must(t, fetchDir(src, dest2))
	wantHome(t, dest2)
}

// An input is stored by content as it is: Manifest (the hash) and the copy keep every name.
func TestInputsKeepEveryName(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	must(t, os.MkdirAll(in, 0o755))
	must(t, os.WriteFile(filepath.Join(in, silly), []byte("an input file"), 0o644))
	m, err := Manifest(in)
	must(t, err)
	if len(m) != 1 || m[0].Path != silly {
		t.Fatalf("input manifest %v", paths(m))
	}
	root := t.TempDir()
	stored, err := Store(root, in)
	must(t, err)
	if _, err := os.Stat(filepath.Join(root, stored.Path, silly)); err != nil {
		t.Fatalf("input file not stored: %v", err)
	}
}

// The ssh path fetches with rsync: the leftover stays on the host, everything else (a ".nfs-notes.md", a
// ".nfsdir/") comes home and passes the manifest check.
func TestSSHFetchLeavesNFSLeftoversAndKeepsLookAlikes(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync")
	}
	dir := t.TempDir()
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\nshift\nexec sh -c \"$*\"\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	staging := filepath.Join(dir, "staging")
	remote := filepath.Join(staging, "c1", "step1")
	must(t, os.MkdirAll(filepath.Dir(remote), 0o755))
	must(t, os.Rename(stepDir(t, true), remote))
	h := sshHost{name: "h", h: Host{SSH: "fakehost", Staging: staging}}
	dest := filepath.Join(dir, "home")
	must(t, h.Fetch(context.Background(), "c1", "step1", dest))
	wantHome(t, dest)
}
