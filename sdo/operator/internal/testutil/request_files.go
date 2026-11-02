package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// PublishRequest writes a request file into a directory that a server polls
// the way the production client does: into a dot-prefixed temporary name, then
// a rename. The server skips dot-prefixed names, so it never reads a request
// that is only partly written. Tests must use this for every write into a
// polled request directory; request_dir_policy_test.go enforces it.
func PublishRequest(t testing.TB, directory, name string, payload []byte) {
	t.Helper()
	hidden := filepath.Join(directory, "."+name)
	if err := os.WriteFile(hidden, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(hidden, filepath.Join(directory, name)); err != nil {
		t.Fatal(err)
	}
}
