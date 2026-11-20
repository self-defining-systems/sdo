//go:build integration

package bench

import (
	"os"
	"path/filepath"
	"testing"
)

func benchRoot(t *testing.T) string {
	t.Helper()
	d := filepath.Join(t.TempDir(), "root")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "hosts.yaml"), []byte("hosts: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestResolveRootPrecedence(t *testing.T) {
	flagRoot, envRoot, mgrRoot := benchRoot(t), benchRoot(t), benchRoot(t)
	ptr := filepath.Join(t.TempDir(), "state", "fuzzops", "bench-root")
	if err := RecordRoot(ptr, mgrRoot); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, flag, env, root, source string
	}{
		{"flag beats env and manager", flagRoot, envRoot, flagRoot, SourceFlag},
		{"env beats manager", "", envRoot, envRoot, SourceEnv},
		{"manager is the fallback", "", "", mgrRoot, SourceManager},
	} {
		root, source := ResolveRoot(c.flag, c.env, ptr)
		if root != c.root || source != c.source {
			t.Errorf("%s: got %q from %q, want %q from %q", c.name, root, source, c.root, c.source)
		}
	}
}

func TestResolveRootNoneFound(t *testing.T) {
	dir := t.TempDir()
	if root, source := ResolveRoot("", "", filepath.Join(dir, "absent")); root != "" || source != "" {
		t.Fatalf("no pointer: %q %q", root, source)
	}
	if root, source := ResolveRoot("", "", ""); root != "" || source != "" {
		t.Fatalf("no pointer path: %q %q", root, source)
	}
	// A pointer to a directory that is no longer a bench root is ignored, not an error.
	ptr := filepath.Join(dir, "ptr")
	if err := RecordRoot(ptr, filepath.Join(dir, "gone")); err != nil {
		t.Fatal(err)
	}
	if root, _ := ResolveRoot("", "", ptr); root != "" {
		t.Fatalf("stale pointer used: %q", root)
	}
}

func TestPointerPathFollowsXDGState(t *testing.T) {
	env := map[string]string{"XDG_STATE_HOME": "/s", "HOME": "/h"}
	get := func(k string) string { return env[k] }
	if got := PointerPath(get); got != "/s/fuzzops/bench-root" {
		t.Fatalf("xdg: %q", got)
	}
	delete(env, "XDG_STATE_HOME")
	if got := PointerPath(get); got != "/h/.local/state/fuzzops/bench-root" {
		t.Fatalf("home: %q", got)
	}
	delete(env, "HOME")
	if got := PointerPath(get); got != "" {
		t.Fatalf("no home: %q", got)
	}
}
