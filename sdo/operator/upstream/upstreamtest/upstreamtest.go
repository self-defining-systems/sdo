// Package upstreamtest holds the conformance suite every upstream watcher must
// pass. Fixtures are local bare Git repositories created at test time.
package upstreamtest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	"sdo.dev/operator/internal/testutil/gitio"
	"sdo.dev/operator/upstream"
)

// ConfigFunc builds the watcher's config for the branch ref of the Git
// repository at url.
type ConfigFunc func(url, ref string) *anypb.Any

const branch = "main"

// Remote is a local bare repository with a work clone that pushes to it.
type Remote struct {
	URL  string
	work string
	n    int
}

// NewRemote creates a bare repository whose main branch holds one commit.
func NewRemote(t testing.TB) *Remote {
	t.Helper()
	dir := t.TempDir()
	r := &Remote{URL: filepath.Join(dir, "remote.git"), work: filepath.Join(dir, "work")}
	run(t, dir, "init", "--quiet", "--bare", "--initial-branch="+branch, r.URL)
	run(t, dir, "init", "--quiet", "--initial-branch="+branch, r.work)
	r.Push(t)
	return r
}

// Push commits a new file and pushes it, returning the new commit ID.
func (r *Remote) Push(t testing.TB) string {
	t.Helper()
	r.n++
	name := "f" + string(rune('a'+r.n)) + ".txt"
	if err := os.WriteFile(filepath.Join(r.work, name), []byte(name), 0o600); err != nil {
		t.Fatal(err)
	}
	run(t, r.work, "add", name)
	run(t, r.work, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "--quiet", "-m", name)
	run(t, r.work, "push", "--quiet", r.URL, "HEAD:refs/heads/"+branch)
	return strings.TrimSpace(run(t, r.work, "rev-parse", "HEAD"))
}

// memoryRemoteRef is the broker's memory-remote ref (broker/upstream.go). A
// ref name cannot also be a directory, so watchers must keep their refs
// elsewhere.
const memoryRemoteRef = "refs/sdo/upstream"

// NewObjectStore creates an empty bare repository to materialize into.
func NewObjectStore(t testing.TB) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "objects.git")
	run(t, filepath.Dir(dir), "init", "--quiet", "--bare", dir)
	return dir
}

func run(t testing.TB, dir string, args ...string) string {
	t.Helper()
	out, err := gitio.Run(context.Background(), dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// TestWatcher runs the watcher family contract against id in c:
//
//	stable        Latest of an unchanged remote is the same revision
//	advances      a pushed commit becomes the latest revision
//	materialize   returns rev.ID, and the commit exists in gitDir
//	memory ref    materializing works beside the broker's memory-remote ref
//	              refs/sdo/upstream, which the accepted repository may hold
//	unreachable   an unreachable remote is upstream.ErrUnavailable from both calls
//	cancelled     a cancelled context fails both calls without ErrUnavailable
//	unknown       an unregistered ID and a foreign config type are rejected
func TestWatcher(t *testing.T, c *upstream.Catalog, id string, config ConfigFunc) {
	t.Helper()
	ctx := context.Background()

	t.Run("stable", func(t *testing.T) {
		r := NewRemote(t)
		cfg := config(r.URL, branch)
		a, err := c.Latest(ctx, id, cfg)
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.Latest(ctx, id, cfg)
		if err != nil || a != b {
			t.Fatalf("Latest of an unchanged remote = %+v, %v; first call %+v", b, err, a)
		}
	})

	t.Run("advances", func(t *testing.T) {
		r := NewRemote(t)
		cfg := config(r.URL, branch)
		before, err := c.Latest(ctx, id, cfg)
		if err != nil {
			t.Fatal(err)
		}
		pushed := r.Push(t)
		after, err := c.Latest(ctx, id, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if after.ID != pushed || after.ID == before.ID {
			t.Fatalf("Latest after push = %q, want the pushed %q (was %q)", after.ID, pushed, before.ID)
		}
	})

	t.Run("materialize", func(t *testing.T) {
		r := NewRemote(t)
		cfg := config(r.URL, branch)
		rev, err := c.Latest(ctx, id, cfg)
		if err != nil {
			t.Fatal(err)
		}
		store := NewObjectStore(t)
		commit, err := c.Materialize(ctx, id, cfg, rev, store)
		if err != nil {
			t.Fatal(err)
		}
		if commit != rev.ID {
			t.Fatalf("Materialize = %q, want rev.ID %q", commit, rev.ID)
		}
		if out, err := gitio.Run(ctx, store, "cat-file", "-e", commit+"^{commit}"); err != nil {
			t.Fatalf("commit %s is not in the object store: %v\n%s", commit, err, out)
		}
	})

	t.Run("memory ref", func(t *testing.T) {
		r := NewRemote(t)
		cfg := config(r.URL, branch)
		store := NewObjectStore(t)
		memoryRef := func() {
			run(t, store, "fetch", "--quiet", "--no-tags", r.URL, "+refs/heads/"+branch+":"+memoryRemoteRef)
		}
		memoryRef()
		for range 2 {
			rev, err := c.Latest(ctx, id, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Materialize(ctx, id, cfg, rev, store); err != nil {
				t.Fatalf("Materialize beside %s: %v", memoryRemoteRef, err)
			}
			r.Push(t)
		}
		memoryRef()
	})

	t.Run("unreachable", func(t *testing.T) {
		cfg := config(filepath.Join(t.TempDir(), "gone.git"), branch)
		if _, err := c.Latest(ctx, id, cfg); !errors.Is(err, upstream.ErrUnavailable) {
			t.Errorf("Latest of an unreachable remote = %v, want ErrUnavailable", err)
		}
		rev := upstream.Revision{ID: strings.Repeat("a", 40), Ref: branch}
		if _, err := c.Materialize(ctx, id, cfg, rev, NewObjectStore(t)); !errors.Is(err, upstream.ErrUnavailable) {
			t.Errorf("Materialize from an unreachable remote = %v, want ErrUnavailable", err)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		r := NewRemote(t)
		cfg := config(r.URL, branch)
		rev, err := c.Latest(ctx, id, cfg)
		if err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := c.Latest(cctx, id, cfg); err == nil || errors.Is(err, upstream.ErrUnavailable) {
			t.Errorf("Latest with a cancelled context = %v, want a non-unavailable error", err)
		}
		if _, err := c.Materialize(cctx, id, cfg, rev, NewObjectStore(t)); err == nil || errors.Is(err, upstream.ErrUnavailable) {
			t.Errorf("Materialize with a cancelled context = %v, want a non-unavailable error", err)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		cfg := config("unused", branch)
		if _, err := c.Latest(ctx, "no-such-watcher", cfg); !errors.Is(err, upstream.ErrUnknownWatcher) {
			t.Errorf("unregistered ID = %v, want ErrUnknownWatcher", err)
		}
		other, err := anypb.New(&anypb.Any{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Latest(ctx, id, other); err == nil {
			t.Error("a foreign config type was accepted")
		}
		if _, err := c.Latest(ctx, id, nil); err == nil {
			t.Error("a nil config was accepted")
		}
	})
}
