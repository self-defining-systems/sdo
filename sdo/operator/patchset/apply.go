package patchset

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"sdo.dev/operator/patchset/internal/gitio"
)

var objectID = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

// Store names the repository whose object database holds the base tree. It is
// only read: results are written to a private object directory.
type Store struct {
	// Dir is any path `git -C Dir` accepts (work tree, git dir, bare repository).
	Dir string
}

// Applied is the result of applying a series. Its objects (and only those) live
// in a private directory that reads the base store through an alternate, so the
// accepted repository is never written. Close removes the directory.
type Applied struct {
	Tree string
	// GitDir is the base store's git directory; ObjectDir the private object
	// directory; Alternate the base store's object directory.
	GitDir, ObjectDir, Alternate string
	cleanup                      func()
}

// Env is the environment under which git reads Tree: GIT_DIR plus the private
// object directory and the alternate.
func (a *Applied) Env() []string {
	return []string{"GIT_DIR=" + a.GitDir, "GIT_OBJECT_DIRECTORY=" + a.ObjectDir, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + a.Alternate}
}

// Close deletes the private object directory.
func (a *Applied) Close() {
	if a != nil && a.cleanup != nil {
		a.cleanup()
		a.cleanup = nil
	}
}

// Edge is the process and scratch-directory seam of Apply. Production uses
// gitio.Process; a test may supply a scripted fake.
type Edge interface {
	Run(ctx context.Context, call gitio.Call) (gitio.Output, error)
	Scratch() (string, func(), error)
}

// Applier applies series through an Edge.
type Applier struct{ edge Edge }

// NewApplier builds an Applier. A nil edge selects real git processes.
func NewApplier(edge Edge) Applier {
	if edge == nil {
		edge = gitio.Process{}
	}
	return Applier{edge: edge}
}

// Apply applies s to the tree base in store using the real git.
func Apply(ctx context.Context, store Store, base string, s Series) (*Applied, error) {
	return NewApplier(nil).Apply(ctx, store, base, s)
}

// Apply applies each patch, in order, to a temporary index seeded from base:
// `git apply --cached` with zero fuzz, no whitespace repair, no configuration,
// no attributes. The result is a pure function of (base tree, series). A patch
// that does not apply yields a *PatchError of kind conflict, upstreamed or
// missing_file naming the patch; any other error is an infrastructure failure.
func (a Applier) Apply(ctx context.Context, store Store, base string, s Series) (*Applied, error) {
	if store.Dir == "" {
		return nil, fmt.Errorf("patchset: store directory is empty")
	}
	if !objectID.MatchString(base) {
		return nil, fmt.Errorf("patchset: base tree %q is not a Git object ID", base)
	}
	scratch, cleanup, err := a.edge.Scratch()
	if err != nil {
		return nil, fmt.Errorf("patchset: scratch directory: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			cleanup()
		}
	}()
	r := runner{ctx: ctx, edge: a.edge, scratch: scratch}
	gitDir, err := r.discover(store.Dir, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return nil, err
	}
	alternate, err := r.discover(store.Dir, "rev-parse", "--path-format=absolute", "--git-path", "objects")
	if err != nil {
		return nil, err
	}
	r.gitDir = gitDir
	r.alternate = alternate
	r.objects = filepath.Join(scratch, "objects")
	r.index = filepath.Join(scratch, "index")
	if out, err := r.must("cat-file", "-t", base); err != nil {
		return nil, err
	} else if kind := strings.TrimSpace(string(out)); kind != "tree" {
		return nil, fmt.Errorf("patchset: %s is a %s, not a tree", base, kind)
	}
	if _, err := r.must("read-tree", base); err != nil {
		return nil, err
	}
	for _, p := range s.patches {
		if err := r.applyOne(p); err != nil {
			return nil, err
		}
	}
	out, err := r.must("write-tree")
	if err != nil {
		return nil, err
	}
	tree := strings.TrimSpace(string(out))
	if !objectID.MatchString(tree) {
		return nil, fmt.Errorf("patchset: git write-tree printed %q", tree)
	}
	ok = true
	return &Applied{Tree: tree, GitDir: gitDir, ObjectDir: r.objects, Alternate: alternate, cleanup: cleanup}, nil
}

type runner struct {
	ctx                               context.Context
	edge                              Edge
	scratch                           string
	gitDir, alternate, objects, index string
}

// baseEnv is the configuration-free environment shared by every call.
func (r runner) baseEnv() []string {
	return []string{
		"HOME=" + r.scratch,
		"XDG_CONFIG_HOME=" + r.scratch,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_ATTR_NOSYSTEM=1",
		"GIT_LITERAL_PATHSPECS=1",
		"GIT_TERMINAL_PROMPT=0",
		"LC_ALL=C",
	}
}

var gitConfig = []string{
	"-c", "core.autocrlf=false",
	"-c", "core.safecrlf=false",
	"-c", "core.attributesFile=/dev/null",
	"-c", "core.fsmonitor=false",
	"-c", "core.hooksPath=/dev/null",
}

func (r runner) call(stdin []byte, args ...string) gitio.Call {
	env := append(r.baseEnv(),
		"GIT_DIR="+r.gitDir,
		"GIT_INDEX_FILE="+r.index,
		"GIT_OBJECT_DIRECTORY="+r.objects,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES="+r.alternate,
	)
	return gitio.Call{Dir: r.scratch, Env: env, Args: append(append([]string(nil), gitConfig...), args...), Stdin: stdin}
}

// discover runs a repository query before the store is known, with no
// repository-selecting environment.
func (r runner) discover(dir string, args ...string) (string, error) {
	out, err := r.edge.Run(r.ctx, gitio.Call{Dir: dir, Env: r.baseEnv(), Args: append(append([]string(nil), gitConfig...), args...)})
	if err != nil {
		return "", fmt.Errorf("patchset: git %s in %s: %w", args[0], dir, err)
	}
	if out.ExitCode != 0 {
		return "", fmt.Errorf("patchset: git %s in %s: exit %d: %s", args[0], dir, out.ExitCode, snippet(out.Stderr))
	}
	return strings.TrimSpace(string(out.Stdout)), nil
}

func (r runner) must(args ...string) ([]byte, error) {
	out, err := r.edge.Run(r.ctx, r.call(nil, args...))
	if err != nil {
		return nil, fmt.Errorf("patchset: git %s: %w", args[0], err)
	}
	if out.ExitCode != 0 {
		return nil, fmt.Errorf("patchset: git %s: exit %d: %s", args[0], out.ExitCode, snippet(out.Stderr))
	}
	return out.Stdout, nil
}

var applyFlags = []string{"apply", "--cached", "--whitespace=nowarn", "-p1"}

func (r runner) apply(p Patch, extra ...string) (gitio.Output, error) {
	args := append(append([]string(nil), applyFlags...), extra...)
	out, err := r.edge.Run(r.ctx, r.call(p.Diff, append(args, "-")...))
	if err != nil {
		return out, fmt.Errorf("patchset: git apply %s: %w", p.Name, err)
	}
	return out, nil
}

func (r runner) applyOne(p Patch) error {
	fwd, err := r.apply(p, "--check")
	if err != nil {
		return err
	}
	if fwd.ExitCode == 0 {
		out, err := r.apply(p)
		if err != nil {
			return err
		}
		if out.ExitCode != 0 {
			return fmt.Errorf("patchset: git apply %s passed --check but failed: %s", p.Name, snippet(out.Stderr))
		}
		return nil
	}
	rev, err := r.apply(p, "--check", "--reverse")
	if err != nil {
		return err
	}
	if rev.ExitCode == 0 {
		return perr(KindUpstreamed, p.Name, p.Paths(), "change is already present (reverse apply succeeds): retire this patch")
	}
	missing, err := r.missing(p)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return perr(KindMissingFile, p.Name, missing, "file not in the tree the patch applies to")
	}
	return perr(KindConflict, p.Name, p.Paths(), "%s", snippet(fwd.Stderr))
}

// missing lists the files the patch needs to exist that the index lacks.
func (r runner) missing(p Patch) ([]string, error) {
	var need []string
	for _, f := range p.Files {
		switch f.Op {
		case OpModify, OpDelete:
			need = append(need, f.Path)
		case OpRename, OpCopy:
			need = append(need, f.OldPath)
		}
	}
	if len(need) == 0 {
		return nil, nil
	}
	out, err := r.must(append([]string{"ls-files", "-z", "--"}, need...)...)
	if err != nil {
		return nil, err
	}
	have := map[string]bool{}
	for _, name := range bytes.Split(out, []byte{0}) {
		have[string(name)] = true
	}
	var missing []string
	seen := map[string]bool{}
	for _, n := range need {
		if !have[n] && !seen[n] {
			seen[n] = true
			missing = append(missing, n)
		}
	}
	sortStrings(missing)
	return missing, nil
}

// snippet is git's stderr, trimmed and bounded, for error details.
func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 1024 {
		s = s[:1024] + "..."
	}
	return s
}
