package patchset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/patchset/internal/gitio"
)

// ViewFile is the file under a checkout's git directory that records its
// effective view. It never leaves the checkout.
const ViewFile = "sdo-effective-view.json"

// View is the private record of a materialized checkout: BaseTree is the tree
// of the accepted base commit (patches and all), EffectiveTree is the tree the
// working files started from, base plus the patch series. Every object of
// EffectiveTree is in the checkout's own object database. `patch create`
// moves EffectiveTree forward; scratch edits are measured against it.
type View struct {
	Version       int    `json:"version"`
	BaseTree      string `json:"base_tree"`
	EffectiveTree string `json:"effective_tree"`
}

// ErrNoView means the checkout has no recorded effective view (it was not
// created by the broker, or predates the patch model).
var ErrNoView = errors.New("checkout has no recorded effective view (it was not created by the SDO broker)")

// checkout runs configuration-free git against one working checkout.
type checkout struct {
	root, gitDir, scratch string
	edge                  gitio.Process
	cleanup               func()
}

func openCheckout(ctx context.Context, root string) (*checkout, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	scratch, cleanup, err := gitio.Process{}.Scratch()
	if err != nil {
		return nil, fmt.Errorf("patchset: scratch directory: %w", err)
	}
	c := &checkout{root: abs, scratch: scratch, cleanup: cleanup}
	out, err := c.git(ctx, nil, nil, "rev-parse", "--absolute-git-dir")
	if err != nil {
		cleanup()
		return nil, err
	}
	c.gitDir = strings.TrimSpace(string(out))
	return c, nil
}

func (c *checkout) close() { c.cleanup() }

// git runs one command in the checkout. extra environment entries override
// the defaults (a private object directory, a scratch index).
func (c *checkout) git(ctx context.Context, extra []string, stdin []byte, args ...string) ([]byte, error) {
	env := []string{
		"HOME=" + c.scratch,
		"XDG_CONFIG_HOME=" + c.scratch,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_ATTR_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"LC_ALL=C",
	}
	if c.gitDir != "" {
		env = append(env, "GIT_DIR="+c.gitDir, "GIT_WORK_TREE="+c.root)
	}
	env = append(env, extra...)
	full := append([]string{
		"-c", "core.autocrlf=false", "-c", "core.safecrlf=false", "-c", "core.fsmonitor=false",
		"-c", "core.hooksPath=/dev/null", "-c", "core.quotepath=false",
	}, args...)
	out, err := c.edge.Run(ctx, gitio.Call{Dir: c.root, Env: env, Args: full, Stdin: stdin})
	if err != nil {
		return nil, fmt.Errorf("patchset: git %s: %w", args[0], err)
	}
	if out.ExitCode != 0 {
		return nil, fmt.Errorf("patchset: git %s: exit %d: %s", args[0], out.ExitCode, snippet(out.Stderr))
	}
	return out.Stdout, nil
}

func (c *checkout) indexEnv(name string, extra []string) []string {
	return append(append([]string(nil), extra...), "GIT_INDEX_FILE="+filepath.Join(c.scratch, name))
}

func (c *checkout) viewPath() string { return filepath.Join(c.gitDir, ViewFile) }

func (c *checkout) loadView() (View, error) {
	data, err := os.ReadFile(c.viewPath())
	if errors.Is(err, os.ErrNotExist) {
		return View{}, ErrNoView
	}
	if err != nil {
		return View{}, err
	}
	var v View
	if err := json.Unmarshal(data, &v); err != nil || v.Version != 1 || !objectID.MatchString(v.BaseTree) || !objectID.MatchString(v.EffectiveTree) {
		return View{}, fmt.Errorf("patchset: effective view record is malformed")
	}
	return v, nil
}

func (c *checkout) saveView(v View) error {
	v.Version = 1
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := c.viewPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.viewPath())
}

// LoadView reads the checkout's effective view; ok is false when there is none.
func LoadView(ctx context.Context, root string) (v View, ok bool, err error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return View{}, false, err
	}
	defer c.close()
	v, err = c.loadView()
	if errors.Is(err, ErrNoView) {
		return View{}, false, nil
	}
	return v, err == nil, err
}

// change is one entry of `git diff-tree --raw`.
type change struct {
	status  byte // 'A', 'M', 'D'
	mode    string
	oldSHA  string
	newSHA  string
	path    string
	content []byte
}

func (c *checkout) diffRaw(ctx context.Context, env []string, from, to string) ([]change, error) {
	out, err := c.git(ctx, env, nil, "diff-tree", "-r", "-z", "--no-renames", "--no-abbrev", "--raw", from, to)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(string(out), "\x00")
	var changes []change
	for i := 0; i+1 < len(parts); i += 2 {
		meta := strings.Fields(strings.TrimPrefix(parts[i], ":"))
		if len(meta) != 5 {
			return nil, fmt.Errorf("patchset: unexpected diff-tree record %q", parts[i])
		}
		ch := change{status: meta[4][0], mode: meta[1], oldSHA: meta[2], newSHA: meta[3], path: parts[i+1]}
		if ch.status == 'D' {
			ch.mode = meta[0]
		}
		if ch.status != 'A' && ch.status != 'M' && ch.status != 'D' {
			return nil, fmt.Errorf("patchset: unexpected change %q for %s", meta[4], ch.path)
		}
		changes = append(changes, ch)
	}
	return changes, nil
}

func (c *checkout) loadBlobs(ctx context.Context, env []string, changes []change) error {
	for i := range changes {
		if changes[i].status == 'D' {
			continue
		}
		data, err := c.git(ctx, env, nil, "cat-file", "blob", changes[i].newSHA)
		if err != nil {
			return err
		}
		changes[i].content = data
	}
	return nil
}

// persist writes the blobs of changes into the checkout's own object database
// and builds, from base, the tree they make there. The result must be the
// tree the series produced; anything else means the files do not reproduce it.
func (c *checkout) persist(ctx context.Context, base, want string, changes []change) error {
	var info bytes.Buffer
	for _, ch := range changes {
		if ch.status == 'D' {
			fmt.Fprintf(&info, "0 %s\t%s\x00", strings.Repeat("0", len(ch.oldSHA)), ch.path)
			continue
		}
		sha, err := c.git(ctx, nil, ch.content, "hash-object", "-w", "--no-filters", "-t", "blob", "--stdin")
		if err != nil {
			return err
		}
		if got := strings.TrimSpace(string(sha)); got != ch.newSHA {
			return fmt.Errorf("patchset: blob of %s hashed to %s, expected %s", ch.path, got, ch.newSHA)
		}
		fmt.Fprintf(&info, "%s %s\t%s\x00", ch.mode, ch.newSHA, ch.path)
	}
	env := c.indexEnv("persist-index", nil)
	if _, err := c.git(ctx, env, nil, "read-tree", base); err != nil {
		return err
	}
	if info.Len() > 0 {
		if _, err := c.git(ctx, env, info.Bytes(), "update-index", "-z", "--index-info"); err != nil {
			return err
		}
	}
	out, err := c.git(ctx, env, nil, "write-tree")
	if err != nil {
		return err
	}
	if got := strings.TrimSpace(string(out)); got != want {
		return fmt.Errorf("patchset: persisted effective tree %s differs from the applied series %s", got, want)
	}
	return nil
}

func (c *checkout) baseTree(ctx context.Context) (string, error) {
	out, err := c.git(ctx, nil, nil, "rev-parse", "--verify", "HEAD^{tree}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// treeSeries reads the patch files of a tree.
func (c *checkout) treeSeries(ctx context.Context, tree string) ([]File, error) {
	out, err := c.git(ctx, nil, nil, "ls-tree", "-z", tree, "--", Directory+"/")
	if err != nil {
		return nil, err
	}
	var files []File
	for _, record := range strings.Split(string(out), "\x00") {
		if record == "" {
			continue
		}
		meta, path, ok := strings.Cut(record, "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 3 {
			return nil, fmt.Errorf("patchset: unexpected ls-tree record %q", record)
		}
		name := filepath.Base(path)
		if fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755") {
			return nil, perr(KindForbidden, name, nil, "%s is not a regular file", path)
		}
		data, err := c.git(ctx, nil, nil, "cat-file", "blob", fields[2])
		if err != nil {
			return nil, err
		}
		files = append(files, File{Name: name, Data: data})
	}
	return files, nil
}

// dirSeries reads the working directory's patch files.
func (c *checkout) dirSeries() ([]File, error) {
	dir := filepath.Join(c.root, filepath.FromSlash(Directory))
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []File
	for _, e := range entries {
		if !e.Type().IsRegular() {
			return nil, perr(KindForbidden, e.Name(), nil, "%s/%s is not a regular file", Directory, e.Name())
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		files = append(files, File{Name: e.Name(), Data: data})
	}
	return files, nil
}

// Materialize prepares a freshly cloned checkout of an accepted commit as the
// agent's scratch view: it applies the commit's patch series to the working
// files (never the index, never a commit) and records the effective view under
// the git directory. The accepted commit and its parentage are untouched; with
// no patches the working files do not change and only the record is written.
func Materialize(ctx context.Context, root string) (View, error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return View{}, err
	}
	defer c.close()
	base, err := c.baseTree(ctx)
	if err != nil {
		return View{}, err
	}
	files, err := c.treeSeries(ctx, base)
	if err != nil {
		return View{}, err
	}
	series, err := ParseSeries(files)
	if err != nil {
		return View{}, err
	}
	view := View{BaseTree: base, EffectiveTree: base}
	if series.Len() > 0 {
		applied, err := Apply(ctx, Store{Dir: c.root}, base, series)
		if err != nil {
			return View{}, err
		}
		defer applied.Close()
		changes, err := c.diffRaw(ctx, applied.Env(), base, applied.Tree)
		if err != nil {
			return View{}, err
		}
		if err := c.loadBlobs(ctx, applied.Env(), changes); err != nil {
			return View{}, err
		}
		if err := c.persist(ctx, base, applied.Tree, changes); err != nil {
			return View{}, err
		}
		if err := writeWorking(c.root, changes); err != nil {
			return View{}, err
		}
		view.EffectiveTree = applied.Tree
	}
	if err := c.saveView(view); err != nil {
		return View{}, err
	}
	return view, nil
}

// writeWorking puts changes into the working tree. Patch paths are validated
// regular files, but a parent that is a symlink would still escape the root.
func writeWorking(root string, changes []change) error {
	for _, ch := range changes {
		target := filepath.Join(root, filepath.FromSlash(ch.path))
		if err := safeParents(root, target); err != nil {
			return err
		}
		if ch.status == 'D' {
			if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			pruneEmpty(root, filepath.Dir(target))
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		mode := os.FileMode(0o644)
		if ch.mode == "100755" {
			mode = 0o755
		}
		if err := os.WriteFile(target, ch.content, mode); err != nil {
			return err
		}
		if err := os.Chmod(target, mode); err != nil {
			return err
		}
	}
	return nil
}

func safeParents(root, target string) error {
	rel, err := filepath.Rel(root, filepath.Dir(target))
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("patchset: %s is outside the checkout", target)
	}
	cur := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("patchset: %s is a symlink; patched paths must not traverse one", cur)
		}
	}
	return nil
}

func pruneEmpty(root, dir string) {
	for dir != root && strings.HasPrefix(dir, root) {
		if os.Remove(dir) != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// scratchPaths lists the non-.sdo files of the working tree that differ from
// tree: modified, added (untracked and not ignored) and deleted. env selects
// the object directory tree lives in.
func (c *checkout) scratchPaths(ctx context.Context, env []string, tree string) ([]string, error) {
	env = c.indexEnv("scratch-index", env)
	if _, err := c.git(ctx, env, nil, "read-tree", tree); err != nil {
		return nil, err
	}
	if _, err := c.git(ctx, env, nil, "add", "-A", "--", ".", ":(exclude).sdo"); err != nil {
		return nil, err
	}
	out, err := c.git(ctx, env, nil, "diff-index", "--cached", "--name-only", "-z", "--no-renames", tree, "--")
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, p := range strings.Split(string(out), "\x00") {
		if p != "" && p != ".sdo" && !strings.HasPrefix(p, ".sdo/") {
			paths = append(paths, p)
		}
	}
	return paths, nil
}

// Scratch lists the app files that differ from the recorded effective view.
// It is empty for a checkout without a view.
func Scratch(ctx context.Context, root string) ([]string, error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return nil, err
	}
	defer c.close()
	view, err := c.loadView()
	if err != nil {
		return nil, err
	}
	return c.scratchPaths(ctx, nil, view.EffectiveTree)
}

// Report is the outcome of Check.
type Report struct {
	Patches int
	Digest  string
	// Effective is the tree the working patch series produces from the base.
	Effective string
	// Uncaptured lists the app files whose working content the series does not
	// reproduce (the `patch.uncaptured` set).
	Uncaptured []string
}

// Check applies the working directory's patch series to the checkout's base
// tree and compares the app files of the working tree with the result. A
// series that does not parse or apply is a *PatchError; infrastructure
// failures are plain errors.
func Check(ctx context.Context, root string) (Report, error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return Report{}, err
	}
	defer c.close()
	view, err := c.loadView()
	if err != nil {
		return Report{}, err
	}
	files, err := c.dirSeries()
	if err != nil {
		return Report{}, err
	}
	series, err := ParseSeries(files)
	if err != nil {
		return Report{}, err
	}
	applied, err := Apply(ctx, Store{Dir: c.root}, view.BaseTree, series)
	if err != nil {
		return Report{}, err
	}
	defer applied.Close()
	uncaptured, err := c.scratchPaths(ctx, applied.Env(), applied.Tree)
	if err != nil {
		return Report{}, err
	}
	return Report{Patches: series.Len(), Digest: series.Digest(), Effective: applied.Tree, Uncaptured: uncaptured}, nil
}

// Uncaptured is the scratch-consistency function: the app files of the working
// tree that the working patch series does not reproduce. The broker publishes
// only when it is empty.
func Uncaptured(ctx context.Context, root string) ([]string, error) {
	report, err := Check(ctx, root)
	return report.Uncaptured, err
}

// List returns the working directory's series, in application order.
func List(ctx context.Context, root string) ([]Patch, error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return nil, err
	}
	defer c.close()
	files, err := c.dirSeries()
	if err != nil {
		return nil, err
	}
	series, err := ParseSeries(files)
	if err != nil {
		return nil, err
	}
	return series.Patches(), nil
}

// PublishedTree is the base tree with the working .sdo/deployment directory in
// place: what the broker would accept, as far as deployment inputs go. Its
// objects are written to the checkout. Rendering Effective of it shows what Go
// would deploy.
func PublishedTree(ctx context.Context, root string) (string, error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return "", err
	}
	defer c.close()
	view, err := c.loadView()
	if err != nil {
		return "", err
	}
	env := c.indexEnv("published-index", nil)
	if _, err := c.git(ctx, env, nil, "read-tree", view.BaseTree); err != nil {
		return "", err
	}
	deployment := filepath.ToSlash(filepath.Dir(Directory))
	if _, err := c.git(ctx, env, nil, "rm", "-r", "--cached", "--ignore-unmatch", "-q", "--", deployment); err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(c.root, filepath.FromSlash(deployment))); err == nil {
		if _, err := c.git(ctx, env, nil, "add", "-A", "-f", "--", deployment); err != nil {
			return "", err
		}
	}
	out, err := c.git(ctx, env, nil, "write-tree")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
