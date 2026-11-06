package patchset

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Format renders a patch file: the strict preamble, a blank line, the diff.
func Format(h Header, diff []byte) []byte {
	var b bytes.Buffer
	b.WriteString("SDO-Patch: 1\n")
	fmt.Fprintf(&b, "Owner: %s\n", h.Owner)
	reason := strings.TrimSpace(h.Reason)
	if strings.Contains(reason, "\n") || len(reason) > 100 {
		b.WriteString("Reason: |\n")
		for _, line := range strings.Split(reason, "\n") {
			if line = strings.TrimRight(line, " \t\r"); line == "" {
				b.WriteString("\n")
			} else {
				fmt.Fprintf(&b, "  %s\n", line)
			}
		}
	} else {
		fmt.Fprintf(&b, "Reason: %s\n", reason)
	}
	if h.Incident != "" {
		fmt.Fprintf(&b, "Incident: %s\n", h.Incident)
	}
	if h.UpstreamBase != "" {
		fmt.Fprintf(&b, "Upstream-Base: %s\n", h.UpstreamBase)
	}
	if h.UpstreamFix != "" {
		fmt.Fprintf(&b, "Upstream-Fix: %s\n", h.UpstreamFix)
	}
	b.WriteString("\n")
	b.Write(diff)
	return b.Bytes()
}

// CreateRequest asks for the scratch edits of Paths (all of them when empty) to
// be captured as the next patch of the working series.
type CreateRequest struct {
	Owner    Owner
	Incident string
	Reason   string
	// Name is the optional slug after the number; derived from Reason if empty.
	Name  string
	Paths []string
}

// Created describes a written patch.
type Created struct {
	Name  string
	Paths []string
	// Effective is the effective view after the patch.
	Effective string
}

var slugRun = regexp.MustCompile(`[^a-z0-9]+`)

var slugName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,48}$`)

func slugOf(reason string) string {
	words := strings.Fields(strings.ToLower(reason))
	if len(words) > 6 {
		words = words[:6]
	}
	slug := strings.Trim(slugRun.ReplaceAllString(strings.Join(words, "-"), "-"), "-")
	if len(slug) > 48 {
		slug = strings.Trim(slug[:48], "-")
	}
	return slug
}

// Create captures scratch edits as a patch. The diff is taken from what the
// working series produces (not from the base) to the working files of the
// chosen paths, written as the next number with the caller as owner, and only
// kept if the extended series applies to the base and yields exactly those
// files. A failed create writes nothing. On success the checkout's recorded
// view moves to the new effective tree, so the captured edits stop being
// scratch.
func Create(ctx context.Context, root string, req CreateRequest) (Created, error) {
	c, err := openCheckout(ctx, root)
	if err != nil {
		return Created{}, err
	}
	defer c.close()
	view, err := c.loadView()
	if err != nil {
		return Created{}, err
	}
	files, err := c.dirSeries()
	if err != nil {
		return Created{}, err
	}
	current, err := ParseSeries(files)
	if err != nil {
		return Created{}, err
	}
	fresh, err := Apply(ctx, Store{Dir: c.root}, view.BaseTree, current)
	if err != nil {
		return Created{}, err
	}
	defer fresh.Close()
	scratch, err := c.scratchPaths(ctx, fresh.Env(), fresh.Tree)
	if err != nil {
		return Created{}, err
	}
	if len(scratch) == 0 {
		return Created{}, errors.New("no scratch edits to capture: the app files already match what the patches produce")
	}
	chosen, err := choosePaths(scratch, req.Paths)
	if err != nil {
		return Created{}, err
	}
	name, err := nextName(files, req)
	if err != nil {
		return Created{}, err
	}
	// Index = effective view + the chosen working files; its diff is the patch.
	env := c.indexEnv("create-index", fresh.Env())
	env = append(env, "GIT_LITERAL_PATHSPECS=1")
	if _, err := c.git(ctx, env, nil, "read-tree", fresh.Tree); err != nil {
		return Created{}, err
	}
	var specs bytes.Buffer
	for _, p := range chosen {
		specs.WriteString(p)
		specs.WriteByte(0)
	}
	if _, err := c.git(ctx, env, specs.Bytes(), "add", "-A", "--pathspec-from-file=-", "--pathspec-file-nul"); err != nil {
		return Created{}, err
	}
	out, err := c.git(ctx, env, nil, "write-tree")
	if err != nil {
		return Created{}, err
	}
	want := strings.TrimSpace(string(out))
	diff, err := c.git(ctx, env, nil, "diff", "--cached", "--no-color", "--full-index", "--no-ext-diff", "--no-textconv", "--no-renames",
		"--src-prefix=a/", "--dst-prefix=b/", "-U3", "--inter-hunk-context=0", fresh.Tree)
	if err != nil {
		return Created{}, err
	}
	if len(bytes.TrimSpace(diff)) == 0 {
		return Created{}, errors.New("the chosen paths produce an empty diff")
	}
	raw := Format(Header{Owner: req.Owner, Reason: req.Reason, Incident: req.Incident}, diff)
	if _, err := Parse(name, raw); err != nil {
		return Created{}, err
	}
	extended, err := ParseSeries(append(append([]File(nil), files...), File{Name: name, Data: raw}))
	if err != nil {
		return Created{}, err
	}
	applied, err := Apply(ctx, Store{Dir: c.root}, view.BaseTree, extended)
	if err != nil {
		return Created{}, err
	}
	defer applied.Close()
	if applied.Tree != want {
		return Created{}, fmt.Errorf("patch %s does not reproduce the working files: series gives tree %s, working files give %s", name, applied.Tree, want)
	}
	changes, err := c.diffRaw(ctx, applied.Env(), view.BaseTree, applied.Tree)
	if err != nil {
		return Created{}, err
	}
	if err := c.loadBlobs(ctx, applied.Env(), changes); err != nil {
		return Created{}, err
	}
	if err := c.persist(ctx, view.BaseTree, applied.Tree, changes); err != nil {
		return Created{}, err
	}
	dir := filepath.Join(c.root, filepath.FromSlash(Directory))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Created{}, err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return Created{}, err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(f.Name())
		return Created{}, err
	}
	if err := f.Close(); err != nil {
		return Created{}, err
	}
	view.EffectiveTree = applied.Tree
	if err := c.saveView(view); err != nil {
		return Created{}, err
	}
	return Created{Name: name, Paths: chosen, Effective: applied.Tree}, nil
}

// choosePaths selects the scratch files named by requested (files or
// directories); none selects all. A request that matches nothing is an error.
func choosePaths(scratch, requested []string) ([]string, error) {
	if len(requested) == 0 {
		return scratch, nil
	}
	picked := map[string]bool{}
	for _, r := range requested {
		clean := path.Clean(filepath.ToSlash(r))
		if clean == "." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) {
			return nil, fmt.Errorf("path %q must be a relative path inside the repository", r)
		}
		if clean == ".sdo" || strings.HasPrefix(clean, ".sdo/") {
			return nil, fmt.Errorf("path %q is under .sdo/, which patches never contain", r)
		}
		matched := false
		for _, s := range scratch {
			if s == clean || strings.HasPrefix(s, clean+"/") {
				picked[s] = true
				matched = true
			}
		}
		if !matched {
			return nil, fmt.Errorf("path %q has no scratch edit to capture (edited paths: %s)", r, strings.Join(scratch, ", "))
		}
	}
	var out []string
	for _, s := range scratch {
		if picked[s] {
			out = append(out, s)
		}
	}
	return out, nil
}

// nextName numbers the new patch one above the highest existing number.
func nextName(files []File, req CreateRequest) (string, error) {
	highest := 0
	for _, f := range files {
		if m := patchName.FindStringSubmatch(f.Name); m != nil {
			n, _ := strconv.Atoi(m[1])
			highest = max(highest, n)
		}
	}
	if highest >= 9999 {
		return "", errors.New("patch numbers are exhausted")
	}
	slug := req.Name
	if slug == "" {
		slug = slugOf(req.Reason)
	}
	if !slugName.MatchString(slug) {
		return "", fmt.Errorf("patch name %q must be lowercase letters, digits and dashes (derive one from the reason or pass --name)", slug)
	}
	return fmt.Sprintf("%04d-%s.patch", highest+1, slug), nil
}
