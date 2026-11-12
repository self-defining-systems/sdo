package broker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/patchset"
)

// Limits of one source publication (the broker's squash of an agent's commits).
const (
	maxSourceFileBytes  = 256 << 10
	maxSourceTotalBytes = 2 << 20
	maxSourceMessage    = 2000
)

// SourceChange is the application-source part of an agent's checkout. An agent
// changes source by committing it; everything else it leaves in the working
// tree is scratch that nothing publishes or deploys.
//
// A checkout's working files start as the effective view (the accepted base
// plus its deployment patches) while HEAD is still the base commit, so a
// commit of those materialized files adds nothing. Kept is therefore measured
// from the view, not from the base: the paths where HEAD's tree differs from
// the view's effective tree.
type SourceChange struct {
	// Kept are the committed app paths, sorted: what the publication carries.
	Kept []string
	// Scratch are the app paths whose working content differs from what HEAD
	// holds (or from the view, when nothing was committed): untracked,
	// unstaged and uncommitted edits. Ignored files are invisible.
	Scratch []string
	// Subjects are the subject lines of the agent's commits since the base,
	// oldest first.
	Subjects []string

	entries map[string]treeEntry
	// effective is the checkout's view tree: what a diff of the kept content
	// is measured from.
	effective string
	// overlap are the kept paths a deployment patch also changes.
	overlap []string
}

type treeEntry struct {
	status byte // 'A', 'M', 'D'
	mode   string
	sha    string // the new blob; zeros for a deletion
}

// SourceChangeOf reads a proposal checkout. A HEAD that does not descend from
// the recorded base (a reset or amend below it) is a source.history rejection.
func SourceChangeOf(ctx context.Context, checkout, baseCommit string) (SourceChange, error) {
	var change SourceChange
	if _, err := runGit(ctx, checkout, "merge-base", "--is-ancestor", baseCommit, "HEAD"); err != nil {
		if ctx.Err() != nil {
			return change, ctx.Err()
		}
		return change, validate.Err([]validate.Diagnostic{{
			Rule:    validate.RuleSourceHistory,
			Message: "HEAD does not descend from the checkout's base commit " + baseCommit,
			Hint:    "commit on top of the checkout's starting commit; do not reset, rebase or amend below it",
		}})
	}
	view, viewed, err := patchset.LoadView(ctx, checkout)
	if err != nil {
		return change, err
	}
	if !viewed {
		tree, err := gitText(ctx, checkout, "rev-parse", baseCommit+"^{tree}")
		if err != nil {
			return change, err
		}
		view = patchset.View{BaseTree: tree, EffectiveTree: tree}
	}
	headTree, err := gitText(ctx, checkout, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return change, err
	}
	change.effective = view.EffectiveTree
	// What HEAD changed relative to the base, and what it changed relative to
	// the view the agent saw. A commit of only the materialized files is the
	// first without the second; the kept change is the intersection.
	fromBase, err := appTreeDelta(ctx, checkout, view.BaseTree, headTree)
	if err != nil {
		return change, err
	}
	fromView, err := appTreeDelta(ctx, checkout, view.EffectiveTree, headTree)
	if err != nil {
		return change, err
	}
	change.entries = map[string]treeEntry{}
	for p := range fromBase {
		if entry, differs := fromView[p]; differs {
			change.entries[p] = entry
			change.Kept = append(change.Kept, p)
		}
	}
	sort.Strings(change.Kept)
	if len(change.Kept) > 0 {
		patched, err := appTreeDelta(ctx, checkout, view.BaseTree, view.EffectiveTree)
		if err != nil {
			return change, err
		}
		for _, p := range change.Kept {
			if _, also := patched[p]; also {
				change.overlap = append(change.overlap, p)
			}
		}
	}
	if change.Scratch, err = scratchAgainst(ctx, checkout, view.EffectiveTree, fromBase); err != nil {
		return change, err
	}
	subjects, err := runGit(ctx, checkout, "log", "--reverse", "--no-merges", "--format=%s", baseCommit+"..HEAD")
	if err != nil {
		return change, err
	}
	for _, line := range strings.Split(string(subjects), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			change.Subjects = append(change.Subjects, line)
		}
	}
	return change, nil
}

// appTreeDelta lists the non-.sdo/ paths that differ between two trees of the
// checkout's object store.
func appTreeDelta(ctx context.Context, checkout, from, to string) (map[string]treeEntry, error) {
	out, err := runGit(ctx, checkout, "diff-tree", "-r", "-z", "--no-renames", "--no-abbrev", "--raw", from, to)
	if err != nil {
		return nil, err
	}
	entries := map[string]treeEntry{}
	parts := strings.Split(string(out), "\x00")
	for i := 0; i+1 < len(parts); i += 2 {
		meta := strings.Fields(strings.TrimPrefix(parts[i], ":"))
		if len(meta) != 5 {
			return nil, fmt.Errorf("unexpected diff-tree record %q", parts[i])
		}
		if layout.IsMemoryPath(parts[i+1]) {
			continue
		}
		entries[parts[i+1]] = treeEntry{status: meta[4][0], mode: meta[1], sha: meta[3]}
	}
	return entries, nil
}

// scratchAgainst lists the app paths of the working tree that differ from the
// reference: the view tree with the paths HEAD changed replaced by HEAD's
// entries. It uses a scratch index, so the checkout's own index is untouched.
func scratchAgainst(ctx context.Context, checkout, view string, committed map[string]treeEntry) ([]string, error) {
	indexDir, err := os.MkdirTemp("", "sdo-scratch-index-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(indexDir)
	env := []string{"GIT_INDEX_FILE=" + filepath.Join(indexDir, "index")}
	if _, err := runGitEnv(ctx, checkout, env, "read-tree", view); err != nil {
		return nil, err
	}
	for p, entry := range committed {
		var err error
		if entry.status == 'D' {
			_, err = runGitEnv(ctx, checkout, env, "update-index", "--force-remove", "--", p)
		} else {
			_, err = runGitEnv(ctx, checkout, env, "update-index", "--add", "--cacheinfo", entry.mode+","+entry.sha+","+p)
		}
		if err != nil {
			return nil, err
		}
	}
	referenceOut, err := runGitEnv(ctx, checkout, env, "write-tree")
	if err != nil {
		return nil, err
	}
	reference := strings.TrimSpace(string(referenceOut))
	if _, err := runGitEnv(ctx, checkout, env, "add", "-A", "--", ".", ":(exclude)"+layout.Dir); err != nil {
		return nil, err
	}
	out, err := runGitEnv(ctx, checkout, env, "diff-index", "--cached", "--name-only", "-z", "--no-renames", reference, "--")
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, p := range strings.Split(string(out), "\x00") {
		if p != "" && !layout.IsMemoryPath(p) {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// sourceDiagnostics are the source.policy and source.size rules over the kept
// paths of one publication, evaluated on the content HEAD holds.
func sourceDiagnostics(ctx context.Context, checkout string, change SourceChange) ([]validate.Diagnostic, error) {
	var diagnostics []validate.Diagnostic
	for _, p := range change.overlap {
		diagnostics = append(diagnostics, validate.Diagnostic{
			Rule: validate.RuleSourcePolicy, Path: p,
			Message: "the committed change touches a file that a deployment patch also changes",
			Hint:    "revert this file and capture the change as a patch (`sdo-operator patch create --reason \"<why>\"`); the patch series is applied on top of committed source",
		})
	}
	total := 0
	for _, p := range change.Kept {
		entry := change.entries[p]
		if entry.status == 'D' {
			continue
		}
		switch entry.mode {
		case "100644", "100755":
		default:
			diagnostics = append(diagnostics, validate.Diagnostic{
				Rule: validate.RuleSourcePolicy, Path: p,
				Message: fmt.Sprintf("mode %s is not allowed: only regular files (100644, 100755) are published", entry.mode),
				Hint:    "no symlinks, submodules or gitlinks; commit the file's content instead",
			})
			continue
		}
		size, err := gitText(ctx, checkout, "cat-file", "-s", "HEAD:"+p)
		if err != nil {
			return nil, err
		}
		var n int
		if _, err := fmt.Sscanf(size, "%d", &n); err != nil {
			return nil, fmt.Errorf("size of %s: %w", p, err)
		}
		total += n
		if n > maxSourceFileBytes {
			diagnostics = append(diagnostics, validate.Diagnostic{
				Rule: validate.RuleSourceSize, Path: p,
				Message: fmt.Sprintf("%d bytes exceeds the %d byte limit per file", n, maxSourceFileBytes),
				Hint:    "a source repair changes source; do not commit generated or binary artifacts",
			})
		}
	}
	if total > maxSourceTotalBytes {
		diagnostics = append(diagnostics, validate.Diagnostic{
			Rule:    validate.RuleSourceSize,
			Message: fmt.Sprintf("%d bytes of changed files exceed the %d byte limit per publication", total, maxSourceTotalBytes),
			Hint:    "commit only the files the repair needs",
		})
	}
	return diagnostics, nil
}

var (
	yamlBlockKey = regexp.MustCompile(`^(\s*)(data|stringData):\s*$`)
	yamlKeyValue = regexp.MustCompile(`^\s*([^\s#:][^:]*):\s*(\S.*)$`)
	yamlDocKind  = regexp.MustCompile(`^kind:\s*["']?Secret["']?\s*$`)
)

// secretAdditions reports the added lines of a source file that place a value
// inside a Kubernetes Secret's data or stringData block. The rule reads added
// lines, not the whole file: an application repository may hold a Secret
// manifest with placeholder data, and an edit elsewhere in it is not a Secret
// change. A templated value ({{ }}, ${ }) is a reference, not a value.
func secretAdditions(content []byte, added map[int]bool) []int {
	var hits []int
	line := 0
	var doc [][]byte
	var docStart int
	flush := func() {
		isSecret := false
		for _, l := range doc {
			if yamlDocKind.Match(bytes.TrimRight(l, "\r")) {
				isSecret = true
				break
			}
		}
		if !isSecret {
			return
		}
		blockIndent := -1
		for i, raw := range doc {
			text := strings.TrimRight(string(raw), "\r")
			trimmed := strings.TrimSpace(text)
			indent := len(text) - len(strings.TrimLeft(text, " \t"))
			if blockIndent >= 0 && trimmed != "" && indent <= blockIndent {
				blockIndent = -1
			}
			if m := yamlBlockKey.FindStringSubmatch(text); m != nil {
				blockIndent = len(m[1])
				continue
			}
			if blockIndent < 0 || !added[docStart+i] {
				continue
			}
			if m := yamlKeyValue.FindStringSubmatch(text); m != nil && !isTemplated(m[2]) {
				hits = append(hits, docStart+i)
			}
		}
	}
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	docStart = 1
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if strings.HasPrefix(text, "---") {
			flush()
			doc, docStart = nil, line+1
			continue
		}
		doc = append(doc, []byte(text))
	}
	flush()
	return hits
}

func isTemplated(value string) bool {
	return strings.Contains(value, "{{") || strings.Contains(value, "${") || strings.HasPrefix(strings.TrimSpace(value), "<") || strings.HasPrefix(strings.TrimSpace(value), "$(")
}

// addedLines reads the new-side line numbers a unified diff adds.
func addedLines(diff []byte) map[int]bool {
	added := map[int]bool{}
	next := 0
	for _, text := range strings.Split(string(diff), "\n") {
		switch {
		case strings.HasPrefix(text, "@@"):
			var oldStart, oldLen, newStart, newLen int
			if n, _ := fmt.Sscanf(text, "@@ -%d,%d +%d,%d @@", &oldStart, &oldLen, &newStart, &newLen); n < 3 {
				if n2, _ := fmt.Sscanf(text, "@@ -%d +%d,%d @@", &oldStart, &newStart, &newLen); n2 < 2 {
					fmt.Sscanf(text, "@@ -%d +%d @@", &oldStart, &newStart)
				}
			}
			next = newStart
		case strings.HasPrefix(text, "+++") || strings.HasPrefix(text, "---"):
		case strings.HasPrefix(text, "+"):
			added[next] = true
			next++
		case strings.HasPrefix(text, "-"):
		case strings.HasPrefix(text, " "):
			next++
		}
	}
	return added
}

// sourceSecretDiagnostics applies memory.secret-data to the added lines of
// the kept YAML files, and memory.secret-material to the added text of every
// kept file. The base content comes from the recorded base commit.
func sourceSecretDiagnostics(ctx context.Context, checkout, baseCommit string, change SourceChange) ([]validate.Diagnostic, error) {
	var diagnostics []validate.Diagnostic
	for _, p := range change.Kept {
		if change.entries[p].status == 'D' {
			continue
		}
		content, err := runGit(ctx, checkout, "show", "HEAD:"+p)
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(content) {
			continue
		}
		diff, err := runGit(ctx, checkout, "-c", "diff.external=", "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "-U0", change.effective, "HEAD^{tree}", "--", p)
		if err != nil {
			return nil, err
		}
		added := addedLines(diff)
		if len(added) == 0 {
			continue
		}
		if ext := strings.ToLower(filepath.Ext(p)); ext == ".yaml" || ext == ".yml" {
			if hits := secretAdditions(content, added); len(hits) > 0 {
				diagnostics = append(diagnostics, validate.Diagnostic{
					Rule: validate.RuleSecretData, Path: p, Line: hits[0],
					Message: "the change adds Kubernetes Secret data",
					Hint:    "never commit Secret values; reference the Secret by name or template the value",
				})
				continue
			}
		}
		if material := validate.SecretMaterialIn(p, addedText(content, added)); material != "" {
			diagnostics = append(diagnostics, validate.Diagnostic{
				Rule: validate.RuleSecretMaterial, Path: p,
				Message: "the change adds credential-looking material: " + material,
				Hint:    "never commit credentials; reference a Secret by name or template the value",
			})
		}
	}
	return diagnostics, nil
}

func addedText(content []byte, added map[int]bool) []byte {
	var out bytes.Buffer
	line := 0
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for scanner.Scan() {
		line++
		if added[line] {
			out.WriteString(scanner.Text())
			out.WriteByte('\n')
		}
	}
	return out.Bytes()
}

// copyCommitted writes the content HEAD holds for path into targetRoot, or
// removes it there when HEAD deleted it. The working file is never read: only
// what the agent committed is kept.
func copyCommitted(ctx context.Context, checkout, targetRoot, relative string, entry treeEntry) error {
	target := filepath.Join(targetRoot, relative)
	if err := safeParents(targetRoot, target); err != nil {
		return err
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return err
	}
	if entry.status == 'D' {
		return nil
	}
	data, err := runGit(ctx, checkout, "cat-file", "blob", "HEAD:"+relative)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if entry.mode == "100755" {
		mode = 0o755
	}
	return os.WriteFile(target, data, mode)
}

// squashBody is the commit-message body of a source publication: the subjects
// of the agent's commits, with every line that could pass for a broker trailer
// removed, capped in runes.
func squashBody(subjects []string) string {
	var lines []string
	for _, subject := range subjects {
		subject = strings.Join(strings.Fields(subject), " ")
		if subject == "" || strings.HasPrefix(strings.ToLower(subject), "sdo-") {
			continue
		}
		lines = append(lines, "- "+subject)
	}
	body := strings.Join(lines, "\n")
	if utf8.RuneCountInString(body) > maxSourceMessage {
		runes := []rune(body)
		body = string(runes[:maxSourceMessage])
	}
	return body
}

// requireSourceRules rejects a source publication that breaks source.policy,
// source.size or the Secret rules, before anything is staged. The agent's
// commits are squashed into one publication, so the rules read the final
// content once; a value one commit adds and a later commit removes is never
// in HEAD, so it never reaches the branch.
func (b *Broker) requireSourceRules(ctx context.Context, checkout, baseCommit string, change SourceChange) error {
	diagnostics, err := sourceDiagnostics(ctx, checkout, change)
	if err != nil {
		return err
	}
	secrets, err := sourceSecretDiagnostics(ctx, checkout, baseCommit, change)
	if err != nil {
		return err
	}
	return validate.Err(append(diagnostics, secrets...))
}

// CleanupMessage is the text the closing turn sends when app files are left
// uncommitted: the paths, and what to do with them. "" when there are none.
func CleanupMessage(scratch []string) string {
	if len(scratch) == 0 {
		return ""
	}
	const shown = 20
	listed := scratch
	if len(listed) > shown {
		listed = listed[:shown]
	}
	more := ""
	if len(scratch) > shown {
		more = fmt.Sprintf(" (and %d more)", len(scratch)-shown)
	}
	return fmt.Sprintf("%d app file(s) are not committed: %s%s. Commit (`git commit -m \"<why>\"`) each change that must survive the next deploy; "+
		"discard the rest (`git checkout -- <path>`, or delete the file). Uncommitted files are never published or deployed.", len(scratch), strings.Join(listed, ", "), more)
}

// SourceChange reads the responder's proposal checkout for an incident.
func (b *Broker) SourceChange(ctx context.Context, incidentID string, phase Phase) (SourceChange, error) {
	proposal, err := b.proposal(incidentID, phase)
	if err != nil {
		return SourceChange{}, err
	}
	return SourceChangeOf(ctx, proposal.Path, proposal.BaseCommit)
}
