package layout

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Schema 15: .sdo/deployment holds only the optional override plan.yaml.
// Deployment files are ordinary committed repository files, so the retired
// views move out of memory, the retired plan fields go, patches are applied to
// the files they change, and the telemetry overlay slot (never rendered) goes.

// retiredDeploymentViews are the .sdo/deployment directories that held authored
// deployment files, moved to deploy/sdo/<dir>.
var retiredDeploymentViews = []string{"manifests", "build", "values"}

// MovedDeploymentRoot is where the retired views' files live after schema 15.
const MovedDeploymentRoot = "deploy/sdo"

// retiredPlanFields are the DeploymentPlan fields schema 15 drops from plan.yaml.
var retiredPlanFields = []string{"expectedWorkloadIds", "strategyId", "strategyConfig"}

// deploymentMigration is the planned schema-15 change: files to write
// (repository-relative), and memory paths to remove, computed before the
// checkout is touched.
type deploymentMigration struct {
	writes  map[string]fileWrite
	removes []string
}

type fileWrite struct {
	data []byte
	mode fs.FileMode
}

// planDeploymentMigration plans schema 15 for the checkout at root. A target
// file that already exists, a patch that does not apply exactly, or a plan that
// does not decode refuses the migration with what to do.
func planDeploymentMigration(root string) (deploymentMigration, error) {
	m := deploymentMigration{writes: map[string]fileWrite{}}
	moved := map[string]string{} // old prefix -> new prefix
	for _, dir := range retiredDeploymentViews {
		from := DeploymentDir + "/" + dir
		to := MovedDeploymentRoot + "/" + dir
		files, err := regularFiles(root, from)
		if err != nil {
			return m, err
		}
		if len(files) == 0 {
			continue
		}
		moved[from] = to
		for _, rel := range files {
			target := to + strings.TrimPrefix(rel, from)
			if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(target))); err == nil {
				return m, fmt.Errorf("%s moves to %s, which already exists: move or merge it by hand and commit, then migrate again", rel, target)
			}
			data, mode, err := readRegularFile(root, rel)
			if err != nil {
				return m, err
			}
			m.writes[target] = fileWrite{data: data, mode: mode}
		}
		m.removes = append(m.removes, from)
	}
	if err := m.planPatches(root); err != nil {
		return m, err
	}
	if err := m.planOverride(root, moved); err != nil {
		return m, err
	}
	overlays := DeploymentDir + "/overlays"
	if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(overlays))); err == nil {
		m.removes = append(m.removes, overlays)
	}
	return m, nil
}

// planOverride drops the retired fields of plan.yaml and rewrites every path
// into a moved view to its new place.
func (m *deploymentMigration) planOverride(root string, moved map[string]string) error {
	data, mode, err := readRegularFile(root, DeploymentPlanFile)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var plan map[string]any
	if err := yaml.Unmarshal(data, &plan); err != nil || plan == nil {
		return fmt.Errorf("%s does not decode (%v): fix or delete it and commit, then migrate again", DeploymentPlanFile, err)
	}
	changed := false
	for _, field := range retiredPlanFields {
		if _, ok := plan[field]; ok {
			delete(plan, field)
			changed = true
		}
	}
	rewritten := rewriteMovedPaths(plan, moved, &changed)
	if !changed {
		return nil
	}
	encoded, err := json.Marshal(rewritten)
	if err != nil {
		return fmt.Errorf("encode %s: %w", DeploymentPlanFile, err)
	}
	m.writes[DeploymentPlanFile] = fileWrite{data: append(encoded, '\n'), mode: mode}
	return nil
}

func rewriteMovedPaths(value any, moved map[string]string, changed *bool) any {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			v[key] = rewriteMovedPaths(item, moved, changed)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = rewriteMovedPaths(item, moved, changed)
		}
		return v
	case string:
		for from, to := range moved {
			if v == from || strings.HasPrefix(v, from+"/") {
				*changed = true
				return to + strings.TrimPrefix(v, from)
			}
		}
	}
	return value
}

// planPatches applies .sdo/deployment/patches in name order to the files they
// change, with exact context, and removes the directory.
func (m *deploymentMigration) planPatches(root string) error {
	dir := DeploymentDir + "/patches"
	names, err := regularFiles(root, dir)
	if err != nil || len(names) == 0 {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		data, _, err := readRegularFile(root, name)
		if err != nil {
			return err
		}
		if err := m.applyPatch(root, string(data)); err != nil {
			return fmt.Errorf("%s does not apply to the accepted tree (%v): apply or drop it by hand (`git apply`), commit, delete %s, then migrate again", name, err, dir)
		}
	}
	m.removes = append(m.removes, dir)
	return nil
}

// applyPatch applies the `git diff` body of one patch file to the planned
// writes (or the checkout's files).
func (m *deploymentMigration) applyPatch(root, text string) error {
	start := strings.Index(text, "diff --git ")
	if start < 0 {
		return errors.New("no diff")
	}
	for _, section := range strings.Split(text[start:], "\ndiff --git ") {
		section = strings.TrimPrefix(section, "diff --git ")
		lines := strings.Split(section, "\n")
		var oldPath, newPath string
		i := 1
		for ; i < len(lines) && !strings.HasPrefix(lines[i], "@@"); i++ {
			switch {
			case strings.HasPrefix(lines[i], "--- "):
				oldPath = strings.TrimPrefix(strings.TrimPrefix(lines[i], "--- "), "a/")
			case strings.HasPrefix(lines[i], "+++ "):
				newPath = strings.TrimPrefix(strings.TrimPrefix(lines[i], "+++ "), "b/")
			}
		}
		if newPath == "" {
			return fmt.Errorf("a diff section without file names")
		}
		target := newPath
		if target == "/dev/null" {
			target = oldPath
		}
		if IsMemoryPath(newPath) || IsMemoryPath(oldPath) || !fs.ValidPath(target) {
			return fmt.Errorf("a patch may change only application files, not %s", newPath)
		}
		var current []string
		mode := fs.FileMode(0o644)
		if oldPath != "/dev/null" {
			data, fileMode, err := m.read(root, oldPath)
			if err != nil {
				return err
			}
			mode = fileMode
			current = splitLines(string(data))
		}
		result, err := applyHunks(current, lines[i:])
		if err != nil {
			return fmt.Errorf("%s: %w", newPath, err)
		}
		if newPath == "/dev/null" {
			m.writes[oldPath] = fileWrite{data: nil, mode: 0}
			continue
		}
		m.writes[newPath] = fileWrite{data: []byte(strings.Join(result, "")), mode: mode}
	}
	return nil
}

func (m *deploymentMigration) read(root, rel string) ([]byte, fs.FileMode, error) {
	if w, ok := m.writes[rel]; ok {
		if w.data == nil && w.mode == 0 {
			return nil, 0, fmt.Errorf("%s was deleted by an earlier patch", rel)
		}
		return w.data, w.mode, nil
	}
	return readRegularFile(root, rel)
}

// splitLines splits keeping each line's newline.
func splitLines(text string) []string {
	var out []string
	for len(text) > 0 {
		i := strings.IndexByte(text, '\n')
		if i < 0 {
			out = append(out, text)
			break
		}
		out = append(out, text[:i+1])
		text = text[i+1:]
	}
	return out
}

// applyHunks applies unified-diff hunks with exact context at their stated
// line numbers.
func applyHunks(current []string, body []string) ([]string, error) {
	var out []string
	next := 0 // index into current
	for i := 0; i < len(body); {
		header := body[i]
		if header == "" || strings.HasPrefix(header, "\\") {
			i++
			continue
		}
		if !strings.HasPrefix(header, "@@ -") {
			return nil, fmt.Errorf("unexpected diff line %q", header)
		}
		oldStart, err := hunkStart(header)
		if err != nil {
			return nil, err
		}
		at := oldStart - 1
		if oldStart == 0 {
			at = 0
		}
		if at < next || at > len(current) {
			return nil, fmt.Errorf("hunk %q is out of range", header)
		}
		out = append(out, current[next:at]...)
		next = at
		i++
		for ; i < len(body) && !strings.HasPrefix(body[i], "@@"); i++ {
			line := body[i]
			noNewline := i+1 < len(body) && strings.HasPrefix(body[i+1], "\\ No newline")
			text := ""
			if len(line) > 0 {
				text = line[1:]
			}
			if !noNewline {
				text += "\n"
			}
			switch {
			case line == "":
			case strings.HasPrefix(line, " "):
				if next >= len(current) || current[next] != text {
					return nil, fmt.Errorf("context does not match at line %d", next+1)
				}
				out = append(out, text)
				next++
			case strings.HasPrefix(line, "-"):
				if next >= len(current) || current[next] != text {
					return nil, fmt.Errorf("removed line does not match at line %d", next+1)
				}
				next++
			case strings.HasPrefix(line, "+"):
				out = append(out, text)
			case strings.HasPrefix(line, "\\"):
			}
		}
	}
	return append(out, current[next:]...), nil
}

func hunkStart(header string) (int, error) {
	rest := strings.TrimPrefix(header, "@@ -")
	end := strings.IndexAny(rest, ", ")
	if end < 0 {
		return 0, fmt.Errorf("malformed hunk header %q", header)
	}
	return strconv.Atoi(rest[:end])
}

// regularFiles lists the regular files below dir (repository-relative,
// slash-separated, sorted); a missing dir has none.
func regularFiles(root, dir string) ([]string, error) {
	base := filepath.Join(root, filepath.FromSlash(dir))
	if _, err := os.Lstat(base); errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	var files []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", p)
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(files)
	return files, err
}

func readRegularFile(root, rel string) ([]byte, fs.FileMode, error) {
	full := filepath.Join(root, filepath.FromSlash(path.Clean(rel)))
	info, err := os.Lstat(full)
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("%s is not a regular file", rel)
	}
	data, err := os.ReadFile(full)
	return data, info.Mode().Perm(), err
}

// apply writes the planned files and removes the retired memory paths.
func (m deploymentMigration) apply(root string) error {
	targets := make([]string, 0, len(m.writes))
	for rel := range m.writes {
		targets = append(targets, rel)
	}
	sort.Strings(targets)
	for _, rel := range targets {
		w := m.writes[rel]
		full := filepath.Join(root, filepath.FromSlash(rel))
		if w.data == nil && w.mode == 0 {
			if err := os.Remove(full); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, w.data, w.mode); err != nil {
			return err
		}
	}
	for _, rel := range m.removes {
		if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
			return err
		}
	}
	return nil
}
