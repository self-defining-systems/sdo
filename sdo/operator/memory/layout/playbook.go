package layout

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// HealthObjectivePlaybook is the health judge's playbook ID. Its fault class
// differs from its ID.
const HealthObjectivePlaybook = "health-objective"

// PlaybookFaultClass is the fault_class a playbook must declare: the playbook
// ID, except for the health-objective playbook.
func PlaybookFaultClass(id string) string {
	if id == HealthObjectivePlaybook {
		return "health-objective-failure"
	}
	return id
}

// PlaybookScriptPath is the repository-relative path of a playbook script.
func PlaybookScriptPath(playbookID, name string) string {
	return path.Join(PlaybookDir(playbookID), PlaybookScriptsDir, name)
}

// WritePlaybook stores a playbook's document and scripts below root. Scripts
// are executable. The playbook index is generated; see GeneratePlaybookIndex.
func WritePlaybook(root string, playbook Playbook) error {
	readme, err := PlaybookPath(playbook.ID)
	if err != nil {
		return err
	}
	if err := writeMemoryFile(root, readme, []byte(playbook.Document.Source), 0o644); err != nil {
		return err
	}
	for _, script := range playbook.Scripts {
		if script.Name == "" || strings.Contains(script.Name, "/") || !strings.HasSuffix(script.Name, ".sh") {
			return fmt.Errorf("invalid playbook script name %q", script.Name)
		}
		if err := writeMemoryFile(root, PlaybookScriptPath(playbook.ID, script.Name), script.Content, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// GeneratePlaybookIndex renders the playbook index from the playbook
// directories below root: one line per active playbook, sorted by ID, titled
// by the README's first heading (the ID when it has none). It returns nil when
// memory holds no playbook README. The index is derived, never authored.
func GeneratePlaybookIndex(root string) ([]byte, error) {
	entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(PlaybooksDir)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var lines []string
	found := false
	for _, entry := range entries {
		id := entry.Name()
		if !entry.IsDir() || !ValidPlaybookID(id) {
			continue
		}
		readme := filepath.Join(root, filepath.FromSlash(PlaybookDir(id)), PlaybookFileName)
		info, err := os.Lstat(readme)
		if errors.Is(err, fs.ErrNotExist) || err == nil && !info.Mode().IsRegular() {
			continue
		}
		if err != nil {
			return nil, err
		}
		data, err := readCapped(readme)
		if err != nil {
			return nil, err
		}
		found = true
		// Malformed lifecycle metadata stays listed; document validation reports it.
		if retired, err := ParsePlaybookRetirement(data); err == nil && retired {
			continue
		}
		lines = append(lines, fmt.Sprintf("- [%s](%s/%s)\n", indexTitle(data, id), id, PlaybookFileName))
	}
	if !found {
		return nil, nil
	}
	index := "# Playbooks\n"
	if len(lines) > 0 {
		index += "\n" + strings.Join(lines, "")
	}
	return []byte(index), nil
}

// WritePlaybookIndex replaces the playbook index with its generated form and
// reports whether the file changed.
func WritePlaybookIndex(root string) (bool, error) {
	generated, err := GeneratePlaybookIndex(root)
	if err != nil || generated == nil {
		return false, err
	}
	existing, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(PlaybookIndexFile)))
	if err == nil && string(existing) == string(generated) {
		return false, nil
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	return true, writeMemoryFile(root, PlaybookIndexFile, generated, 0o644)
}

var indexTitleEscaper = strings.NewReplacer(`\`, `\\`, `[`, `\[`, `]`, `\]`)

func indexTitle(readme []byte, id string) string {
	text := string(readme)
	if rest, ok := strings.CutPrefix(text, "---\n"); ok {
		if _, body, closed := strings.Cut(rest, "---\n"); closed {
			text = body
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if title, ok := strings.CutPrefix(line, "# "); ok {
			if title = strings.TrimSpace(title); title != "" {
				return indexTitleEscaper.Replace(title)
			}
			break
		}
	}
	return id
}

func readCapped(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, MaxArtifactBytes))
}

func writeMemoryFile(root, rel string, content []byte, mode fs.FileMode) error {
	target := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(target, content, mode); err != nil {
		return err
	}
	return os.Chmod(target, mode)
}

// PlaybookRetired validates optional lifecycle metadata. An omitted status is
// active. A retired reason is preserved in history, never offered as advice.
func PlaybookRetired(doc Document) (bool, error) {
	value, present := doc.Meta["status"]
	status := "active"
	if present {
		var ok bool
		status, ok = value.(string)
		if !ok || (status != "active" && status != "retired") {
			return false, errors.New("playbook status must be active or retired")
		}
	}
	reason, hasReason := doc.Meta["retired_reason"]
	if status == "retired" {
		text, ok := reason.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return false, errors.New("retired playbook requires a nonempty string retired_reason")
		}
		return true, nil
	}
	if hasReason {
		return false, errors.New("retired_reason requires status: retired")
	}
	return false, nil
}

// ReadPlaybookRetired reads the lifecycle marker before program discovery or
// staging; malformed metadata fails closed rather than reactivating a program.
func ReadPlaybookRetired(directory string) (bool, error) {
	filename := filepath.Join(directory, PlaybookFileName)
	info, err := os.Lstat(filename)
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("playbook README must be a regular file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxArtifactBytes))
	if err != nil {
		return false, err
	}
	return ParsePlaybookRetirement(data)
}

// ParsePlaybookRetirement reads only lifecycle metadata. Full schema/body/size
// checks belong to document validation; old compiler fixtures can be prose-only
// or have program front matter without the document schema.
func ParsePlaybookRetirement(data []byte) (bool, error) {
	if !strings.HasPrefix(string(data), "---\n") {
		return false, nil
	}
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 {
		return false, errors.New("playbook front matter is not closed within the metadata budget")
	}
	var meta map[string]any
	if err := yaml.Unmarshal([]byte(parts[1]), &meta); err != nil {
		return false, fmt.Errorf("playbook front matter: %w", err)
	}
	return PlaybookRetired(Document{Meta: meta})
}
