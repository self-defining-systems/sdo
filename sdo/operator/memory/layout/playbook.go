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
// are executable. It does not touch the playbook index; see IndexPlaybook.
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

// IndexPlaybook appends a playbook's link to the playbook index, creating the
// index when it does not exist. It is idempotent.
func IndexPlaybook(root, id string) error {
	if !ValidPlaybookID(id) {
		return fmt.Errorf("invalid playbook ID %q", id)
	}
	target := filepath.Join(root, filepath.FromSlash(PlaybookIndexFile))
	existing, err := os.ReadFile(target)
	if errors.Is(err, fs.ErrNotExist) {
		existing = []byte("# Playbooks\n\n")
	} else if err != nil {
		return err
	}
	link := fmt.Sprintf("- [%s](%s/%s)\n", id, id, PlaybookFileName)
	if strings.Contains(string(existing), link) {
		return nil
	}
	if len(existing) > 0 && existing[len(existing)-1] != '\n' {
		existing = append(existing, '\n')
	}
	return writeMemoryFile(root, PlaybookIndexFile, append(existing, link...), 0o644)
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
