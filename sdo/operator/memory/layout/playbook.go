package layout

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
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
