package validate

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"sdo.dev/operator/memory/layout"
)

var faultClassLine = regexp.MustCompile(`(?m)^fault_class:.*$`)

// NormalizeAuthoredPlaybooks derives what an author never needs to get right:
// each changed playbook's fault_class comes from its ID, and a change below the
// playbooks directory regenerates the playbook index, dropping any edit to it.
// Like whitespace, it is fixed, never rejected. It returns the paths it rewrote.
func NormalizeAuthoredPlaybooks(root string, paths []string) ([]string, error) {
	var changed []string
	touched := false
	for _, path := range paths {
		touched = touched || strings.HasPrefix(path, layout.PlaybooksDir+"/")
		kind, id, ok := layout.Classify(path)
		if !ok || kind != layout.KindPlaybook {
			continue
		}
		file := filepath.Join(root, filepath.FromSlash(path))
		data, err := os.ReadFile(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		doc, err := layout.ParseDocument(data)
		if err != nil {
			continue
		}
		fault := layout.PlaybookFaultClass(id)
		if doc.Meta["fault_class"] != fault {
			front, rest, _ := strings.Cut(strings.TrimPrefix(doc.Source, "---\n"), "---\n")
			line := "fault_class: " + fault
			if faultClassLine.MatchString(front) {
				front = faultClassLine.ReplaceAllLiteralString(front, line)
			} else {
				front += line + "\n"
			}
			if err := os.WriteFile(file, []byte("---\n"+front+"---\n"+rest), 0o644); err != nil {
				return nil, err
			}
			changed = append(changed, path)
		}
	}
	if !touched {
		return changed, nil
	}
	if rewritten, err := layout.WritePlaybookIndex(root); err != nil {
		return nil, err
	} else if rewritten {
		changed = append(changed, layout.PlaybookIndexFile)
	}
	return changed, nil
}
