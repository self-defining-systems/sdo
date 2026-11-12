package validate

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/safefs"
)

var faultClassLine = regexp.MustCompile(`(?m)^fault_class:.*$`)

// frontMatterKey matches an existing top-level key of a README's front matter.
func frontMatterKey(front, key string) bool {
	return regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `:`).MatchString(front)
}

// NormalizeAuthoredPlaybooks derives what an author never needs to get right:
// each changed playbook's fault_class comes from its ID, its schema stamp,
// owner and originating incident are filled in when absent (incidentID empty
// invents no incident), and a change below the playbooks directory regenerates
// the playbook index, dropping any edit to it. Like whitespace, it is fixed,
// never rejected, and a value the README already holds is never overwritten.
// It returns the paths it rewrote.
func NormalizeAuthoredPlaybooks(root string, paths []string, incidentID string) ([]string, error) {
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
		front, rest, found := strings.Cut(strings.TrimPrefix(string(data), "---\n"), "---\n")
		if !strings.HasPrefix(string(data), "---\n") || !found {
			continue
		}
		for _, stamp := range []struct{ key, line string }{
			{"schema_version", "schema_version: 1"},
			{"owner", "owner: responder"},
			{"originating_incident", "originating_incident: " + incidentID},
		} {
			if !frontMatterKey(front, stamp.key) && (stamp.key != "originating_incident" || incidentID != "") {
				front += stamp.line + "\n"
			}
		}
		fault := layout.PlaybookFaultClass(id)
		line := "fault_class: " + fault
		if faultClassLine.MatchString(front) {
			front = faultClassLine.ReplaceAllLiteralString(front, line)
		} else {
			front += line + "\n"
		}
		if updated := "---\n" + front + "---\n" + rest; updated != string(data) {
			if err := safefs.WriteFile(root, path, []byte(updated), 0o644); err != nil {
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
