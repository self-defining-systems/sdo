package memory

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// GetAt reads only immutable accepted outcome bytes. Historical storage layout
// and retired JSONL imports remain private to this repository edge.
func (s *JSONStore) GetAt(ctx context.Context, commit, incidentID string) (Outcome, error) {
	if s == nil || s.root == "" || !fullCommitPattern.MatchString(commit) || strings.TrimSpace(incidentID) == "" {
		return Outcome{}, fmt.Errorf("outcome requires repository, full accepted commit and incident ID")
	}
	git := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", append([]string{"-C", s.root}, args...)...).Output()
	}
	if _, err := git("merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
		return Outcome{}, fmt.Errorf("outcome commit is not accepted: %w", err)
	}
	read := func(path string) ([]byte, error) {
		listing, err := git("ls-tree", commit, "--", path)
		if err != nil {
			return nil, err
		}
		if len(listing) == 0 {
			return nil, ErrNotFound
		}
		fields := strings.Fields(string(listing))
		if len(fields) < 3 || (fields[0] != "100644" && fields[0] != "100755") || fields[1] != "blob" {
			return nil, fmt.Errorf("accepted outcome is not a regular file")
		}
		return git("show", commit+":"+path)
	}
	data, err := read(layout.OutcomePath(incidentID))
	if err == nil {
		outcome, err := parse(data)
		if err != nil {
			return Outcome{}, err
		}
		if outcome.IncidentID != incidentID {
			return Outcome{}, fmt.Errorf("accepted outcome identity differs")
		}
		return outcome, nil
	}
	if err != ErrNotFound {
		return Outcome{}, err
	}
	data, err = read(layout.LegacyOutcomesFile)
	if err != nil {
		return Outcome{}, err
	}
	var selected *Outcome
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		outcome, err := normalizeLegacy([]byte(line))
		if err != nil {
			return Outcome{}, err
		}
		if outcome.IncidentID != incidentID {
			continue
		}
		if selected != nil && string(selected.document) != string(outcome.document) {
			return Outcome{}, ErrConflict
		}
		selected = &outcome
	}
	if selected == nil {
		return Outcome{}, ErrNotFound
	}
	return *selected, nil
}
