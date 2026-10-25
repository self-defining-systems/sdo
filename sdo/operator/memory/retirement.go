package memory

import (
	"context"
	"fmt"
	"os/exec"
	"sdo.dev/operator/memory/layout"
	"strings"
)

// PlaybookActivationChanged compares accepted lifecycle metadata, so ordinary
// prose changes do not restart the controller but retirement/reactivation does.
func PlaybookActivationChanged(ctx context.Context, repository, before, after string, paths []string) (bool, error) {
	for _, path := range paths {
		kind, _, ok := layout.Classify(path)
		if !ok || kind != layout.KindPlaybook {
			continue
		}
		states := []bool{}
		for _, revision := range []string{before, after} {
			listed, err := exec.CommandContext(ctx, "git", "-C", repository, "ls-tree", revision, "--", path).CombinedOutput()
			if err != nil {
				return false, fmt.Errorf("read playbook at %s: %w: %s", revision, err, listed)
			}
			if len(listed) == 0 {
				states = append(states, false)
				continue
			}
			data, err := exec.CommandContext(ctx, "git", "-C", repository, "show", revision+":"+path).CombinedOutput()
			if err != nil {
				return false, fmt.Errorf("read playbook at %s: %w: %s", revision, err, data)
			}
			// Legacy prose has no lifecycle marker and is active.
			if !strings.HasPrefix(string(data), "---\n") {
				states = append(states, false)
				continue
			}
			doc, err := layout.ParseDocument(data)
			if err != nil {
				return false, err
			}
			retired, err := layout.PlaybookRetired(doc)
			if err != nil {
				return false, err
			}
			states = append(states, retired)
		}
		if states[0] != states[1] {
			return true, nil
		}
	}
	return false, nil
}
