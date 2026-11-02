package memory

import (
	"context"
	"fmt"

	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/memory/layout"
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
			listed, err := memoryio.Git(ctx, repository, true, nil, "ls-tree", revision, "--", path)
			if err != nil {
				return false, fmt.Errorf("read playbook at %s: %w: %s", revision, err, listed)
			}
			if len(listed) == 0 {
				states = append(states, false)
				continue
			}
			data, err := memoryio.Git(ctx, repository, true, nil, "show", revision+":"+path)
			if err != nil {
				return false, fmt.Errorf("read playbook at %s: %w: %s", revision, err, data)
			}
			retired, err := layout.ParsePlaybookRetirement(data)
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
