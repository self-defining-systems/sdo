package memory

import (
	"context"
	"strings"
)

// HealthRefreshPreservesInstall admits only an unbroken first-parent chain of
// independently validated judge publications after an installation. It preserves
// the frozen install snapshot; it does not grant health authority to adoption.
func HealthRefreshPreservesInstall(ctx context.Context, repository, installed, current string) (bool, error) {
	if repository == "" || installed == "" || current == "" {
		return false, nil
	}
	accepted, err := AcceptedHealthRevision(ctx, repository)
	if err != nil {
		return false, err
	}
	if accepted.NeedsRefresh || accepted.Commit != current {
		return false, nil
	}
	original, err := HealthInputs(ctx, repository, installed)
	if err != nil {
		return false, err
	}
	// Traverse actual parent links, so neither a merge nor an unrelated history
	// can supply provenance for the captured installation.
	for revision := current; revision != installed; {
		raw, err := healthGit(ctx, repository, "show", "-s", "--format=%P%x00%B", revision)
		if err != nil {
			return false, err
		}
		parts := strings.SplitN(string(raw), "\x00", 2)
		parents := strings.Fields(parts[0])
		if len(parents) != 1 || len(parts) != 2 {
			return false, nil
		}
		trailers := "\n" + strings.TrimSpace(parts[1]) + "\n"
		for _, required := range []string{"SDO-Actor: health_judge", "SDO-Phase: initial", "SDO-Validation: passed"} {
			if !strings.Contains(trailers, "\n"+required+"\n") {
				return false, nil
			}
		}
		inputs, err := HealthInputs(ctx, repository, revision)
		if err != nil {
			return false, err
		}
		if inputs.SourceInputs != original.SourceInputs || inputs.Objective != original.Objective {
			return false, nil
		}
		revision = parents[0]
	}
	return true, nil
}
