package memory

import (
	"context"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// HealthRefreshPreservesInstall admits only an unbroken first-parent chain of
// validated initial-judge publications and Upkeep schema migrations after
// installation. Memory older than the build is migrated right after install;
// that is the install, not a change to it. Either kind may change only memory
// files: application inputs and the objective must equal the install's. It
// preserves the frozen install snapshot; it does not grant health authority to
// adoption.
func HealthRefreshPreservesInstall(ctx context.Context, repository, installed, current string) (bool, error) {
	if repository == "" || installed == "" || current == "" {
		return false, nil
	}
	accepted, err := AcceptedHealthRevision(ctx, repository, nil)
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
		if !hasTrailers(trailers, "health_judge") {
			migration := hasTrailers(trailers, "upkeep")
			if migration {
				if migration, err = stampsSchema(ctx, repository, revision); err != nil {
					return false, err
				}
			}
			if !migration {
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

func hasTrailers(trailers, actor string) bool {
	for _, required := range []string{"SDO-Actor: " + actor, "SDO-Phase: initial", "SDO-Validation: passed"} {
		if !strings.Contains(trailers, "\n"+required+"\n") {
			return false
		}
	}
	return true
}

// stampsSchema reports whether the commit rewrites the schema-version file. The
// broker lets Upkeep change detectors only together with that stamp, so it
// marks a migration commit for every schema version, present and future.
func stampsSchema(ctx context.Context, repository, revision string) (bool, error) {
	raw, err := healthGit(ctx, repository, "diff-tree", "-r", "--no-commit-id", "--name-only", revision)
	if err != nil {
		return false, err
	}
	for _, name := range strings.Fields(string(raw)) {
		if name == layout.SchemaVersionFile {
			return true, nil
		}
	}
	return false, nil
}
