package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
)

// RecordDeploymentSource makes source the deployment source of the accepted
// repository's .sdo/deployment/plan.yaml, in a human commit. An existing plan
// keeps its images and expected workloads; only its source is replaced. The
// commit is skipped when the file already says exactly this. It returns the
// plan now in the tree.
func RecordDeploymentSource(ctx context.Context, repository string, source *contracts.DeploymentSource) (*contracts.DeploymentPlan, error) {
	if repository == "" || source == nil {
		return nil, errors.New("repository and deployment source are required")
	}
	plan, err := planfile.Read(repository, planfile.Catalogs{})
	if errors.Is(err, planfile.ErrAbsent) {
		plan = &contracts.DeploymentPlan{}
	} else if err != nil {
		return nil, err
	}
	plan.Source = source
	want, err := planfile.Format(plan)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(repository, filepath.FromSlash(layout.DeploymentPlanFile))
	if have, err := os.ReadFile(path); err == nil && bytes.Equal(have, want) {
		if dirty, err := gitOutput(ctx, repository, "status", "--porcelain", "--", layout.DeploymentPlanFile); err != nil || dirty == "" {
			return plan, err
		}
	}
	if err := planfile.Write(repository, plan); err != nil {
		return nil, err
	}
	if _, err := gitOutput(ctx, repository, "add", "--force", "--", layout.DeploymentPlanFile); err != nil {
		return nil, err
	}
	message := "sdo: record the deployment plan\n\nSDO-Actor: human\nSDO-Phase: initial\nSDO-Origin: install-existing"
	if _, err := gitOutput(ctx, repository, "commit", "-m", message, "--", layout.DeploymentPlanFile); err != nil {
		return nil, err
	}
	return plan, nil
}

func gitOutput(ctx context.Context, repository string, args ...string) (string, error) {
	raw, err := memoryio.Git(ctx, repository, true, nil, args...)
	if err != nil {
		return "", fmt.Errorf("record deployment plan git %s: %w: %s", args[0], err, raw)
	}
	return string(bytes.TrimSpace(raw)), nil
}
