package memory

import (
	"context"

	"sdo.dev/operator/memory/layout"
)

// DefaultHealthObjective is SDO's standard goal for an installation whose
// accepted memory carries none.
const DefaultHealthObjective = "Users can reach the application and its operations succeed: every exposed entry answers correctly, and nothing the application runs stays in a failed or unready state."

// EnsureGoal supplies the goal when the accepted HEAD has none. The goal is
// human-owned, so an onboarding agent cannot write it; install records SDO's
// standard goal in a human commit instead of letting the controller fail at
// start (the health judge and every objective read need it). It reports
// whether it committed. An existing goal, however it reads, is never replaced.
func EnsureGoal(ctx context.Context, repository, application string) (bool, error) {
	if _, err := healthGit(ctx, repository, "cat-file", "-e", "HEAD:"+layout.GoalFile); err == nil {
		return false, nil
	}
	if err := WriteGoal(repository, application, DefaultHealthObjective, nil); err != nil {
		return false, err
	}
	if _, err := gitOutput(ctx, repository, "add", "--force", "--", layout.GoalFile); err != nil {
		return false, err
	}
	message := "sdo: supply the standard goal\n\nSDO-Actor: human\nSDO-Phase: initial\nSDO-Origin: install-existing"
	if _, err := gitOutput(ctx, repository, "commit", "-m", message, "--", layout.GoalFile); err != nil {
		return false, err
	}
	return true, nil
}
