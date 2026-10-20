package memory

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// RequireCleanProposalCheckout rejects worker filesystem edits before the
// trusted mapper writes typed artifacts into the broker proposal.
func RequireCleanProposalCheckout(ctx context.Context, checkout string) error {
	if checkout == "" {
		return fmt.Errorf("proposal checkout is required")
	}
	command := exec.CommandContext(ctx, "git", "-C", checkout, "status", "--porcelain", "--untracked-files=all")
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect proposal checkout: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) != "" {
		return fmt.Errorf("worker edited proposal checkout directly; submit typed artifacts instead")
	}
	command = exec.CommandContext(ctx, "git", "-C", checkout, "ls-files", "--others", "-z", "--", ".sdo/lib", ".sdo/corpus")
	output, err = command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect shared library edits: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if len(output) != 0 {
		return fmt.Errorf("worker edited shared library directly; submit typed artifacts instead")
	}
	return nil
}
