package processio

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func AcceptedHead(ctx context.Context, repository string) (string, error) {
	root, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("inspect accepted repository root: %w", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(strings.TrimSpace(string(root)))
	if err != nil {
		return "", err
	}
	resolvedRepository, err := filepath.EvalSymlinks(repository)
	if err != nil {
		return "", err
	}
	if resolvedRoot != resolvedRepository {
		return "", errors.New("accepted repository path must be its Git root")
	}
	status, err := exec.CommandContext(ctx, "git", "-C", repository, "status", "--porcelain").Output()
	if err != nil {
		return "", fmt.Errorf("inspect accepted repository status: %w", err)
	}
	if len(status) != 0 {
		return "", errors.New("accepted repository has uncommitted changes")
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("inspect accepted commit: %w", err)
	}
	return strings.TrimSpace(string(commit)), nil
}
func HeadCommit(ctx context.Context, root string) string {
	output, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if commit := strings.TrimSpace(string(output)); err == nil && commit != "" {
		return commit
	}
	return ""
}
