package deployment

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/shellpath"
)

// ShellDetectorProbe uses the same immutable accepted scripts and read-only
// runner as steady-state monitoring. Source readiness remains independently checked.
type ShellDetectorProbe struct {
	Config shellpath.RunnerConfig
	Reader shellpath.Reader
	Limit  time.Duration
}

func (p ShellDetectorProbe) Evaluate(ctx context.Context, root, _, namespace string) (string, error) {
	if namespace != p.Config.Namespace || root != p.Config.AcceptedRoot {
		return "", fmt.Errorf("shell health probe repository or namespace differs from runner")
	}
	output, err := deploymentio.Output(ctx, "git", []string{"-C", root, "rev-parse", "HEAD"})
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(output))
	reader := p.Reader
	if reader == nil {
		reader = shellpath.HTTPReader{URL: shellpath.RunnerURL(p.Config.ControlNamespace)}
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 2 * time.Minute
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	for {
		report, err := reader.Read(bounded)
		if err == nil && report.AcceptedCommit == commit && len(report.Results) > 0 {
			status := "clear"
			for _, result := range report.Results {
				if result.Error != "" || result.TimedOut || result.ExitCode < 0 {
					return "error", nil
				}
				if result.ExitCode != 0 {
					status = "active"
				}
			}
			return status, nil
		}
		if err := deploymentio.Wait(bounded, 250*time.Millisecond); err != nil {
			return "", fmt.Errorf("wait for accepted shell health report: %w", err)
		}
	}
}
