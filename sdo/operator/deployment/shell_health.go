package deployment

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/shellpath"
)

// ShellDetectorProbe uses the same immutable accepted scripts and read-only
// runner as steady-state monitoring. Source readiness remains independently checked.
type ShellDetectorProbe struct {
	Client kubernetes.Interface
	Config shellpath.RunnerConfig
	Reader shellpath.Reader
	Limit  time.Duration
}

func (p ShellDetectorProbe) Evaluate(ctx context.Context, root, _, namespace string) (string, error) {
	if namespace != p.Config.Namespace || root != p.Config.AcceptedRoot {
		return "", fmt.Errorf("shell health probe repository or namespace differs from runner")
	}
	if p.Client != nil {
		if err := shellpath.EnsureReadOnlyRunner(ctx, p.Client, p.Config); err != nil {
			return "", err
		}
	} else if p.Reader == nil {
		return "", fmt.Errorf("shell health probe requires a runner client")
	}
	output, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
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
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
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
		select {
		case <-bounded.Done():
			return "", fmt.Errorf("wait for accepted shell health report: %w", bounded.Err())
		case <-ticker.C:
		}
	}
}
