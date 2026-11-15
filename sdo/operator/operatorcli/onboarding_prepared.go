package operatorcli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

// preparedTree clones the repository's accepted commit and installs the
// detectors Go adds when it publishes memory (the generic health set, the
// traffic-probe detector of each probe workload, request metrics), so the live
// items judge the detectors the controller will run, not only the ones the
// agent authored. The agent's own repository is never changed. The returned
// function removes the clone.
func preparedTree(ctx context.Context, repo, telemetryEnvironment string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "sdo-onboarding-prepared-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	run := func(args ...string) error {
		out, err := exec.CommandContext(ctx, "git", append([]string{"-c", "user.name=sdo", "-c", "user.email=sdo@invalid", "-C", dir}, args...)...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if out, err := exec.CommandContext(ctx, "git", "clone", "-q", repo, dir).CombinedOutput(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("clone %s: %w: %s", repo, err, strings.TrimSpace(string(out)))
	}
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion}
	if telemetryEnvironment != "" {
		if env, err = telemetry.LoadEnvironment(telemetryEnvironment); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	sources, err := validate.EffectiveTelemetrySources(dir, env.Policy(), telemetrysources.Registry())
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("telemetry sources: %w", err)
	}
	if err := memory.PrepareHealthWith(dir, memory.HealthOptions{AlertSources: validate.HasAlertSource(sources), RequestMetrics: validate.HasRangeSource(sources)}); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("prepare the generic detectors: %w", err)
	}
	if err := run("add", "-A"); err != nil {
		cleanup()
		return "", nil, err
	}
	if err := run("commit", "-q", "--allow-empty", "-m", "sdo: prepared generic detectors"); err != nil {
		cleanup()
		return "", nil, err
	}
	return dir, cleanup, nil
}
