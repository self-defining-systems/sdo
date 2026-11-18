package operatorcli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/memory/validate"
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
		_, err := processio.Git(ctx, append([]string{"-c", "user.name=sdo", "-c", "user.email=sdo@invalid", "-C", dir}, args...)...)
		return err
	}
	if _, err := processio.Git(ctx, "clone", "-q", repo, dir); err != nil {
		cleanup()
		return "", nil, err
	}
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion}
	if telemetryEnvironment != "" {
		if env, err = telemetry.LoadEnvironment(telemetryEnvironment); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	derived, err := environmentTelemetry(env, telemetrysources.Registry())
	if err != nil {
		cleanup()
		return "", nil, err
	}
	if err := prepareTree(dir, derived); err != nil {
		cleanup()
		return "", nil, err
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

// detectorCount is the number of detector packages in a prepared tree.
func detectorCount(root string) int {
	count := 0
	_ = filepath.WalkDir(filepath.Join(root, ".sdo", "diagnostics", "detectors"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "manifest.yaml" {
			count++
		}
		return nil
	})
	return count
}

// environmentTelemetry is the telemetry every tree preparation runs under: the
// environment's policy with its provided sources, the same derivation the
// brokers use, so request metrics and metrics alerts join the tree exactly
// where publication would add them.
func environmentTelemetry(env telemetry.Environment, registry *telemetry.Registry) (broker.Telemetry, error) {
	return broker.TelemetryFromEnvironment(env, registry)
}

// prepareTree installs, under root, the detectors Go adds at publication. The
// memory check, the onboarding check, publication and install all reach the
// same validate.PrepareHealthTree through telemetry from environmentTelemetry.
func prepareTree(root string, derived broker.Telemetry) error {
	if err := validate.PrepareHealthTree(root, derived.Policy, derived.Registry); err != nil {
		return fmt.Errorf("prepare the generic detectors: %w", err)
	}
	return nil
}

// prepareInstallTree makes the repository's accepted head a prepared tree: an
// onboarding memory carries only what the agent authored, and a controller
// started on it would hold zero health detectors. It commits the Go-installed
// detectors when they are missing or stale and returns the resulting head.
func prepareInstallTree(ctx context.Context, repo string, derived broker.Telemetry) (string, error) {
	if err := prepareTree(repo, derived); err != nil {
		return "", err
	}
	git := func(args ...string) error {
		_, err := processio.Git(ctx, append([]string{"-c", "user.name=sdo", "-c", "user.email=sdo@invalid", "-C", repo}, args...)...)
		return err
	}
	if err := git("add", "--force", "-A", "--", ".sdo"); err != nil {
		return "", err
	}
	if out, err := processio.Git(ctx, "-C", repo, "status", "--porcelain", "--", ".sdo"); err != nil {
		return "", err
	} else if strings.TrimSpace(out) != "" {
		message := "sdo: prepare the generic detectors\n\nSDO-Actor: human\nSDO-Phase: initial\nSDO-Origin: install-existing"
		if err := git("commit", "-q", "-m", message); err != nil {
			return "", err
		}
	}
	return acceptedHead(ctx, repo)
}
