package builder

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
)

// RunConfig binds a built application controller to its transport-neutral
// protobuf launch contract. Storage projections and cache publication come
// from the repository edge.
type RunConfig struct {
	Build              Config
	Controller         *contractsv1alpha1.ControllerConfig
	ResolveDiagnostics func(context.Context, string) (DiagnosticsProjection, error)
	PublishProber      func(string, string) (string, error)
}

type DiagnosticsProjection struct {
	Directory        string
	Fingerprint      string
	AcceptedRevision string
}

// RunOnce builds the currently accepted diagnostics and runs one controller
// process. The workspace remains intact until that process exits.
func RunOnce(ctx context.Context, config RunConfig) error {
	return runWithHealthRefresh(ctx, config, runOnce)
}

func runWithHealthRefresh(ctx context.Context, config RunConfig, run func(context.Context, RunConfig) error) error {
	var deadline time.Time
	if config.Controller != nil && config.Controller.GetDuration().AsDuration() > 0 {
		deadline = time.Now().Add(config.Controller.GetDuration().AsDuration())
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		config.Controller = proto.Clone(config.Controller).(*contractsv1alpha1.ControllerConfig)
	}
	for {
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("health unverified: installation budget exhausted before controller launch: %w", context.DeadlineExceeded)
			}
			config.Controller.Duration = durationpb.New(remaining)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err := run(ctx, config)
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == memory.HealthRefreshExitCode {
			continue
		}
		return err
	}
}

func runOnce(ctx context.Context, config RunConfig) error {
	if config.Controller == nil {
		return errors.New("controller config is required")
	}
	if config.ResolveDiagnostics == nil {
		return errors.New("accepted diagnostics resolver is required")
	}
	projected, err := config.ResolveDiagnostics(ctx, config.Build.ApplicationRoot)
	if err != nil {
		return err
	}
	buildConfig := config.Build
	buildConfig.DiagnosticsRoot = projected.Directory
	workspace, err := BuildWorkspace(ctx, buildConfig)
	if err != nil {
		return err
	}
	defer workspace.Close()
	binary, err := workspace.ValidateAndBuild(ctx)
	if err != nil {
		return err
	}
	var prober string
	if workspace.HasProber {
		if config.PublishProber == nil {
			return errors.New("traffic prober publisher is required")
		}
		prober, err = config.PublishProber(config.Controller.RepositoryMountPath, filepath.Join(workspace.Directory, "sdo-prober"))
		if err != nil {
			return err
		}
	}
	provenance, err := buildProvenance(projected, binary)
	if err != nil {
		return err
	}
	data, err := controllerConfigJSON(config, prober, provenance)
	if err != nil {
		return fmt.Errorf("serialize controller config: %w", err)
	}
	configPath := filepath.Join(workspace.Directory, "controller-config.json")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, binary, "--config", configPath)
	command.Dir = workspace.Directory
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("controller run: %w", err)
	}
	return nil
}

// Supervise rebuilds from the accepted application tree after each normal
// controller exit. The runtime must exit before acknowledging any closure
// whose detector update needs an independently observed restart.
func Supervise(ctx context.Context, config RunConfig) error {
	if config.Controller == nil {
		return errors.New("controller config is required")
	}
	if config.Controller.ExitAfterClosure || config.Controller.Duration != nil {
		return errors.New("supervision cannot be combined with exit_after_closure or duration")
	}
	config.Controller = proto.Clone(config.Controller).(*contractsv1alpha1.ControllerConfig)
	config.Controller.RestartAfterClosure = true
	return supervise(ctx, config, RunOnce, os.Stdout)
}

// supervise announces every relaunch on out as a JSON record, which the
// benchmark adapter reads from the controller log to know that learned
// detectors are live again.
func supervise(ctx context.Context, config RunConfig, run func(context.Context, RunConfig) error, out io.Writer) error {
	for launches := 1; ; launches++ {
		if err := run(ctx, config); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "{\"controller_supervisor\":\"relaunch\",\"launches\":%d}\n", launches); err != nil {
			return err
		}
	}
}

type provenance struct {
	Fingerprint    string
	ArtifactDigest string
	Revision       string
}

func buildProvenance(projected DiagnosticsProjection, binary string) (provenance, error) {
	if projected.Fingerprint == "" || projected.AcceptedRevision == "" {
		return provenance{}, errors.New("accepted diagnostics projection lacks provenance")
	}
	artifact, err := os.ReadFile(binary)
	if err != nil {
		return provenance{}, err
	}
	digest := sha256.Sum256(artifact)
	return provenance{Fingerprint: projected.Fingerprint, ArtifactDigest: fmt.Sprintf("sha256:%x", digest), Revision: projected.AcceptedRevision}, nil
}

func controllerConfigJSON(config RunConfig, prober string, built provenance) ([]byte, error) {
	if config.Controller == nil {
		return nil, errors.New("controller config is required")
	}
	launch := proto.Clone(config.Controller).(*contractsv1alpha1.ControllerConfig)
	launch.AppRoot = config.Build.ApplicationRoot
	launch.ProberBinary = prober
	launch.DiagnosticsFingerprint = built.Fingerprint
	launch.ControllerArtifactDigest = built.ArtifactDigest
	launch.ControllerRevision = built.Revision
	// Tasks investigate the source compiled with this accepted policy; deployed
	// identity remains separate evidence for image/rendered-lock comparisons.
	launch.SourceCommit = built.Revision
	return wire.Marshal(launch)
}
