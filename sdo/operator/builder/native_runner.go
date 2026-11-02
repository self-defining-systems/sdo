package builder

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/builder/internal/buildio"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
)

// RunConfig binds a built application controller to its transport-neutral
// protobuf launch contract. Storage projections and cache publication come
// from the repository edge.
type RunConfig struct {
	ShellControllerBinary string
	PrepareShell          func(context.Context) error

	Build              Config
	Controller         *contractsv1alpha1.ControllerConfig
	ResolveDiagnostics func(context.Context, string) (DiagnosticsProjection, error)
}

type DiagnosticsProjection struct {
	Directory        string
	Fingerprint      string
	AcceptedRevision string
}

// RunOnce builds the currently accepted diagnostics and runs one controller
// process. The workspace remains intact until that process exits. Accepted
// memory changes reach the running controller through its memory host, so the
// controller is never rebuilt or relaunched for them.
func RunOnce(ctx context.Context, config RunConfig) error {
	if config.Controller == nil {
		return errors.New("controller config is required")
	}
	source := config.Controller.GetCodePath().GetOverrides().GetDetectorSource()
	if source == "shell" || (source == "" && config.Controller.GetCodePath().GetName() == "shell") {
		if config.PrepareShell != nil {
			if err := config.PrepareShell(ctx); err != nil {
				return err
			}
		}
		return runShellController(ctx, config)
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
	buildConfig.CompileOnly, err = memory.AdoptedBuild(ctx, config.Build.ApplicationRoot, projected.AcceptedRevision)
	if err != nil {
		return err
	}
	workspace, err := BuildWorkspace(ctx, buildConfig)
	if err != nil {
		return err
	}
	defer workspace.Close()
	binary, err := workspace.BuildForLaunch(ctx)
	if err != nil {
		return err
	}

	provenance, err := buildProvenance(projected, binary)
	if err != nil {
		return err
	}
	artifact, err := workspace.BuildMemoryHost(ctx)
	if err != nil {
		return err
	}
	config.Controller = proto.Clone(config.Controller).(*contractsv1alpha1.ControllerConfig)
	config.Controller.MemoryHostBinary = artifact.Path
	config.Controller.MemoryHostDigest = artifact.Digest

	data, err := controllerConfigJSON(config, provenance)
	if err != nil {
		return fmt.Errorf("serialize controller config: %w", err)
	}
	configPath := filepath.Join(workspace.Directory, "controller-config.json")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		return err
	}
	return buildio.RunController(ctx, binary, configPath, workspace.Directory)
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

func controllerConfigJSON(config RunConfig, built provenance) ([]byte, error) {
	if config.Controller == nil {
		return nil, errors.New("controller config is required")
	}
	launch := proto.Clone(config.Controller).(*contractsv1alpha1.ControllerConfig)
	launch.AppRoot = config.Build.ApplicationRoot
	launch.DiagnosticsFingerprint = built.Fingerprint
	launch.ControllerArtifactDigest = built.ArtifactDigest
	launch.ControllerRevision = built.Revision
	// Tasks investigate the source compiled with this accepted policy; deployed
	// identity remains separate evidence for image/rendered-lock comparisons.
	launch.SourceCommit = built.Revision
	return wire.Marshal(launch)
}

// runShellController starts the image-bound controller directly. Accepted shell
// scripts are activated by the runner and never enter the Go builder.
func runShellController(ctx context.Context, config RunConfig) error {
	directory, err := os.MkdirTemp("", "sdo-shell-controller-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	data, err := wire.Marshal(config.Controller)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, "controller-config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	binary := config.ShellControllerBinary
	if binary == "" {
		binary = "/usr/local/bin/sdo-shell-controller"
	}
	return buildio.RunController(ctx, binary, path, "")
}
