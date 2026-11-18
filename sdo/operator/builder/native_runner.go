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

// ControllerStopGrace is how long a stopping controller child has to shut down
// (releasing its leader Lease) before it is killed; the controller pod's
// terminationGracePeriodSeconds must exceed it.
const ControllerStopGrace = buildio.StopGrace

// WithForcedStop carries a forced-stop channel to the controller child's
// supervisor: closing it kills the child without waiting out ControllerStopGrace.
func WithForcedStop(ctx context.Context, force <-chan struct{}) context.Context {
	return buildio.WithForcedStop(ctx, force)
}

// ControllerStopped reports whether err is the controller child ending because
// it was asked to stop (terminated status or killed by a signal).
func ControllerStopped(err error) bool { return buildio.Stopped(err) }

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
// controller is never rebuilt or relaunched for them. A controller that exits
// with memory.HealthRefreshExitCode makes RunOnce return an error for which
// NeedsRebuild is true; Run turns that into the next pass.
func RunOnce(ctx context.Context, config RunConfig) error {
	_, err := runPass(ctx, config)
	return err
}

// runPass is RunOnce that also reports the accepted revision it launched at
// ("" for the image-bound shell controller).
func runPass(ctx context.Context, config RunConfig) (string, error) {
	if config.Controller == nil {
		return "", errors.New("controller config is required")
	}
	source := config.Controller.GetCodePath().GetOverrides().GetDetectorSource()
	if source == "shell" || (source == "" && config.Controller.GetCodePath().GetName() == "shell") {
		if config.PrepareShell != nil {
			if err := config.PrepareShell(ctx); err != nil {
				return "", err
			}
		}
		return "", runShellController(ctx, config)
	}
	if config.ResolveDiagnostics == nil {
		return "", errors.New("accepted diagnostics resolver is required")
	}
	projected, err := config.ResolveDiagnostics(ctx, config.Build.ApplicationRoot)
	if err != nil {
		return "", err
	}
	projectedRevision := projected.AcceptedRevision
	workspace, binary, artifact, err := compileLaunch(ctx, config.Build, projected)
	if err != nil {
		return projectedRevision, err
	}
	defer workspace.Close()

	provenance, err := buildProvenance(projected, binary)
	if err != nil {
		return projectedRevision, err
	}
	config.Controller = proto.Clone(config.Controller).(*contractsv1alpha1.ControllerConfig)
	config.Controller.MemoryHostBinary = artifact.Path
	config.Controller.MemoryHostDigest = artifact.Digest

	data, err := controllerConfigJSON(config, provenance)
	if err != nil {
		return projectedRevision, fmt.Errorf("serialize controller config: %w", err)
	}
	configPath := filepath.Join(workspace.Directory, "controller-config.json")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		return projectedRevision, err
	}
	return projectedRevision, buildio.RunController(ctx, binary, configPath, workspace.Directory)
}

// compileLaunch is every compile a controller start does for the accepted memory: the launch gates and the controller
// binary (BuildForLaunch) and the memory host. The caller owns the returned workspace, which holds both executables.
func compileLaunch(ctx context.Context, build Config, projected DiagnosticsProjection) (*Workspace, string, MemoryHostArtifact, error) {
	build.DiagnosticsRoot = projected.Directory
	compileOnly, err := memory.AdoptedBuild(ctx, build.ApplicationRoot, projected.AcceptedRevision)
	if err != nil {
		return nil, "", MemoryHostArtifact{}, err
	}
	build.CompileOnly = compileOnly
	workspace, err := BuildWorkspace(ctx, build)
	if err != nil {
		return nil, "", MemoryHostArtifact{}, err
	}
	binary, err := workspace.BuildForLaunch(ctx)
	if err != nil {
		_ = workspace.Close()
		return nil, "", MemoryHostArtifact{}, err
	}
	artifact, err := workspace.BuildMemoryHost(ctx)
	if err != nil {
		_ = workspace.Close()
		return nil, "", MemoryHostArtifact{}, err
	}
	return workspace, binary, artifact, nil
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
