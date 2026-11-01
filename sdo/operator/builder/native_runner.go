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
)

// RunConfig binds a built application controller to its transport-neutral
// protobuf launch contract. Storage projections and cache publication come
// from the repository edge.
type RunConfig struct {
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
// process. The workspace remains intact until that process exits.
func RunOnce(ctx context.Context, config RunConfig) error {
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
	return wire.Marshal(launch)
}
