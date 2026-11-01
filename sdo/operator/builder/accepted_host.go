package builder

import (
	"context"
	"errors"
	"sdo.dev/operator/memory"
)

// BuildAcceptedMemoryHost stages accepted code before compiling. A concurrent
// publication invalidates the candidate rather than binding it to mixed bytes.
func BuildAcceptedMemoryHost(ctx context.Context, config Config, commit string) (*Workspace, MemoryHostArtifact, error) {
	projected, err := memory.AcceptedDiagnostics(ctx, config.ApplicationRoot)
	if err != nil {
		return nil, MemoryHostArtifact{}, err
	}
	if projected.AcceptedRevision != commit {
		return nil, MemoryHostArtifact{}, errors.New("accepted revision changed before host staging")
	}
	config.MemoryHost = true
	config.DiagnosticsRoot = projected.Directory
	workspace, err := BuildWorkspace(ctx, config)
	if err != nil {
		return nil, MemoryHostArtifact{}, err
	}
	after, err := memory.AcceptedDiagnostics(ctx, config.ApplicationRoot)
	if err != nil || after.AcceptedRevision != commit || after.Fingerprint != projected.Fingerprint {
		_ = workspace.Close()
		return nil, MemoryHostArtifact{}, errors.New("accepted revision changed while host staging")
	}
	artifact, err := workspace.BuildMemoryHost(ctx)
	if err != nil {
		_ = workspace.Close()
		return nil, MemoryHostArtifact{}, err
	}
	return workspace, artifact, nil
}
