package builder

import (
	"context"
	"fmt"

	"sdo.dev/operator/memory"
)

// WarmAcceptedMemoryHost makes the Go build cache hold what the controller's start-up build of the repository's
// accepted memory needs: it runs that build (BuildAcceptedMemoryHost: the staged diagnostics, `go mod tidy`, the
// memory host's `go build`) against the operator source at config.OperatorRoot and discards the executable. A lane
// whose controller restarts on a new operator source tree recompiles the operator packages it imports, which costs
// more than the restart itself; run beforehand (by whatever places the new tree, while the old controller keeps
// serving), the restart only links. Nothing is launched and no repository file is written. It returns the accepted
// commit it built.
func WarmAcceptedMemoryHost(ctx context.Context, config Config) (string, error) {
	projected, err := memory.AcceptedDiagnostics(ctx, config.ApplicationRoot)
	if err != nil {
		return "", err
	}
	workspace, _, err := BuildAcceptedMemoryHost(ctx, config, projected.AcceptedRevision)
	if err != nil {
		return "", fmt.Errorf("warm the Go build cache at %s: %w", projected.AcceptedRevision, err)
	}
	if err := workspace.Close(); err != nil {
		return "", err
	}
	return projected.AcceptedRevision, nil
}
