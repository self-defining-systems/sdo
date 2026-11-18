package builder

import (
	"context"
	"fmt"

	"sdo.dev/operator/memory"
)

// WarmLaunchBuild makes the Go build cache hold what a controller start compiles for the repository's accepted memory:
// it runs that start's compiles (compileLaunch, the one RunOnce uses: the launch gates, the controller binary and the
// memory host) against the operator source at config.Build.OperatorRoot and discards the executables. A lane whose
// controller restarts on a new operator source tree recompiles every package that imports the operator packages and
// their tests, which costs more than the restart itself; run beforehand (by whatever places the new tree, while the
// old controller keeps serving), the restart only links. Nothing is launched and no repository file is written: the
// accepted tree is read as it is (the controller's own start-up upkeep, a schema migration, is not run). It returns
// the accepted commit it built.
func WarmLaunchBuild(ctx context.Context, config RunConfig) (string, error) {
	projected, err := memory.AcceptedDiagnostics(ctx, config.Build.ApplicationRoot)
	if err != nil {
		return "", err
	}
	workspace, _, _, err := compileLaunch(ctx, config.Build, DiagnosticsProjection{Directory: projected.Directory, Fingerprint: projected.Fingerprint, AcceptedRevision: projected.AcceptedRevision})
	if err != nil {
		return "", fmt.Errorf("warm the Go build cache at %s: %w", projected.AcceptedRevision, err)
	}
	if err := workspace.Close(); err != nil {
		return "", err
	}
	return projected.AcceptedRevision, nil
}
