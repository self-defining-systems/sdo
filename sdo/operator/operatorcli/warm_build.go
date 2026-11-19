package operatorcli

import (
	"context"
	"encoding/json"
	"io"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// RunWarmBuild is `sdo-operator warm-build controller <the controller's own arguments>`: the compiles a controller start
// does for the accepted memory (builder.WarmLaunchBuild), run in the environment of the pod it is started in (the
// controller Job's image, Go environment, build cache volume, mounted settings and projected operator tree), so the Go
// build cache that pod mounts holds what the next controller start compiles. A swap to a new operator source tree runs
// it before it stops the controller. It does not run the start-up upkeep (schema migration, health refresh) and never
// writes the accepted repository (git runs with optional locks off in its pod).
//
// What it writes: the Go build cache (GOCACHE), which is the point, and scratch: the staged workspace and the
// toolchain's work directories, in fresh directories inside TMPDIR and GOTMPDIR that are removed when it ends, however
// it ends (SIGTERM or SIGINT cancels the build and the cleanup still runs). It answers one JSON line.
func RunWarmBuild(ctx context.Context, args []string, output io.Writer) error {
	run, err := ParseControllerLaunch(args)
	if err != nil {
		return err
	}
	ctx, stop := processio.TerminationContext(ctx)
	defer stop()
	cleanup, err := processio.ScopedTemp("warm-build-")
	if err != nil {
		return err
	}
	defer cleanup()
	commit, err := builder.WarmLaunchBuild(ctx, run)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]string{"commit": commit})
}
