package operatorcli

import (
	"context"
	"encoding/json"
	"io"

	"sdo.dev/operator/builder"
)

// RunWarmBuild is `sdo-operator warm-build controller <the controller's own arguments>`: the compiles a controller start
// does for the accepted memory (builder.WarmLaunchBuild), run in the environment of the pod it is started in (the
// controller Job's image, Go environment, build cache volume, mounted settings and projected operator tree), so the Go
// build cache that pod mounts holds what the next controller start compiles. A swap to a new operator source tree runs
// it before it stops the controller. It reads the accepted repository and writes nothing but the cache, and answers
// one JSON line.
func RunWarmBuild(ctx context.Context, args []string, output io.Writer) error {
	run, err := ParseControllerLaunch(args)
	if err != nil {
		return err
	}
	commit, err := builder.WarmLaunchBuild(ctx, run)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]string{"commit": commit})
}
