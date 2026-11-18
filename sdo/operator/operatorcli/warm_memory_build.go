package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"time"

	"sdo.dev/operator/builder"
)

// RunWarmMemoryBuild is `sdo-operator warm-memory-build --repository <accepted repository>`: the controller's start-up
// build of the repository's accepted memory (builder.WarmAcceptedMemoryHost), run in the environment of the pod it is
// started in, so the Go build cache that pod mounts holds what the next controller start compiles. A swap to a new
// operator source tree runs it on the dev mount before it stops the controller. It answers one JSON line.
func RunWarmMemoryBuild(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("warm-memory-build", flag.ContinueOnError)
	repository := flags.String("repository", "", "accepted repository (absolute path)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(*repository) || flags.NArg() != 0 {
		return errors.New("warm-memory-build requires --repository with an absolute path")
	}
	started := time.Now()
	commit, err := builder.WarmAcceptedMemoryHost(ctx, builder.Config{ApplicationRoot: *repository, OperatorRoot: builder.DefaultOperatorRoot()})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"commit": commit, "seconds": time.Since(started).Seconds()})
}
