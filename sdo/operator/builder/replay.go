package builder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// BuildReplay stages the very same detector/program registration as the
// controller, then links an offline executable instead of starting a controller.
// The caller owns the workspace until the binary is finished.
func BuildReplay(ctx context.Context, config Config) (*Workspace, string, error) {
	workspace, err := BuildWorkspace(ctx, config)
	if err != nil {
		return nil, "", err
	}
	failed := true
	defer func() {
		if failed {
			_ = workspace.Close()
		}
	}()
	dir := filepath.Join(workspace.Directory, "cmd", "replay")
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", err
	}
	source := fmt.Sprintf(`package main
import (
 "context"
 "fmt"
 "os"
 "sdo.dev/operator/runtime/replay"
 %q
)
func main(){registry,err:=generated.Programs();if err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(2)};os.Exit(replay.Run(context.Background(),registry,generated.All(),os.Args[1:],os.Stdout,os.Stderr))}
`, workspace.ModulePath+"/generated")
	if err = os.WriteFile(filepath.Join(dir, "main.go"), []byte(source), 0o644); err != nil {
		return nil, "", err
	}
	binary := filepath.Join(workspace.Directory, "sdo-replay")
	// Same source inspection, module rewriting and compiler as the controller.
	// Corpus-label acceptance remains separate: hand-written offline fixtures are
	// inputs to this executable, not fabricated controller provenance.
	for _, args := range [][]string{{"mod", "tidy"}, lean("build", "-buildvcs=false", "-o", binary, "./cmd/replay")} {
		start := time.Now()
		err = workspace.runGo(ctx, args...)
		workspace.StageTimings = append(workspace.StageTimings, StageTiming{Stage: args[0], Duration: time.Since(start)})
		if err != nil {
			return nil, "", err
		}
	}
	failed = false
	return workspace, binary, nil
}
