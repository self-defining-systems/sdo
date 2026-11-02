package installio

import (
	"context"
	"os/exec"
	"time"
)

func Now() time.Time { return time.Now() }
func Git(ctx context.Context, root string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...).CombinedOutput()
}
func Wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
