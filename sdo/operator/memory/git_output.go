package memory

import (
	"bytes"
	"context"
	"fmt"

	"sdo.dev/operator/memory/internal/memoryio"
)

func gitOutput(ctx context.Context, repository string, args ...string) (string, error) {
	raw, err := memoryio.Git(ctx, repository, true, nil, args...)
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, raw)
	}
	return string(bytes.TrimSpace(raw)), nil
}
