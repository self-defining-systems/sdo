package buildio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// StageError reports the Go toolchain stage that rejected a workspace.
type StageError struct {
	Stage  string
	Output string
	Err    error
}

func (e *StageError) Error() string {
	return fmt.Sprintf("go %s: %v: %s", e.Stage, e.Err, e.Output)
}

func (e *StageError) Unwrap() error { return e.Err }

func RunGo(ctx context.Context, directory string, args ...string) error {
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = directory
	// The workspace is a self-contained module; a caller's go.work must not select it.
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		return &StageError{Stage: strings.Join(args, " "), Output: strings.TrimSpace(string(output)), Err: err}
	}
	return nil
}

func SeedGoBuildCache(ctx context.Context) error {
	seed := os.Getenv("SDO_GO_CACHE_SEED")
	if seed == "" {
		return nil
	}
	if _, err := os.Stat(seed); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect Go build cache seed: %w", err)
	}
	cache := os.Getenv("GOCACHE")
	if cache == "" {
		return errors.New("GOCACHE is required when SDO_GO_CACHE_SEED is set")
	}
	marker := filepath.Join(cache, ".sdo-seeded")
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect Go build cache: %w", err)
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return fmt.Errorf("create Go build cache: %w", err)
	}
	command := exec.CommandContext(ctx, "cp", "-R", seed+string(os.PathSeparator)+".", cache)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("seed Go build cache: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		return fmt.Errorf("mark seeded Go build cache: %w", err)
	}
	return nil
}
