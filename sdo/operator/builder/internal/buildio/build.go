package buildio

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
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

// RunGo runs one Go toolchain stage in directory and reports its wall-clock
// duration, including when the stage is rejected. A nil environment inherits
// the process environment.
func RunGo(ctx context.Context, directory string, environment []string, args ...string) (time.Duration, error) {
	// Go's exact-input cache can survive a new isolated staging directory only
	// when temporary source paths are omitted from compiled package identities.
	if len(args) > 0 && (args[0] == "build" || args[0] == "vet" || args[0] == "test") {
		args = append([]string{args[0], "-trimpath"}, args[1:]...)
	}
	stage := strings.Join(args, " ")
	started := time.Now()
	slog.Info("memory_check_stage_started", "stage", stage)
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = directory
	if environment == nil {
		environment = os.Environ()
	}
	// The workspace is a self-contained module; a caller's go.work must not select it.
	command.Env = append(environment, "GOWORK=off")
	output, err := command.CombinedOutput()
	elapsed := time.Since(started)
	slog.Info("memory_check_stage_finished", "stage", stage, "duration", elapsed)
	if err != nil {
		return elapsed, &StageError{Stage: stage, Output: strings.TrimSpace(string(output)), Err: err}
	}
	return elapsed, nil
}

func SeedGoBuildCache(ctx context.Context) error {
	// The cache helper reads image entries in place and writes misses to scratch.
	if os.Getenv("GOCACHEPROG") != "" {
		return nil
	}
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
