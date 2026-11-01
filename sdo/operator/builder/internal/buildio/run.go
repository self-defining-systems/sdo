package buildio

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

func RunController(ctx context.Context, binary, configPath, directory string) error {
	command := exec.CommandContext(ctx, binary, "--config", configPath)
	command.Dir = directory
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("controller run: %w", err)
	}
	return nil
}
