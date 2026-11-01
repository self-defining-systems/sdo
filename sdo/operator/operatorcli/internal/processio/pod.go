package processio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func ReadPodFile(ctx context.Context, namespace, kubeconfig, podName, path string) ([]byte, error) {
	command := exec.CommandContext(ctx, "kubectl", "--namespace", namespace, "exec", podName, "--", "cat", path)
	if kubeconfig != "" {
		command.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	}
	output, readErr := command.Output()
	if readErr != nil {
		var exit *exec.ExitError
		if errors.As(readErr, &exit) {
			return nil, fmt.Errorf("read native incident ledger: %w: %s", readErr, strings.TrimSpace(string(exit.Stderr)))
		}
		return nil, fmt.Errorf("read native incident ledger: %w", readErr)
	}
	return output, nil
}
