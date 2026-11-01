package processio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

func StreamPodArchive(ctx context.Context, namespace, kubeconfig, podName, script string, consume func(io.Reader) error) error {
	command := exec.CommandContext(ctx, "kubectl", "--namespace", namespace, "exec", podName, "--", "sh", "-c", script)
	if kubeconfig != "" {
		command.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	}
	pipe, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	copyErr := consume(pipe)
	if copyErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if copyErr != nil {
		return copyErr
	}
	if waitErr != nil {
		return fmt.Errorf("export runtime artifacts: %w: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	return nil
}
