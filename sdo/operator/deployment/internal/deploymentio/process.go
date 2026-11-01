// Package deploymentio owns deployment process, socket and timer adapters.
package deploymentio

import (
	"bytes"
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/util/rand"
	"net"
	"os/exec"
	"path/filepath"
	"time"
)

func Run(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...)
	if input != nil {
		c.Stdin = bytes.NewReader(input)
	}
	return c.CombinedOutput()
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
func JobSuffix() string { return rand.String(8) }
func Evaluate(ctx context.Context, directory, binary, namespace string, prober bool, args []string) ([]byte, []byte, error) {
	if prober {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, nil, fmt.Errorf("reserve local prober address: %w", err)
		}
		address := l.Addr().String()
		_ = l.Close()
		p := exec.CommandContext(ctx, filepath.Join(directory, "sdo-prober"), "--namespace", namespace, "--listen", address)
		p.Dir = directory
		if err := p.Start(); err != nil {
			return nil, nil, fmt.Errorf("start bounded health prober: %w", err)
		}
		defer func() { _ = p.Process.Kill(); _ = p.Wait() }()
		args = append(args, "--prober-url", "http://"+address)
	}
	c := exec.CommandContext(ctx, binary, args...)
	c.Dir = directory
	var out, stderr bytes.Buffer
	c.Stdout, c.Stderr = &out, &stderr
	err := c.Run()
	return out.Bytes(), stderr.Bytes(), err
}
