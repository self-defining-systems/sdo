package processio

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/memory/validate"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ValidationInfrastructureError distinguishes resource/transport failures from
// authored-content diagnostics. It must never consume a proposal retry.
type ValidationInfrastructureError struct{ Message string }

func (e *ValidationInfrastructureError) Error() string { return e.Message }
func (*ValidationInfrastructureError) ExitCode() int   { return 2 }

func RunValidationProcess(ctx context.Context, binary string, args []string, scratch string, timeout time.Duration, memory int64) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.Command(binary, args...)
	cmd.Dir = scratch
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedValidationOutput
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return &ValidationInfrastructureError{fmt.Sprintf("start validation subprocess: %v", err)}
	}
	// Always reap the entire group, including go/test descendants surviving an
	// early exit. Aggregate resident memory is measured across the whole group.
	defer syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
			return &ValidationInfrastructureError{fmt.Sprintf("validation subprocess timeout/cancellation: %v", ctx.Err())}
		case <-ticker.C:
			used, err := processGroupMemory(cmd.Process.Pid)
			if err != nil || used > memory {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
				return &ValidationInfrastructureError{fmt.Sprintf("validation subprocess memory limit (%d bytes), observed %d: %v", memory, used, err)}
			}
		case err := <-done:
			ds, parseErr := validate.ParseReport(stdout.String())
			if err != nil {
				if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 && parseErr == nil && len(ds) > 0 {
					return validate.Err(ds)
				}
				return &ValidationInfrastructureError{fmt.Sprintf("validation subprocess: %v: %s", err, stderr.String())}
			}
			if parseErr != nil {
				return &ValidationInfrastructureError{fmt.Sprintf("validation subprocess report: %v", parseErr)}
			}
			return validate.Err(ds)
		}
	}
}

func processGroupMemory(group int) (int64, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	var total int64
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if err != nil {
			continue
		}
		// comm may contain spaces or parentheses; fields after its final ')' are stable.
		fields := strings.Fields(string(data)[strings.LastIndex(string(data), ")")+1:])
		if len(fields) < 22 {
			continue
		}
		pgid, _ := strconv.Atoi(fields[2])
		if pgid != group {
			continue
		}
		rss, err := strconv.ParseInt(fields[21], 10, 64)
		if err != nil {
			return 0, err
		}
		total += rss * int64(os.Getpagesize())
	}
	return total, nil
}

// Child output is untrusted too: never let diagnostics exhaust the controller.
type limitedValidationOutput struct{ bytes.Buffer }

func (b *limitedValidationOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := validate.MaxReportBytes + len(validate.ReportPrefix) + 2 - b.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.Buffer.Write(p)
	}
	return n, nil
}

func OperatorRoot() string { return os.Getenv("SDO_OPERATOR_ROOT") }
