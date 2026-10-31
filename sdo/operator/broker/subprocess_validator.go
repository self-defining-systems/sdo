package broker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/sdk/sdktest"
	"strconv"
	"strings"
	"syscall"
	"time"

	"sdo.dev/operator/memory/validate"
)

// ValidationInfrastructureError distinguishes resource/transport failures from
// authored-content diagnostics. It must never consume a proposal retry.
type ValidationInfrastructureError struct{ Message string }

func (e *ValidationInfrastructureError) Error() string { return e.Message }
func (*ValidationInfrastructureError) ExitCode() int   { return 2 }

// SubprocessValidator uses the running controller executable and SDK. The
// broker already owns a fresh staged clone; compiler scratch is private too.
type SubprocessValidator struct {
	Binary, OperatorRoot, HealthyBaseline string
	Timeout                               time.Duration
	MemoryBytes                           int64
}

func (v SubprocessValidator) Validator() Validator {
	return func(ctx context.Context, candidate string, paths []string) error {
		binary := v.Binary
		if binary == "" {
			var err error
			binary, err = os.Executable()
			if err != nil {
				return err
			}
		}
		root := v.OperatorRoot
		if root == "" {
			root = os.Getenv("SDO_OPERATOR_ROOT")
			if root == "" {
				root = "/opt/sdo/sdo/operator"
			}
		}
		root, err := filepath.Abs(root)
		if err != nil {
			return err
		}
		args := []string{"validation-child", candidate, root}
		needed, err := validate.BaselineGateRequired(candidate, paths)
		if err != nil {
			return err
		}
		baseline := ""
		if needed {
			baseline = v.HealthyBaseline
		}
		args = append(args, baseline)
		scratch, err := os.MkdirTemp("", "sdo-validation-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(scratch)
		timeout := v.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Minute
		}
		memory := v.MemoryBytes
		if memory <= 0 {
			memory = 3 << 30
		}
		return runValidationProcess(ctx, binary, args, scratch, timeout, memory)
	}
}

// RunValidationChild is called before normal launch by both the operator CLI
// and synthesized controllers, so the child always runs the parent's binary.
func RunValidationChild() {
	if len(os.Args) < 2 || os.Args[1] != "validation-child" {
		return
	}
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "invalid validation child arguments")
		os.Exit(2)
	}
	var notes []sdktest.MitigationReplayResult
	ds, err := validate.CompileCandidate(context.Background(), validate.Request{MitigationReplayNotes: &notes, Root: os.Args[2], OperatorRoot: os.Args[3], HealthyBaseline: os.Args[4], Layers: validate.Compile})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err = validate.WriteReport(os.Stdout, ds, notes...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(ds) > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

func runValidationProcess(ctx context.Context, binary string, args []string, scratch string, timeout time.Duration, memory int64) error {
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
