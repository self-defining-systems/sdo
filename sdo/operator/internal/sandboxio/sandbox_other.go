//go:build !linux

package sandboxio

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func limitSelf(memory int64) error {
	data := uint64(memory) * 2
	if err := syscall.Setrlimit(syscall.RLIMIT_DATA, &syscall.Rlimit{Cur: data, Max: data}); err != nil {
		return fmt.Errorf("set RLIMIT_DATA: %w", err)
	}
	return nil
}

func childAttributes() *syscall.SysProcAttr { return nil }

func start(cmd *exec.Cmd) error { return cmd.Start() }

func executable() (string, error) { return os.Executable() }

// peakBytes is unknown here: thread-creation failures stay crashes.
func peakBytes(*os.ProcessState) int64 { return 0 }
