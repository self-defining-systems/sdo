package sandboxio

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
)

// limitSelf caps the child's writable mappings at twice its heap limit
// (RLIMIT_DATA also counts heap the runtime has released but not unmapped)
// and makes it the OOM killer's first choice inside the controller's cgroup.
func limitSelf(memory int64) error {
	data := uint64(memory) * 2
	if err := syscall.Setrlimit(syscall.RLIMIT_DATA, &syscall.Rlimit{Cur: data, Max: data}); err != nil {
		return fmt.Errorf("set RLIMIT_DATA: %w", err)
	}
	// Best effort: raising one's own OOM score needs no privilege, but /proc
	// may be unavailable; the data limit is the bound.
	_ = os.WriteFile("/proc/self/oom_score_adj", []byte("1000"), 0)
	return nil
}

// childAttributes kill the child when the thread that started it exits.
func childAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}

// start keeps the starting goroutine on one OS thread: the parent-death
// signal follows the thread that forked, and Go does not end an unlocked
// thread that a goroutine merely used.
func start(cmd *exec.Cmd) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return cmd.Start()
}

// executable is the running binary even if its file was replaced on disk.
func executable() (string, error) { return "/proc/self/exe", nil }

// peakBytes is the child's peak resident set, 0 when unknown.
func peakBytes(ps *os.ProcessState) int64 {
	if ru, ok := ps.SysUsage().(*syscall.Rusage); ok {
		return ru.Maxrss << 10
	}
	return 0
}
