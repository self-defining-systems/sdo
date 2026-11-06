package sandboxio

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
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

// peakWatch learns a child's peak resident set. The kernel's rusage
// ru_maxrss of a child starts at the parent's own high-water mark (the child
// is forked sharing the parent's memory and the mark is carried over exec), so
// a large parent would make every child look big. The watch therefore also
// samples the child's own VmHWM while it runs.
type peakWatch struct {
	pid     int
	stop    chan struct{}
	done    chan struct{}
	sampled int64
}

func watchPeak(pid int) *peakWatch {
	w := &peakWatch{pid: pid, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			if v := hwm(fmt.Sprintf("/proc/%d/status", pid)); v > w.sampled {
				w.sampled = v
			}
			select {
			case <-w.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return w
}

// finish stops the sampling and returns the peak: rusage's value when it is
// above anything the parent itself reached (so it is the child's own), else
// the largest sample, a lower bound.
func (w *peakWatch) finish(ps *os.ProcessState) int64 {
	close(w.stop)
	<-w.done
	if ru, ok := ps.SysUsage().(*syscall.Rusage); ok {
		if maxrss := ru.Maxrss << 10; maxrss > hwm("/proc/self/status") {
			return maxrss
		}
	}
	return w.sampled
}

// hwm reads VmHWM (bytes) from a /proc status file, 0 when unavailable.
func hwm(file string) int64 {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			f := strings.Fields(rest)
			if len(f) > 0 {
				if kb, err := strconv.ParseInt(f[0], 10, 64); err == nil {
					return kb << 10
				}
			}
		}
	}
	return 0
}
