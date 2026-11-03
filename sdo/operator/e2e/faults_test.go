//go:build integration && controller_e2e

package e2e

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// SetAgentMode changes what the next launched agent does: "normal", "crash" (exit 3) or "hang".
func (w *World) SetAgentMode(mode string) {
	w.T.Helper()
	if err := os.WriteFile(filepath.Join(w.Control, "agent-mode"), []byte(mode), 0o666); err != nil {
		w.T.Fatal(err)
	}
}

// KillController sends SIGKILL to the controller container's whole sandbox (a kill -9 of the process tree).
func (w *World) KillController() {
	w.T.Helper()
	if n := w.Kubelet.Run.Signal(ControlNamespace, "sdo-controller-run", syscall.SIGKILL); n == 0 {
		w.T.Fatal("no running controller to kill")
	}
}

// KillMemoryHost sends SIGKILL to the controller's memory-host subprocess and reports whether one was found.
func (w *World) KillMemoryHost() bool {
	w.T.Helper()
	r := w.Kubelet.Run
	r.mu.Lock()
	defer r.mu.Unlock()
	var roots []int
	for key, cmd := range r.procs {
		if strings.HasPrefix(key, ControlNamespace+"/sdo-controller-run") && cmd.Process != nil && cmd.Process.Signal(syscall.Signal(0)) == nil {
			roots = append(roots, cmd.Process.Pid)
		}
	}
	killed := false
	for _, pid := range descendantPIDs("/proc", roots, "sdo-memory-host") {
		process, err := os.FindProcess(pid)
		if err == nil && process.Signal(syscall.SIGKILL) == nil {
			killed = true
		}
		if process != nil {
			_ = process.Release()
		}
	}
	return killed
}

// descendantPIDs searches only process trees rooted in this Runtime's sandboxes.
// A global executable-name scan would also find hosts belonging to other tests
// or real cluster lanes. Read every thread's children: Go starts subprocesses
// from threads other than the process leader.
func descendantPIDs(procRoot string, roots []int, executable string) []int {
	queue := append([]int(nil), roots...)
	seen := map[int]bool{}
	for _, pid := range roots {
		seen[pid] = true
	}
	var matches []int
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		tasks := filepath.Join(procRoot, strconv.Itoa(pid), "task")
		threads, _ := os.ReadDir(tasks)
		for _, thread := range threads {
			children, _ := os.ReadFile(filepath.Join(tasks, thread.Name(), "children"))
			for _, value := range strings.Fields(string(children)) {
				child, err := strconv.Atoi(value)
				if err != nil || child <= 0 || seen[child] {
					continue
				}
				seen[child] = true
				queue = append(queue, child)
				cmdline, _ := os.ReadFile(filepath.Join(procRoot, value, "cmdline"))
				argv0, _, _ := strings.Cut(string(cmdline), "\x00")
				if filepath.Base(argv0) == executable {
					matches = append(matches, child)
				}
			}
		}
	}
	return matches
}

// Window is a period the run declares as a deliberate fault, excluded from the cadence check.
type Window struct{ From, To time.Time }
