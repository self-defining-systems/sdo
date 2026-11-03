//go:build integration && controller_e2e

package e2e

import (
	"os"
	"path/filepath"
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
	entries, _ := os.ReadDir("/proc")
	killed := false
	for _, entry := range entries {
		pid := 0
		for _, c := range entry.Name() {
			if c < '0' || c > '9' {
				pid = -1
				break
			}
			pid = pid*10 + int(c-'0')
		}
		if pid <= 0 {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		argv := strings.Split(string(cmdline), "\x00")
		if len(argv) > 0 && strings.HasSuffix(argv[0], "/sdo-memory-host") {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			killed = true
		}
	}
	return killed
}

// Window is a period the run declares as a deliberate fault, excluded from the cadence check.
type Window struct{ From, To time.Time }
