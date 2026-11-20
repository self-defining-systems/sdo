package bench

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

// ManagerHolder reports whether a manager holds <root>/bench.lock and, when it does, who: "pid N" as the manager
// wrote it when it took the lock ("pid unknown" for a holder that wrote nothing).
func ManagerHolder(root Root) (held bool, holder string) {
	f, err := os.OpenFile(root.path("bench.lock"), os.O_RDONLY, 0)
	if err != nil {
		return false, ""
	}
	defer f.Close()
	if syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB) == nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return false, ""
	}
	b, _ := os.ReadFile(f.Name())
	holder = strings.TrimSpace(string(b))
	if holder == "" {
		holder = "pid unknown"
	}
	return true, holder
}

// writeHolder records this process in the lock file the manager holds.
func writeHolder(f *os.File) {
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("pid %d\n", os.Getpid())), 0)
}
