//go:build integration

package deploy_test

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// cgroup and sysfs files report size 0 whatever they hold, so a size test (`[ -s file ]`) reads them as empty. Found live:
// every slice was reported as not excluded although its AllowedCPUs were right. A named pipe has the same trait here: it
// is size 0 and has content only when read.
func TestSliceCpusAreReadFromFilesThatReportSizeZero(t *testing.T) {
	b := blockSetup(t)
	b.must("block", "reserve", "--cpus", "40-55", "--memory", "8G", "--host-reserve-cpus", "2")
	dir := filepath.Join(b.cg, "pipe.slice")
	os.MkdirAll(dir, 0o755)
	fifo := filepath.Join(dir, "cpuset.cpus.effective")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Skip("no fifo:", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
				f.WriteString("0-39\n")
				f.Close()
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		if f, err := os.OpenFile(fifo, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil { // lets a writer blocked in open() return
			f.Close()
		}
	})
	st := b.must("block", "status")
	if strings.Contains(st, "pipe.slice") || !strings.Contains(st, "two-way yes") {
		t.Fatalf("a slice whose effective cpus (0-39) are outside the block, read from a file of size 0, is excluded:\n%s", st)
	}
}
