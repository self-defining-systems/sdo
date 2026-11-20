//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Kernel settings are read from /proc/sys, which every user can read: `sysctl` is in /usr/sbin, not on the PATH of a
// plain ssh session (found live on chelan4, where the reading was silently missing).
func TestKernelSettingsAreReadFromProcSysWithoutSysctlOnThePath(t *testing.T) {
	proc := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(proc, "fs", "inotify"), 0o755))
	must(t, os.WriteFile(filepath.Join(proc, "fs", "inotify", "max_user_instances"), []byte("512\n"), 0o644))
	t.Setenv("HOST_CANARY_PROCSYS", proc)
	conf := filepath.Join(proc, "sysctl.conf")
	must(t, os.WriteFile(conf, []byte("# lanes\nfs.inotify.max_user_instances = 512\n"), 0o644))
	t.Setenv("HOST_CANARY_SYSCTL_FILES", conf)
	t.Setenv("PATH", "/usr/bin:/bin") // no sbin, as over ssh
	mins := Host{}.KernelMins()
	c, err := localCanary(context.Background(), 10*time.Second, mins)
	if err != nil || c.Sysctl["fs.inotify.max_user_instances"] != 512 {
		t.Fatalf("%+v %v", c, err)
	}
	if _, ok := c.Sysctl["vm.max_map_count"]; ok {
		t.Fatalf("a setting that cannot be read is absent, not 0: %+v", c.Sysctl)
	}
	if why := kernelLow(c, mins); !strings.Contains(why, "max_user_instances is 512") || !strings.Contains(why, conf+":2:") {
		t.Fatalf("%q", why)
	}
	if got := unreadKernelSettings(c, mins); len(got) != 2 {
		t.Fatalf("the two settings that were not read are reported as such: %v", got)
	}
}
