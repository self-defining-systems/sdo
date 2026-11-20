//go:build integration

package bench

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// `lanes host status` reads the effective values and says which is low and where a file sets it.
func TestHostStatusNamesALowKernelSetting(t *testing.T) {
	root, _ := hostsForReserve(t, `reserve: {cpus: "88-111"}`)
	f := &fakeHost{block: "88-111", other: "0-87,112-255"}
	run := func(_ context.Context, _ string, script string) (string, error) {
		if !strings.Contains(script, "KMIN=") {
			return "", nil // the container probe
		}
		if !strings.Contains(script, "fs.inotify.max_user_instances=8192") {
			t.Errorf("the probe must ask for the minimums:\n%s", script)
		}
		return "sysctl fs.inotify.max_user_instances 512\noverride fs.inotify.max_user_instances /etc/sysctl.conf:9:fs.inotify.max_user_instances=512\nrdma-shared\n{\"rc\":0,\"ip_ms\":3,\"hung\":0}\n", nil
	}
	var out, errw bytes.Buffer
	HostCommand(context.Background(), root, []string{"status", "h1"}, &out, &errw, f.helper, run)
	for _, want := range []string{"NOT ADMITTED", "max_user_instances is 512", "/etc/sysctl.conf:9", "RDMA"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status lacks %q:\n%s", want, out.String())
		}
	}
}
