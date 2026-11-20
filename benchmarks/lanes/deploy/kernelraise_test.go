//go:build integration

package deploy_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `kernel raise KEY=MIN...` writes /etc/sysctl.d/99-zz-lanes.conf and applies it, and only ever raises: a value already
// above its minimum is neither lowered live nor lowered in the file, and what the file already holds is kept if higher.
func TestKernelRaiseOnlyRaises(t *testing.T) {
	b := blockSetup(t)
	fake := filepath.Join(b.dir, "bin", "sysctl")
	script := "#!/bin/sh\necho \"sysctl $*\" >> \"$SC_STATE/sysctl.log\"\ncase \"$1\" in\n-n) cat \"$SC_STATE/sysctl.$2\" ;;\n-w) k=${2%%=*}; printf '%s\\n' \"${2#*=}\" > \"$SC_STATE/sysctl.$k\" ;;\nesac\n"
	os.WriteFile(fake, []byte(script), 0o755)
	d := filepath.Join(b.dir, "sysctl.d")
	os.MkdirAll(d, 0o755)
	os.WriteFile(filepath.Join(b.sc, "sysctl.fs.inotify.max_user_instances"), []byte("512\n"), 0o644)
	os.WriteFile(filepath.Join(b.sc, "sysctl.vm.max_map_count"), []byte("1048576\n"), 0o644)
	os.WriteFile(filepath.Join(b.sc, "sysctl.fs.inotify.max_user_watches"), []byte("524288\n"), 0o644)
	os.WriteFile(filepath.Join(d, "99-zz-lanes.conf"), []byte("fs.inotify.max_user_watches = 4000000\n"), 0o644)
	b.vars = append(b.vars, "SDO_EVAL_SYSCTL="+fake, "SDO_EVAL_SYSCTL_D="+d)
	b.must("kernel", "raise", "fs.inotify.max_user_instances=8192", "vm.max_map_count=262144", "fs.inotify.max_user_watches=1048576")
	if got := cgfile(t, b.sc, "sysctl.fs.inotify.max_user_instances"); got != "8192" {
		t.Errorf("a value below its minimum is raised live: %q", got)
	}
	if got := cgfile(t, b.sc, "sysctl.vm.max_map_count"); got != "1048576" {
		t.Errorf("a value above its minimum is never lowered: %q", got)
	}
	conf, _ := os.ReadFile(filepath.Join(d, "99-zz-lanes.conf"))
	for _, want := range []string{"fs.inotify.max_user_instances = 8192", "vm.max_map_count = 262144", "fs.inotify.max_user_watches = 4000000"} {
		if !strings.Contains(string(conf), want) {
			t.Errorf("the file lacks %q (a higher value already in the file is kept):\n%s", want, conf)
		}
	}
	if got := cgfile(t, b.sc, "sysctl.fs.inotify.max_user_watches"); got != "4000000" {
		t.Errorf("a live value below what the file holds is raised to the file's value: %q", got)
	}
	log, _ := os.ReadFile(filepath.Join(b.sc, "sysctl.log"))
	if strings.Contains(string(log), "-w vm.max_map_count") {
		t.Errorf("a value that is high enough is not written live:\n%s", log)
	}
	if out, err := b.do("kernel", "raise", "kernel.core_pattern=1"); err == nil {
		t.Errorf("a key outside the helper's list must be refused: %s", out)
	}
	if out, err := b.do("kernel", "raise", "vm.max_map_count=abc"); err == nil {
		t.Errorf("a value that is not a number must be refused: %s", out)
	}
}
