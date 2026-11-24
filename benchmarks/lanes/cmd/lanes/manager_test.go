package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// fakeSystemd answers the commands managerCommand runs and records them.
type fakeSystemd struct {
	active  bool
	unit    string
	stopped []string // units that still have steps in their cgroups
	linger  string
	calls   []string
	show    string
	listErr error
}

func (f *fakeSystemd) run(name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	switch {
	case strings.Contains(line, " list-units "):
		if f.listErr != nil {
			return "Failed to connect to bus", f.listErr
		}
		matches := func(unit string) bool {
			for _, pattern := range args[len(args)-2:] {
				if ok, _ := filepath.Match(pattern, unit); ok {
					return true
				}
			}
			return false
		}
		var rows []string
		for _, unit := range f.stopped {
			if matches(unit) {
				rows = append(rows, unit+" loaded inactive dead lanes manager")
			}
		}
		if f.active && matches(f.unit) {
			rows = append(rows, f.unit+" loaded active running lanes manager")
		}
		return strings.Join(rows, "\n"), nil
	case strings.Contains(line, " stop "):
		if f.active && args[2] == f.unit {
			f.stopped = append(f.stopped, f.unit)
			f.active = false
		}
	case name == "systemd-run":
		for _, arg := range args {
			if unit, ok := strings.CutPrefix(arg, "--unit="); ok {
				unit += ".service"
				for _, stopped := range f.stopped {
					if stopped == unit {
						return "Unit " + unit + " was already loaded or has a fragment file.", errors.New("exit 1")
					}
				}
				f.unit, f.active = unit, true
				break
			}
		}
	case name == "loginctl":
		return f.linger, nil
	case strings.Contains(line, " show "):
		if f.active && args[2] == f.unit {
			return f.show, nil
		}
		return "LoadState=not-found\nActiveState=inactive", nil
	case strings.HasSuffix(line, " version"):
		return "abc123", nil
	}
	return "", nil
}

func (f *fakeSystemd) ran(sub string) bool {
	for _, c := range f.calls {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

func runManager(t *testing.T, f *fakeSystemd, root string, args ...string) (code int, out, errs string) {
	t.Helper()
	t.Setenv("USER", "tester")
	if f.active && f.unit == "" {
		f.unit = managerUnit(root)
	}
	var o, e bytes.Buffer
	code = managerCommand(append(args, "--root", root), []string{"PATH=/bin", "FUZZOPS_X=1", "SECRET=no"}, "/opt/lanes", &o, &e, f.run)
	return code, o.String(), e.String()
}

func TestManagerUnitIsNamedFromRoot(t *testing.T) {
	a, b := managerUnit("/data/a"), managerUnit("/data/b")
	if a == b || !strings.HasPrefix(a, "lanes-") || !strings.HasSuffix(a, ".service") || a != managerUnit("/data/a") {
		t.Errorf("units %q %q", a, b)
	}
}

func TestManagerStartCreatesTheUnit(t *testing.T) {
	root := t.TempDir()
	f := &fakeSystemd{linger: "Linger=yes"}
	code, out, errs := runManager(t, f, root, "start")
	if code != 0 {
		t.Fatalf("exit %d: %s %s", code, out, errs)
	}
	var sr string
	for _, c := range f.calls {
		if strings.HasPrefix(c, "systemd-run ") {
			sr = c
		}
	}
	for _, want := range []string{"--user", "--unit=" + strings.TrimSuffix(managerUnit(root), ".service") + "-", "Restart=on-failure", "KillMode=process", "--setenv=PATH=/bin", "--setenv=FUZZOPS_X=1", "-- /opt/lanes serve --root " + root} {
		if !strings.Contains(sr, want) {
			t.Errorf("systemd-run %q lacks %q", sr, want)
		}
	}
	if strings.Contains(sr, "SECRET") || strings.Contains(errs, "linger") {
		t.Errorf("leaked env or spurious linger warning: %q / %q", sr, errs)
	}
}

func TestManagerStartWarnsWhenLingerIsOff(t *testing.T) {
	f := &fakeSystemd{linger: "Linger=no"}
	code, _, errs := runManager(t, f, t.TempDir(), "start")
	if code != 0 || !strings.Contains(errs, "sudo loginctl enable-linger tester") {
		t.Errorf("exit %d, stderr %q", code, errs)
	}
}

func TestManagerStartIsIdempotent(t *testing.T) {
	f := &fakeSystemd{active: true, linger: "Linger=yes"}
	code, out, _ := runManager(t, f, t.TempDir(), "start")
	if code != 0 || f.ran("systemd-run") || !strings.Contains(out, "starting") {
		t.Errorf("exit %d, calls %v", code, f.calls)
	}
}

// holdLock takes bench.lock like a running manager and writes its holder line.
func holdLock(t *testing.T, root, holder string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(root, "bench.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(holder + "\n")
}

func TestManagerStartRefusesASecondManagerAndNamesTheHolder(t *testing.T) {
	root := t.TempDir()
	holdLock(t, root, "pid 4242")
	f := &fakeSystemd{linger: "Linger=yes"}
	code, _, errs := runManager(t, f, root, "start")
	if code == 0 || f.ran("systemd-run") || !strings.Contains(errs, "pid 4242") {
		t.Errorf("exit %d, stderr %q, calls %v", code, errs, f.calls)
	}
}

func TestManagerStartNoticesItsOwnRunningUnit(t *testing.T) {
	root := t.TempDir()
	holdLock(t, root, "pid 4242")
	f := &fakeSystemd{active: true}
	code, out, _ := runManager(t, f, root, "start")
	if code != 0 || f.ran("systemd-run") || !strings.Contains(out, "already running") {
		t.Errorf("exit %d, out %q", code, out)
	}
}

func TestManagerStopLeavesStepsAlone(t *testing.T) {
	f := &fakeSystemd{active: true}
	code, out, _ := runManager(t, f, t.TempDir(), "stop")
	if code != 0 || !f.ran("systemctl --user stop lanes-") || !strings.Contains(out, "steps keep running") {
		t.Errorf("exit %d, out %q, calls %v", code, out, f.calls)
	}
	f = &fakeSystemd{}
	if code, out, _ = runManager(t, f, t.TempDir(), "stop"); code != 0 || f.ran(" stop ") || !strings.Contains(out, "not running") {
		t.Errorf("stop of a stopped manager: exit %d, out %q, calls %v", code, out, f.calls)
	}
}

func TestManagerRestartWhileOldUnitsStillHaveSteps(t *testing.T) {
	root := t.TempDir()
	legacy := managerUnit(root)
	f := &fakeSystemd{
		unit: legacy, active: true, linger: "Linger=yes",
		show: "LoadState=loaded\nActiveState=active\nSubState=running"}
	// The legacy service and every replacement leave a step running when stopped.
	previous := legacy
	for range 2 {
		code, out, errs := runManager(t, f, root, "restart")
		if code != 0 || !f.active || f.unit == previous || len(f.stopped) == 0 || f.stopped[len(f.stopped)-1] != previous {
			t.Fatalf("restart from %s: exit %d, %s%s; calls %v", previous, code, out, errs, f.calls)
		}
		if code, out, errs := runManager(t, f, root, "status"); code != 0 || !strings.Contains(out, f.unit) {
			t.Fatalf("status of %s: exit %d, %s%s", f.unit, code, out, errs)
		}
		previous = f.unit
	}
	if code, out, errs := runManager(t, f, root, "stop"); code != 0 || f.active || !f.ran(" stop "+previous) {
		t.Fatalf("stop of %s: exit %d, %s%s; calls %v", previous, code, out, errs, f.calls)
	}
}

func TestManagerRestartRefusesConcurrentLifecycleCommand(t *testing.T) {
	root := t.TempDir()
	dir, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	f := &fakeSystemd{active: true}
	if code, _, errs := runManager(t, f, root, "restart"); code == 0 || len(f.calls) != 0 || !strings.Contains(errs, root) {
		t.Fatalf("concurrent restart: exit %d, %s; calls %v", code, errs, f.calls)
	}
}

func TestManagerStartRequiresSuccessfulDiscovery(t *testing.T) {
	f := &fakeSystemd{listErr: errors.New("exit 1")}
	if code, _, errs := runManager(t, f, t.TempDir(), "start"); code == 0 || f.ran("systemd-run") || !strings.Contains(errs, "Failed to connect to bus") {
		t.Fatalf("failed discovery: exit %d, %s; calls %v", code, errs, f.calls)
	}
}

func TestManagerStatusShowsUnitBinaryRevisionAndRoot(t *testing.T) {
	root := t.TempDir()
	f := &fakeSystemd{active: true, show: "LoadState=loaded\nActiveState=active\nSubState=running\nMainPID=77\nNRestarts=1\nExecStart={ path=/opt/lanes ; argv[]=/opt/lanes serve --root " + root + " ; ignore_errors=no }"}
	code, out, _ := runManager(t, f, root, "status")
	for _, want := range []string{root, managerUnit(root), "active (running)", "pid 77", "/opt/lanes", "abc123"} {
		if !strings.Contains(out, want) {
			t.Errorf("status lacks %q: %q", want, out)
		}
	}
	if code != 0 {
		t.Errorf("exit %d", code)
	}
	f = &fakeSystemd{}
	if code, out, _ = runManager(t, f, root, "status"); code != 3 || !strings.Contains(out, "not running") {
		t.Errorf("not running: exit %d, %q", code, out)
	}
}

func TestManagerLogsPrintsTheManagerLog(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bench.log"), []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := &fakeSystemd{}
	code, _, _ := runManager(t, f, root, "logs", "--no-follow", "-n", "2")
	if code != 0 || !f.ran("tail -n 2 "+filepath.Join(root, "bench.log")) {
		t.Errorf("exit %d, calls %v", code, f.calls)
	}
}
