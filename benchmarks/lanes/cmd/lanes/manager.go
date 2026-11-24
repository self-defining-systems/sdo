package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/bench"
)

// runner runs a command and returns its combined output; managerCommand reaches systemd only through it.
type runner func(name string, args ...string) (string, error)

func execRunner(name string, args ...string) (string, error) {
	b, err := exec.Command(name, args...).CombinedOutput()
	return strings.TrimSpace(string(b)), err
}

// managerUnit names the legacy service and provides the prefix for its replacements.
func managerUnit(root string) string {
	sum := sha256.Sum256([]byte(root))
	return "lanes-" + hex.EncodeToString(sum[:])[:10] + ".service"
}

// passedEnv lists the variables of the caller the manager needs: its PATH (ssh, kubectl), ssh agent, kubeconfig and
// the lanes settings. A systemd --user unit starts with almost none.
func passedEnv(environ []string) []string {
	var out []string
	for _, kv := range environ {
		k, _, _ := strings.Cut(kv, "=")
		if k == "PATH" || k == "SSH_AUTH_SOCK" || k == "KUBECONFIG" || strings.HasPrefix(k, "FUZZOPS_") || strings.HasPrefix(k, "LANES_") {
			out = append(out, "--setenv="+kv)
		}
	}
	return out
}

// managerCommand is `lanes manager start|stop|restart|status|logs --root R`: the manager (`lanes serve`) as a
// systemd --user unit, so nobody types systemctl. Stopping it leaves the steps running on the hosts.
func managerCommand(args []string, environ []string, exe string, out, errw io.Writer, run runner) int {
	getenv := func(k string) string {
		for _, kv := range environ {
			if v, ok := strings.CutPrefix(kv, k+"="); ok {
				return v
			}
		}
		return ""
	}
	fs := flag.NewFlagSet("manager", flag.ContinueOnError)
	fs.SetOutput(errw)
	root := fs.String("root", getenv("FUZZOPS_BENCH_ROOT"), "")
	noFollow := fs.Bool("no-follow", false, "")
	lines := fs.Int("n", 50, "")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return 2
	}
	if len(pos) != 1 || (pos[0] != "start" && pos[0] != "stop" && pos[0] != "restart" && pos[0] != "status" && pos[0] != "logs") {
		fmt.Fprintln(errw, "usage: lanes manager start|stop|restart|status|logs --root R")
		return 2
	}
	if *root == "" {
		fmt.Fprintln(errw, "--root (or $FUZZOPS_BENCH_ROOT) is required")
		return 2
	}
	abs, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(errw, err)
		return 2
	}
	m := &mgr{root: abs, exe: exe, environ: environ, out: out, errw: errw, run: run}
	if pos[0] == "logs" {
		return m.logs(*lines, !*noFollow)
	}
	if pos[0] != "status" {
		// Serialize discovery and mutation, including both halves of restart.
		// The directory flock covers the gap before serve takes bench.lock.
		dir, err := os.Open(abs)
		if err != nil {
			fmt.Fprintln(errw, err)
			return 1
		}
		defer dir.Close()
		if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			fmt.Fprintf(errw, "locking manager commands for %s: %v\n", abs, err)
			return 1
		}
	}
	if err := m.discover(); err != nil {
		fmt.Fprintln(errw, err)
		return 1
	}
	switch pos[0] {
	case "start":
		return m.start()
	case "stop":
		return m.stop()
	case "restart":
		if code := m.stop(); code != 0 {
			return code
		}
		return m.start()
	default:
		return m.status()
	}
}

type mgr struct {
	root, unit, exe string
	environ         []string
	out, errw       io.Writer
	run             runner
}

func (m *mgr) systemctl(args ...string) (string, error) {
	return m.run("systemctl", append([]string{"--user"}, args...)...)
}

// discover finds both legacy and generated units. Stopped units may stay loaded
// while detached steps keep their cgroups alive.
func (m *mgr) discover() error {
	legacy := managerUnit(m.root)
	prefix := strings.TrimSuffix(legacy, ".service") + "-"
	o, err := m.systemctl("list-units", "--all", "--plain", "--no-legend", "--no-pager", "--full", legacy, prefix+"*.service")
	if err != nil {
		return fmt.Errorf("listing manager units for %s: %w\n%s", m.root, err, o)
	}
	for _, row := range strings.Split(o, "\n") {
		fields := strings.Fields(row)
		if len(fields) < 4 {
			continue
		}
		switch fields[2] {
		case "active", "activating", "reloading", "deactivating":
			if m.unit != "" {
				return fmt.Errorf("multiple live manager units for %s: %s and %s", m.root, m.unit, fields[0])
			}
			m.unit = fields[0]
		}
	}
	return nil
}

func (m *mgr) start() int {
	held, holder := bench.ManagerHolder(bench.Root(m.root))
	switch {
	case held && m.unit != "":
		fmt.Fprintf(m.out, "manager already running: unit %s (%s)\n", m.unit, holder)
		return 0
	case held:
		fmt.Fprintf(m.errw, "a manager already holds %s/bench.lock (%s): stop that process first (kill it, or Ctrl-C its `lanes serve`); steps keep running\n", m.root, holder)
		return 1
	case m.unit != "":
		fmt.Fprintf(m.out, "manager unit %s is already starting\n", m.unit)
		return 0
	}
	m.lingerWarning()
	// Each launch gets its own name. KillMode=process lets steps keep running
	// in the old cgroup, which can keep the old unit name reserved.
	stem := strings.TrimSuffix(managerUnit(m.root), ".service") + "-" + strings.ToLower(rand.Text())
	m.unit = stem + ".service"
	args := []string{"--user", "--unit=" + stem, "--collect",
		"--description=lanes manager for " + m.root,
		"--property=Type=exec", "--property=Restart=on-failure", "--property=RestartSec=10",
		"--property=KillMode=process"}
	args = append(args, passedEnv(m.environ)...)
	args = append(args, "--", m.exe, "serve", "--root", m.root)
	if o, err := m.run("systemd-run", args...); err != nil {
		fmt.Fprintf(m.errw, "starting %s: %v\n%s\n", m.unit, err, o)
		return 1
	}
	fmt.Fprintf(m.out, "manager started: unit %s, root %s\nlogs: lanes manager logs --root %s\n", m.unit, m.root, m.root)
	return 0
}

// lingerWarning warns when the user's units die at logout.
func (m *mgr) lingerWarning() {
	user := os.Getenv("USER")
	if user == "" {
		return
	}
	o, err := m.run("loginctl", "show-user", user, "-p", "Linger")
	if err == nil && strings.TrimSpace(o) == "Linger=no" {
		fmt.Fprintf(m.errw, "warning: linger is off for %s, so the manager stops when you log out. Fix: sudo loginctl enable-linger %s\n", user, user)
	}
}

func (m *mgr) stop() int {
	if m.unit == "" {
		if held, holder := bench.ManagerHolder(bench.Root(m.root)); held {
			fmt.Fprintf(m.out, "a manager holds %s/bench.lock (%s): stop that process directly; steps keep running\n", m.root, holder)
		} else {
			fmt.Fprintln(m.out, "manager not running")
		}
		return 0
	}
	if o, err := m.systemctl("stop", m.unit); err != nil {
		fmt.Fprintf(m.errw, "stopping %s: %v\n%s\n", m.unit, err, o)
		return 1
	}
	fmt.Fprintf(m.out, "manager stopped: unit %s; steps keep running on their hosts\n", m.unit)
	m.unit = ""
	return 0
}

// status prints the unit state, the binary it runs and its revision, and the root. Exit 0 when running, 3 when not.
func (m *mgr) status() int {
	fmt.Fprintf(m.out, "root:     %s\n", m.root)
	code := 3
	if m.unit == "" {
		fmt.Fprintln(m.out, "state:    not running (no unit)")
	} else {
		code = m.unitStatus()
	}
	if held, holder := bench.ManagerHolder(bench.Root(m.root)); held {
		fmt.Fprintf(m.out, "lock:     held (%s)\n", holder)
	} else {
		fmt.Fprintln(m.out, "lock:     free")
	}
	return code
}

func (m *mgr) unitStatus() int {
	o, err := m.systemctl("show", m.unit, "-p", "LoadState,ActiveState,SubState,MainPID,NRestarts,ExecStart")
	if err != nil {
		fmt.Fprintf(m.errw, "checking %s: %v\n%s\n", m.unit, err, o)
		return 1
	}
	props := map[string]string{}
	for _, l := range strings.Split(o, "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			props[k] = v
		}
	}
	fmt.Fprintf(m.out, "unit:     %s\n", m.unit)
	running := props["ActiveState"] == "active"
	if props["LoadState"] != "loaded" {
		fmt.Fprintln(m.out, "state:    not running (no unit)")
	} else {
		fmt.Fprintf(m.out, "state:    %s (%s), pid %s, restarts %s\n", props["ActiveState"], props["SubState"], props["MainPID"], props["NRestarts"])
		if bin := execPath(props["ExecStart"]); bin != "" {
			fmt.Fprintf(m.out, "binary:   %s\n", bin)
			rev, err := m.run(bin, "version")
			if err != nil {
				rev = "unknown (" + err.Error() + ")"
			}
			fmt.Fprintf(m.out, "revision: %s\n", rev)
		}
	}
	if running {
		return 0
	}
	return 3
}

// execPath extracts the binary from systemd's ExecStart property: "{ path=/x/lanes ; argv[]=... ; ... }".
func execPath(exec string) string {
	_, rest, ok := strings.Cut(exec, "path=")
	if !ok {
		return ""
	}
	p, _, _ := strings.Cut(rest, " ;")
	return strings.TrimSpace(p)
}

// logs prints the last n lines of the manager log, then follows it when follow is set.
func (m *mgr) logs(n int, follow bool) int {
	args := []string{"-n", strconv.Itoa(n)}
	if follow {
		args = append(args, "-F")
	}
	args = append(args, m.root+"/bench.log")
	if follow {
		c := exec.Command("tail", args...)
		c.Stdout, c.Stderr = m.out, m.errw
		if err := c.Run(); err != nil {
			fmt.Fprintln(m.errw, err)
			return 1
		}
		return 0
	}
	o, err := m.run("tail", args...)
	if err != nil {
		fmt.Fprintln(m.errw, err)
		return 1
	}
	fmt.Fprintln(m.out, o)
	return 0
}

// revision is the VCS revision the binary was built from, "+dirty" when the tree had changes.
func revision() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	rev, dirty := "unknown", ""
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "+dirty"
			}
		}
	}
	return rev + dirty
}
