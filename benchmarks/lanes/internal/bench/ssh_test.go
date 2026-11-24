//go:build integration

package bench

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A remote host: steps start over ssh (setsid), inputs are shipped by
// content, results come home by rsync and are checked against the step's
// manifest. ssh here is a stand-in that runs the command locally.
func TestBenchRemoteHost(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("no rsync")
	}
	l := newLab(t)
	dir := filepath.Dir(string(l.root))
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\nshift\nexec sh -c \"$*\"\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	// The remote host's own fuzzops checkout, with fuzzops-bench built beside bin/fuzzops.
	src := testdata("fakefuzzops")
	remote := filepath.Join(dir, "remote-fuzzops")
	must(t, exec.Command("cp", "-r", src, remote).Run())
	useLanesExecutable(t, benchBin) // the remote step binary is the lanes install's, shipped from this executable
	inv, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, err)
	// Each edit must apply: a lab inventory that gains a field must not turn this into a silent local run.
	edit := func(s, old, new string) string {
		if !strings.Contains(s, old) {
			t.Fatalf("lab inventory has no %q:\n%s", old, s)
		}
		return strings.Replace(s, old, new, 1)
	}
	s := edit(string(inv), "local: true,", "ssh: fakehost,")
	s = edit(s, "staging: "+dir+"/staging", "staging: "+dir+"/remote-staging")
	fake := testdata("fakefuzzops")
	s = edit(s, "{h1: "+fake+"}", "{h1: "+remote+"}")
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(s), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1, 2]]", 0)
	l.until("both runs done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane released", idle(l))
	x := l.exp("e1")
	if _, err := os.Stat(filepath.Join(l.root.RunDir("e1", RunID("a@x", 1, 2, 1)), "record.json")); err != nil {
		t.Fatalf("record not home: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "remote-staging", "inputs", x.Lock.Instances.SHA256)); err != nil {
		t.Fatalf("instances not staged on the host: %v", err)
	}
	// What stays is the lane's own state and the hook input its prepared record names; no step's directory.
	hookInputs := 0
	ents, _ := os.ReadDir(filepath.Join(dir, "remote-staging", "c1"))
	for _, e := range ents {
		switch name := e.Name(); {
		case name == "step.lock" || name == "lane" || name == freshFingerprintName:
		case strings.HasSuffix(name, hookInputDirName("")):
			hookInputs++
		default:
			t.Errorf("staging not cleaned: %s", name)
		}
	}
	if hookInputs > 1 {
		t.Errorf("%d hook inputs kept, want at most the prepared one", hookInputs)
	}
	l.noFault()
}

// Starting a step returns at once, not when the step ends: an ssh session
// (here a shell whose output is read to its end) must not wait for the step.
func TestDetachReturnsWhileTheStepRuns(t *testing.T) {
	dir, err := os.MkdirTemp("", "bench-detach-") // not t.TempDir: the step writes into it after the test
	must(t, err)
	bin := filepath.Join(dir, "slow")
	must(t, os.WriteFile(bin, []byte("#!/bin/sh\nsleep 5\n"), 0o755))
	start := time.Now()
	var out []byte
	out, err = exec.Command("sh", "-c", detach(bin, filepath.Join(dir, "step.json"))).Output()
	if err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("start waited %s for the step", d)
	}
}

// The disk floor must not depend on how old the pinned fuzzops binary a probe runs is: the probe goes through the
// first (oldest) pin, and a pin from before free_bytes reports none. The host's free space is then read directly.
func TestProbeReportsFreeDiskWhenThePinnedBinaryDoesNot(t *testing.T) {
	dir := t.TempDir()
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\nshift\nexec sh -c \"$*\"\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	pin := filepath.Join(dir, "old-pin")
	useFakeLanes(t, "#!/bin/sh\necho '{\"load5\":3,\"steps\":{}}'\n")
	h := sshHost{name: "h", h: Host{SSH: "fakehost", Staging: filepath.Join(dir, "staging")}}
	p, err := h.Probe(context.Background(), map[string]string{}, []string{pin})
	must(t, err)
	if p.Load5 != 3 {
		t.Fatalf("load5 %v, want the binary's 3", p.Load5)
	}
	if p.Free <= 0 {
		t.Fatalf("free %d: the manager has no disk reading for a host whose pinned binary predates it", p.Free)
	}
}

// A reachable host that has no workload checkout for any pin cannot take items of those pins, but it is not
// unreachable: its probe answers (load, disk, steps) and raises no host alarm.
func TestProbeOfAHostWithoutAWorkloadCheckoutIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\nshift\nexec sh -c \"$*\"\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	useFakeLanes(t, "#!/bin/sh\necho '{\"load5\":2,\"steps\":{}}'\n")
	h := sshHost{name: "h", h: Host{SSH: "fakehost", Staging: filepath.Join(dir, "staging")}}
	p, err := h.Probe(context.Background(), map[string]string{}, nil)
	if err != nil {
		t.Fatalf("a host with no workload checkout is reachable, got an error: %v", err)
	}
	if p.Load5 != 2 {
		t.Fatalf("load5 %v, want the probe's 2", p.Load5)
	}
}

// A spec whose pin a host has no checkout of is refused there per pin, in words that name the pin and the cause.
func TestUnusableHostsNameThePinAndTheMissingCheckout(t *testing.T) {
	inv, err := loadInv(t, `hosts:
  a: {local: true, staging: /s, kubeconfigs: /k, evidence: /e, max_builds: 1, max_load5: 10, clusters: [c1]}
  b: {local: false, ssh: b, staging: /s, kubeconfigs: /k, evidence: /e, max_builds: 1, max_load5: 10, clusters: [c2]}
checkouts:
  fuzzops: {0b528ac: {a: /fz}}
`)
	must(t, err)
	s := Spec{Fuzzops: "0b528ac", Hosts: []string{"a", "b"}}
	got := inv.UnusableReasons(s)
	if len(got) != 1 || got[0] != "b: not usable for fuzzops@0b528ac: no checkout" {
		t.Fatalf("reasons = %q", got)
	}
}

// A host behind a jump host: every ssh and rsync of the host goes through it.
func TestSSHJumpIsUsedForCommandsAndRsync(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\necho \"$*\" >> "+argsFile+"\nlast=\"\"; for a; do last=$a; done\nexec sh -c \"$last\"\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	h := sshHost{name: "h", h: Host{SSH: "inner", SSHJump: "jumper", Staging: filepath.Join(dir, "st")}}
	if _, err := h.run(context.Background(), nil, "true"); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(argsFile); !strings.Contains(string(got), "-J jumper inner true") {
		t.Fatalf("ssh args %q: the jump host is missing", got)
	}
	if rs := h.sshForRsync(); !strings.Contains(rs, "-J jumper") || !strings.HasPrefix(rs, fakeSSH) {
		t.Fatalf("rsync -e %q", rs)
	}
	if plain := (sshHost{h: Host{SSH: "inner"}}).sshForRsync(); strings.Contains(plain, "-J") {
		t.Fatalf("no jump configured, got %q", plain)
	}
}
