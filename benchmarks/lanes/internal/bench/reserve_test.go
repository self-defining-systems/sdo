//go:build integration

package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeHost stands in for the root helper's host: a block, the slices' AllowedCPUs and the containers docker lists.
type fakeHost struct {
	block, other string // lanes.slice and the other slices' AllowedCPUs
	calls        []string
	fail         error
}

func (f *fakeHost) helper(_ context.Context, _ string, args ...string) (string, error) {
	f.calls = append(f.calls, strings.Join(args, " "))
	if f.fail != nil {
		return "", f.fail
	}
	switch strings.Join(args[:2], " ") {
	case "block reserve":
		for i, a := range args {
			if a == "--cpus" {
				f.block = args[i+1]
			}
		}
		if f.block == "" {
			f.block = "88-111"
		}
		f.other = "0-87,112-255"
	case "block unreserve":
		f.block, f.other = "", ""
	case "block adopt":
		f.block = args[3]
	}
	two := "no"
	if f.block != "" && f.other != "" && !overlaps(f.other, f.block) {
		two = "yes"
	}
	b := f.block
	if b == "" {
		b = "-"
	}
	return fmt.Sprintf("block %s\ntwo-way %s\nslice user.slice AllowedCPUs=%s\nslice lanes.slice AllowedCPUs=%s\n", b, two, f.other, f.block), nil
}

func overlaps(a, b string) bool {
	x, _ := ParseCPUList(a)
	y, _ := ParseCPUList(b)
	for _, i := range x {
		for _, j := range y {
			if i == j {
				return true
			}
		}
	}
	return false
}

// writeHosts writes a one-host root whose host has the given extra settings.
func writeHosts(t *testing.T, extra string) Root {
	t.Helper()
	dir := t.TempDir()
	body := "hosts:\n  h1: {local: true, " + extra + "staging: " + dir + "/s, max_builds: 1, max_load5: 40, kubeconfigs: " + dir + "/k, evidence: " + dir + "/e, clusters: [c1]}\n"
	must(t, os.WriteFile(filepath.Join(dir, "hosts.yaml"), []byte(body), 0o644))
	return Root(dir)
}

func hostsForReserve(t *testing.T, reserve string) (Root, Inventory) {
	t.Helper()
	root := writeHosts(t, reserve+", ")
	inv, err := LoadInventory(string(root))
	must(t, err)
	return root, inv
}

func TestReserveConfigValidation(t *testing.T) {
	two := 3
	for name, tc := range map[string]struct {
		r  *HostReserve
		ok bool
	}{
		"cpus":           {&HostReserve{CPUs: "88-111,200-223", Memory: "400Gi", HostReserveCPUs: 2}, true},
		"cores and numa": {&HostReserve{Cores: 24, NUMA: &two}, true},
		"both":           {&HostReserve{CPUs: "1-2", Cores: 2, NUMA: &two}, false},
		"neither":        {&HostReserve{Memory: "1G"}, false},
		"cores no numa":  {&HostReserve{Cores: 4}, false},
		"bad list":       {&HostReserve{CPUs: "9-3"}, false},
		"bad memory":     {&HostReserve{CPUs: "1-2", Memory: "lots"}, false},
	} {
		if err := tc.r.validate(); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
	if got := strings.Join((HostReserve{Cores: 24, NUMA: &two, Memory: "400Gi", HostReserveCPUs: 2}).args(), " "); got != "--cores 24 --numa 3 --memory 400Gi --host-reserve-cpus 2" {
		t.Fatal(got)
	}
	if _, err := hostsInventory(t, "{cpus: \"1-2\", cores: 2, numa: 0}"); err == nil {
		t.Fatal("hosts.yaml with both forms must be refused")
	}
}

func hostsInventory(t *testing.T, reserve string) (Inventory, error) {
	t.Helper()
	return LoadInventory(string(writeHosts(t, "reserve: "+reserve+", ")))
}

func TestIsolationAdmission(t *testing.T) {
	h := Host{}
	rs := map[string]Reservation{"r2": {Host: "r2", Block: "8-15", TwoWay: true}, "r1": {Host: "r1", Block: "8-15"}}
	for name, tc := range map[string]struct {
		host, spec, drift string
		wantMode, want    string
	}{
		"no reservation":       {host: "h", wantMode: "", want: "no active reservation"},
		"one-way-ok by spec":   {host: "h", spec: IsolationOneWayO, wantMode: IsolationOneWayO},
		"two-way":              {host: "r2", wantMode: IsolationTwoWay},
		"one-way":              {host: "r1", wantMode: IsolationOneWay},
		"spec needs two-way":   {host: "r1", spec: IsolationTwoWay, want: "requires two-way"},
		"drifted host":         {host: "r2", drift: "AllowedCPUs changed", want: "drifted"},
		"two-way on two-way":   {host: "r2", spec: IsolationTwoWay, wantMode: IsolationTwoWay},
		"one-way-ok but drift": {host: "r2", spec: IsolationOneWayO, drift: "x", want: "drifted"},
	} {
		mode, _, why := isolationOf(h, Spec{Isolation: tc.spec}, rs, tc.host, map[string]string{tc.host: tc.drift})
		if mode != tc.wantMode || (tc.want == "") != (why == "") || !strings.Contains(why, tc.want) {
			t.Errorf("%s: mode %q why %q", name, mode, why)
		}
	}
	if m, _, why := isolationOf(Host{Isolation: IsolationOneWayO}, Spec{}, nil, "h", nil); m != IsolationOneWayO || why != "" {
		t.Errorf("host-level one-way-ok: %q %q", m, why)
	}
}

func TestHostReserveStatusAdoptUnreserve(t *testing.T) {
	root, inv := hostsForReserve(t, `reserve: {cpus: "88-111", memory: 400Gi, host_reserve_cpus: 2}`)
	_ = inv
	f := &fakeHost{}
	var out, errw bytes.Buffer
	runner := func(_ context.Context, _, script string) (string, error) {
		return "lanes-c1_.slice 88-89\nfuzzy-db docker 90-91\nother x 1-2\nlane2 eval.slice 3-4\n", nil
	}
	ctx := context.Background()
	if rc := HostCommand(ctx, root, []string{"reserve", "h1"}, &out, &errw, f.helper, runner); rc != 0 {
		t.Fatalf("%d %s %s", rc, out.String(), errw.String())
	}
	if !strings.Contains(f.calls[0], "block reserve --cpus 88-111 --memory 400Gi --host-reserve-cpus 2") || !strings.Contains(out.String(), "reserved 88-111 (two-way)") {
		t.Fatalf("%v\n%s", f.calls, out.String())
	}
	rs, _ := root.LoadReservations()
	if rs["h1"].Block != "88-111" || !rs["h1"].TwoWay {
		t.Fatalf("%+v", rs)
	}
	out.Reset()
	HostCommand(ctx, root, []string{"reserve", "h1"}, &out, &errw, f.helper, runner) // idempotent from the bench's side
	out.Reset()
	HostCommand(ctx, root, []string{"status", "h1"}, &out, &errw, f.helper, runner)
	for _, want := range []string{"block 88-111, two-way", "foreign container fuzzy-db is pinned to 90-91, inside the block", "our container lane2 is pinned to 3-4, outside the block"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status lacks %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "other") {
		t.Errorf("a foreign container outside the block is not reported:\n%s", out.String())
	}
	// drift shows in status
	f.other = "0-255"
	out.Reset()
	HostCommand(ctx, root, []string{"status", "h1"}, &out, &errw, f.helper, runner)
	if !strings.Contains(out.String(), "DRIFT") {
		t.Errorf("drift not shown:\n%s", out.String())
	}
	// unreserve refuses while a lane runs, unless --drain
	must(t, writeJSON(root.LanePath("c1"), LaneFile{Lane: "c1", Host: "h1", State: LaneRunning, Since: time.Now()}))
	if rc := HostCommand(ctx, root, []string{"unreserve", "h1"}, &out, &errw, f.helper, runner); rc == 0 || !strings.Contains(errw.String(), "lanes are running") || f.block == "" {
		t.Fatalf("must refuse: %d %s", rc, errw.String())
	}
	if rc := HostCommand(ctx, root, []string{"unreserve", "h1", "--drain"}, &out, &errw, f.helper, runner); rc != 0 || f.block != "" {
		t.Fatalf("--drain must go on: %d %s", rc, errw.String())
	}
	if rs, _ := root.LoadReservations(); len(rs) != 0 {
		t.Fatalf("record kept: %+v", rs)
	}
	// adopt records a block that exists
	f2 := &fakeHost{block: "88-111", other: "0-87,112-255"}
	out.Reset()
	if rc := HostCommand(ctx, root, []string{"adopt", "h1"}, &out, &errw, f2.helper, runner); rc != 0 {
		t.Fatalf("%d %s", rc, errw.String())
	}
	if rs, _ := root.LoadReservations(); !rs["h1"].Adopted || rs["h1"].Block != "88-111" {
		t.Fatalf("%+v", rs)
	}
}

// Drift stops admission on the host until the slices match their record again, and raises an alarm.
func TestDriftStopsAdmission(t *testing.T) {
	l := newLab(t)
	f := &fakeHost{block: "8-15", other: "0-7,16-31"}
	m := l.m
	m.Helper = f.helper
	m.DriftEvery = time.Nanosecond
	must(t, writeJSON(l.root.reservationPath("h1"), Reservation{Host: "h1", Block: "8-15", TwoWay: true}))
	must(t, m.loadInventory())
	m.checkReservations(context.Background())
	if why := m.drift["h1"]; why != "" {
		t.Fatalf("matching slices drifted: %s", why)
	}
	s := Spec{Hosts: []string{"h1"}}
	f.other = "0-31" // someone widened user.slice
	time.Sleep(time.Millisecond)
	m.checkReservations(context.Background())
	if m.drift["h1"] == "" || !strings.Contains(l.readRoot("alarms.jsonl"), "reservation-drift") {
		t.Fatalf("no drift alarm: %v", m.drift)
	}
	if _, _, why := isolationOf(m.inv.Hosts["h1"], s, m.reservations, "h1", m.drift); !strings.Contains(why, "drifted") {
		t.Fatalf("admission not stopped: %q", why)
	}
	f.other = "0-7,16-31"
	time.Sleep(time.Millisecond)
	m.checkReservations(context.Background())
	if m.drift["h1"] != "" {
		t.Fatalf("still drifted: %s", m.drift["h1"])
	}
	f.fail = errors.New("ssh down")
	time.Sleep(time.Millisecond)
	m.checkReservations(context.Background())
	if m.drift["h1"] == "" {
		t.Fatal("an unreadable host is not admitted")
	}
}

type captureOps struct {
	HostOps
	steps []Step
}

func (c *captureOps) Start(_ context.Context, s Step) error { c.steps = append(c.steps, s); return nil }

// Every lane step of a reserved host carries the block (so its cpuset must be inside it) and starts inside the lane's cgroup.
func TestReservedHostStepsRunInTheLaneCgroup(t *testing.T) {
	l := newLab(t)
	m := l.m
	must(t, m.loadInventory())
	m.reservations = map[string]Reservation{"h1": {Host: "h1", Block: "8-15", TwoWay: true}}
	ops := &captureOps{}
	m.Ops = func(string, Host) HostOps { return ops }
	t.Cleanup(func() { m.Ops = nil })
	m.lanes = map[string]*LaneFile{"c1": {Lane: "c1", Host: "h1", State: LaneFree}}
	m.exps = map[string]*Experiment{}
	for _, kind := range []string{StepBuild, StepRun, StepRelease, StepUnhold, StepRecover} {
		if !m.start(context.Background(), m.lanes["c1"], Step{Kind: kind, Fuzzops: "/fz"}) {
			t.Fatal(kind)
		}
	}
	if len(ops.steps) != 5 {
		t.Fatalf("%d steps", len(ops.steps))
	}
	for _, s := range ops.steps {
		pre := laneExecPrefix(s)
		if s.Limits == nil || s.Limits.Block != "8-15" || s.Limits.CPUs != "8-15" || s.IsolationMode != IsolationTwoWay {
			t.Errorf("%s: limits %+v mode %q", s.Kind, s.Limits, s.IsolationMode)
		}
		if !strings.Contains(pre, "lane-exec 'c1' -- ") || !strings.HasPrefix(pre, "sudo -n --preserve-env "+HelperPath) {
			t.Errorf("%s: step is not started in the lane cgroup: %q", s.Kind, pre)
		}
		line := detach("/fz/bin/lanes", "/st/step.json", pre)
		if !strings.Contains(line, "setsid -f sudo -n --preserve-env") || !strings.Contains(line, "lane-exec 'c1' -- '/fz/bin/lanes' step") {
			t.Errorf("%s: %s", s.Kind, line)
		}
	}
	if laneParentSlice(ops.steps[0]) != "lanes-c1.slice" || LaneSlice("tp-c2a") != "lanes-tp_c2a.slice" {
		t.Errorf("slice names %q", laneParentSlice(ops.steps[0]))
	}
	// an unreserved host's command line is what it always was
	if detach("/b", "/s/step.json", laneExecPrefix(Step{})) != detach("/b", "/s/step.json") {
		t.Error("an unreserved host's steps changed")
	}
}

func TestLaneCpusMustBeInsideTheBlock(t *testing.T) {
	if !cpusWithin([]int{8, 9}, "8-15") || cpusWithin([]int{8, 16}, "8-15") || cpusWithin([]int{1}, "bad") {
		t.Fatal("cpusWithin")
	}
}

func TestInstallScriptOnlyInstallsTheHelperAndItsRule(t *testing.T) {
	read := func(p string) ([]byte, error) { return []byte("file " + p), nil }
	s, err := installScript("deploy/host", "benchuser", read)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"visudo -cf", "install -m 0755", HelperPath, "ln -sf lane-reserve " + AliasPath, "/etc/sudoers.d/lane-reserve"} {
		if !strings.Contains(s, want) {
			t.Errorf("install script lacks %q", want)
		}
	}
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "sudo ") && !strings.HasPrefix(line, "sudo ") {
			t.Errorf("sudo only at the start of a command: %q", line)
		}
	}
}
