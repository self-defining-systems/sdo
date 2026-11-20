//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// canaryEnv puts the fake `ip` first in PATH and a hung-task counter file where the script reads it.
func canaryEnv(t *testing.T, hung int) (state string) {
	t.Helper()
	state = t.TempDir()
	t.Setenv("FAKE_STATE", state)
	t.Setenv("PATH", testdata("fakebin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	f := filepath.Join(state, "hung")
	must(t, os.WriteFile(f, []byte(strconv.Itoa(hung)+"\n"), 0o644))
	t.Setenv("HOST_CANARY_HUNG_FILE", f)
	t.Setenv("HOST_CANARY_IP_TIMEOUT", "1")
	return state
}

func TestLocalCanaryReadsIPTimeAndHungTasks(t *testing.T) {
	state := canaryEnv(t, 7)
	c, err := localCanary(context.Background(), 10*time.Second, nil)
	if err != nil || c.Failed != "" || c.HungTasks != 7 || c.IPMillis > 1500 {
		t.Fatalf("%+v %v", c, err)
	}
	must(t, os.WriteFile(filepath.Join(state, "ip-delay"), []byte("0.3"), 0o644))
	c, _ = localCanary(context.Background(), 10*time.Second, nil)
	if c.IPMillis < 250 || c.Failed != "" {
		t.Fatalf("a slow ip must show in ip_ms: %+v", c)
	}
}

// ip that never answers (the jammed lock) is a failed reading after its bound, not a hang of the manager.
func TestLocalCanaryReadsLaneSlicePressure(t *testing.T) {
	canaryEnv(t, 0)
	dir := t.TempDir()
	slice := filepath.Join(dir, LaneSlice("lane-a"))
	must(t, os.MkdirAll(slice, 0o755))
	must(t, os.WriteFile(filepath.Join(slice, "cpu.pressure"), []byte("some avg10=1.00 avg60=71.50 avg300=3.00 total=1\nfull avg10=0.00 avg60=9.00 avg300=0.00 total=0\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(slice, "memory.pressure"), []byte("some avg10=0.00 avg60=50.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=22.25 avg300=0.00 total=0\n"), 0o644))
	t.Setenv("HOST_CANARY_LANES_DIR", dir)
	c, err := localCanary(context.Background(), 10*time.Second, nil)
	got, ok := c.LanePSI[LaneSlice("lane-a")]
	if err != nil || !ok || got.CPUSome != 71.5 || got.MemFull != 22.25 {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestLocalCanaryBoundsAnIPThatHangs(t *testing.T) {
	state := canaryEnv(t, 0)
	must(t, os.WriteFile(filepath.Join(state, "ip-hang"), nil, 0o644))
	start := time.Now()
	c, _ := localCanary(context.Background(), 10*time.Second, nil)
	if c.Failed == "" || !strings.Contains(c.Failed, "ip -br link") {
		t.Fatalf("a hung ip must be a failed reading: %+v", c)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the probe took %s, its bound is 1s for ip", time.Since(start))
	}
}

// Over ssh the canary is taken first with its own bound: a host that does not answer ssh at all costs one probe timeout,
// not the two minutes of an ordinary call, and no other call is made to it.
func TestSSHProbeBoundsTheCanaryAndAsksNothingElseOfAJammedHost(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\necho x >> "+calls+"\nsleep 30\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	h := sshHost{name: "h", h: Host{SSH: "jammed", Staging: filepath.Join(dir, "st")}, health: HostHealthConf{ProbeTimeout: "400ms"}.Thresholds()}
	start := time.Now()
	p, err := h.Probe(context.Background(), map[string]string{}, []string{dir})
	if err == nil || p.Canary == nil || p.Canary.Failed == "" {
		t.Fatalf("a host that does not answer must give a failed canary and an error: %+v %v", p.Canary, err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("took %s", time.Since(start))
	}
	if b, _ := os.ReadFile(calls); strings.Count(string(b), "x") != 1 {
		t.Fatalf("ssh calls: %q, want only the canary", b)
	}
}

func TestSSHProbeCarriesTheCanary(t *testing.T) {
	canaryEnv(t, 3)
	dir := t.TempDir()
	fakeSSH := filepath.Join(dir, "ssh")
	must(t, os.WriteFile(fakeSSH, []byte("#!/bin/sh\nshift\nexec sh -c \"$*\"\n"), 0o755))
	t.Setenv("FUZZOPS_BENCH_SSH", fakeSSH)
	pin := filepath.Join(dir, "pin")
	useFakeLanes(t, "#!/bin/sh\necho '{\"load5\":1,\"free_bytes\":9,\"steps\":{}}'\n")
	h := sshHost{name: "h", h: Host{SSH: "fakehost", Staging: filepath.Join(dir, "st")}}
	p, err := h.Probe(context.Background(), map[string]string{}, []string{pin})
	must(t, err)
	if p.Canary == nil || p.Canary.HungTasks != 3 || p.Canary.Failed != "" {
		t.Fatalf("canary %+v", p.Canary)
	}
}

// A localHost built without thresholds (the lab tests build one) must still take a canary: a zero bound is the default,
// not an instant timeout that would leave every such host unhealthy for good.
func TestLocalHostWithoutThresholdsStillTakesACanary(t *testing.T) {
	canaryEnv(t, 0)
	p, err := localHost{staging: t.TempDir()}.Probe(context.Background(), map[string]string{}, nil)
	if err != nil || p.Canary == nil || p.Canary.Failed != "" {
		t.Fatalf("%+v %v", p.Canary, err)
	}
}
