package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func good(ms, hung int64) *Canary { return &Canary{IPMillis: ms, HungTasks: hung} }

// The gate is a counter over probes, not over time: it needs no clock. Defaults: the host is unhealthy after a probe
// that failed, after two slow `ip` in a row, or when hung tasks appear; it is healthy again after three good probes.
func TestHostHealthStateMachine(t *testing.T) {
	th := HostHealthConf{}.Thresholds()
	type step struct {
		c           *Canary
		wantHealthy bool
		reason      string
	}
	for name, steps := range map[string][]step{
		"fast probes stay healthy":        {{good(5, 0), true, ""}, {good(30, 0), true, ""}},
		"one slow ip is not enough":       {{good(3000, 0), true, ""}, {good(5, 0), true, ""}},
		"two slow ips in a row":           {{good(3000, 0), true, ""}, {good(2500, 0), false, "ip -br link took"}},
		"a failed probe":                  {{&Canary{Failed: "ssh timed out"}, false, "ssh timed out"}},
		"new hung tasks":                  {{good(5, 4), true, ""}, {good(5, 4), true, ""}, {good(5, 6), false, "2 new hung task"}},
		"unknown hung count is no signal": {{good(5, -1), true, ""}, {good(5, -1), true, ""}},
		"no canary data is no signal":     {{nil, true, ""}},
		"three good probes recover": {{&Canary{Failed: "x"}, false, "x"}, {good(5, 0), false, "x"}, {good(5, 0), false, "x"},
			{good(5, 0), true, ""}},
		"a bad probe restarts the recovery": {{&Canary{Failed: "x"}, false, "x"}, {good(5, 0), false, "x"}, {good(5, 0), false, "x"},
			{&Canary{Failed: "y"}, false, "y"}, {good(5, 0), false, "y"}, {good(5, 0), false, "y"}, {good(5, 0), true, ""}},
		"a slow probe while unhealthy is not good": {{&Canary{Failed: "x"}, false, "x"}, {good(5, 0), false, "x"}, {good(9000, 0), false, "x"},
			{good(5, 0), false, "x"}, {good(5, 0), false, "x"}, {good(5, 0), true, ""}},
	} {
		var h healthState
		for i, s := range steps {
			h.Observe(s.c, th)
			if h.Healthy() != s.wantHealthy || !strings.Contains(h.Reason(), s.reason) {
				t.Fatalf("%s: probe %d: healthy=%v reason=%q, want healthy=%v reason containing %q", name, i, h.Healthy(), h.Reason(), s.wantHealthy, s.reason)
			}
		}
	}
}

func TestHostHealthThresholdsComeFromTheInventory(t *testing.T) {
	th := HostHealthConf{IPSlow: "500ms", SlowProbes: 1, GoodProbes: 1}.Thresholds()
	var h healthState
	h.Observe(good(600, 0), th)
	if h.Healthy() {
		t.Fatal("one probe over ip_slow 500ms with slow_probes 1 must be unhealthy")
	}
	h.Observe(good(5, 0), th)
	if !h.Healthy() {
		t.Fatal("good_probes 1: one good probe recovers")
	}
	if d := (HostHealthConf{}).Thresholds(); d.IPSlow != 2*time.Second || d.SlowProbes != 2 || d.GoodProbes != 3 || d.ProbeTimeout != 10*time.Second {
		t.Fatalf("defaults: %+v", d)
	}
	inv := Inventory{Hosts: map[string]Host{"h": {Local: true, Staging: "/s", Kubeconfigs: "/k", MaxBuilds: 1, MaxLoad5: 1, Health: HostHealthConf{IPSlow: "soon"}}}}
	if err := inv.Validate(); err == nil || !strings.Contains(err.Error(), "health.ip_slow") {
		t.Fatalf("a bad duration must be refused: %v", err)
	}
}

// canaryOps answers the manager's probe with a canary read from a script of results.
type canaryOps struct {
	HostOps
	next func() (*Canary, error)
}

func (c canaryOps) Probe(context.Context, map[string]string, []string) (Probe, error) {
	cn, err := c.next()
	return Probe{Canary: cn, Steps: map[string]string{}}, err
}

func healthManager(t *testing.T, script *[]*Canary) *Manager {
	t.Helper()
	root := Root(t.TempDir())
	inv := "hosts:\n  h1: {local: true, staging: /tmp/s, max_builds: 1, max_load5: 100, kubeconfigs: /tmp/k, evidence: /tmp/e, clusters: [c1]}\n"
	if err := os.WriteFile(filepath.Join(string(root), "hosts.yaml"), []byte(inv), 0o644); err != nil {
		t.Fatal(err)
	}
	return &Manager{Root: root, Ops: func(string, Host) HostOps {
		return canaryOps{next: func() (*Canary, error) {
			c := (*script)[0]
			*script = (*script)[1:]
			if c != nil && c.Failed != "" {
				return c, os.ErrDeadlineExceeded
			}
			return c, nil
		}}
	}}
}

// An unhealthy host gets no new lanes or steps, an alarm fires once, and the reason is in `lanes status`; after three
// good probes the host is admitted again. Nothing on the host is touched.
func TestUnhealthyHostStopsNewWorkAndAlarms(t *testing.T) {
	script := []*Canary{good(5, 0), {Failed: "probe timed out after 10s"}, {Failed: "probe timed out after 10s"}, good(5, 0), good(5, 0), good(5, 0)}
	m := healthManager(t, &script)
	pass := func() {
		t.Helper()
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	block := func() string {
		return m.buildBlock("h1", Spec{Hosts: []string{"h1"}}, map[string]bool{"h1": m.admitted("h1")})
	}
	pass()
	if m.hostBlock("h1") != "" {
		t.Fatalf("a good probe: %s", m.hostBlock("h1"))
	}
	pass()
	if why := m.hostBlock("h1"); !strings.Contains(why, "unhealthy") || !strings.Contains(why, "timed out") {
		t.Fatalf("a timed-out probe must stop the host: %q", why)
	}
	pass()
	if got := m.hosts["h1"].Unhealthy; !strings.Contains(got, "timed out") {
		t.Fatalf("lanes status must show the reason: %q", got)
	}
	if why := block(); !strings.Contains(why, "unhealthy") {
		t.Fatalf("no build may start on an unhealthy host: %q", why)
	}
	alarms := RecentAlarms(m.Root, time.Hour, m.now())
	if len(alarms) != 1 || alarms[0].Alarm != "host-unhealthy" || alarms[0].Host != "h1" {
		t.Fatalf("one alarm for the transition, not one per probe: %+v", alarms)
	}
	pass()
	pass()
	if m.hostBlock("h1") == "" {
		t.Fatal("two good probes are not enough")
	}
	pass()
	if m.hostBlock("h1") != "" || m.hosts["h1"].Unhealthy != "" {
		t.Fatalf("three good probes recover: %q", m.hostBlock("h1"))
	}
}
