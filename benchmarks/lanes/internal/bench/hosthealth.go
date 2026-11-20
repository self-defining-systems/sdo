package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// Host health gate (docs/host-safety.md, gate 3). Creating or destroying a container takes a network namespace under
// the kernel's global rtnl lock; when the lock jams, dockerd, kubelet and sshd hang with it. The earliest cheap sign
// is that `ip -br link`, which takes the same lock, gets slow, and that the kernel reports hung tasks. The gate probes
// both once per tick. It only stops new work and raises an alarm: it never kills, tears down or resets anything.

// Canary is one bounded reading of a host's kernel health.
type Canary struct {
	// Failed says why the reading could not be taken (the probe timed out, ssh failed): the host counts as unhealthy.
	Failed string `json:"failed,omitempty"`
	// IPMillis is how long `ip -br link` took.
	IPMillis int64 `json:"ip_ms"`
	// HungTasks is the kernel's count of hung-task reports so far (-1: not readable on this host).
	HungTasks int64 `json:"hung"`
	// Sysctl holds the effective value of each kernel setting the host has a minimum for (a setting that cannot be read
	// is absent); Override says, for one below its minimum, where a file sets it (kernel minimums, kernelmin.go).
	Sysctl   map[string]int64  `json:"sysctl,omitempty"`
	Override map[string]string `json:"override,omitempty"`
	// RDMAShared: `rdma system show` says netns shared while ib_core is loaded (a warning, never a gate).
	RDMAShared bool `json:"rdma_shared,omitempty"`
	// MemPSIFull is the host's memory pressure, "full" avg60 in percent (/proc/pressure/memory); 0 when not readable.
	MemPSIFull float64 `json:"mem_psi_full,omitempty"`
}

// HostHealthConf is a host's `health:` block of hosts.yaml; zero values mean the defaults.
type HostHealthConf struct {
	ProbeTimeout string `json:"probe_timeout,omitempty"` // bound of the whole probe (default 10s)
	IPSlow       string `json:"ip_slow,omitempty"`       // `ip -br link` slower than this is slow (default 2s)
	SlowProbes   int    `json:"slow_probes,omitempty"`   // slow probes in a row that make the host unhealthy (default 2)
	GoodProbes   int    `json:"good_probes,omitempty"`
	// MemPSIFull is the memory pressure ("full" avg60, percent) over which the host takes no new work (default 40).
	MemPSIFull float64 `json:"mem_psi_full,omitempty"` // good probes in a row that make it healthy again (default 3)
}

// HealthThresholds are HostHealthConf with the defaults filled in.
type HealthThresholds struct {
	ProbeTimeout, IPSlow   time.Duration
	SlowProbes, GoodProbes int
	MemPSIFull             float64
}

// Thresholds fills in the defaults.
func (c HostHealthConf) Thresholds() HealthThresholds {
	t := HealthThresholds{ProbeTimeout: 10 * time.Second, IPSlow: 2 * time.Second, SlowProbes: 2, GoodProbes: 3, MemPSIFull: 40}
	if d, err := time.ParseDuration(c.ProbeTimeout); err == nil && d > 0 {
		t.ProbeTimeout = d
	}
	if d, err := time.ParseDuration(c.IPSlow); err == nil && d > 0 {
		t.IPSlow = d
	}
	if c.MemPSIFull > 0 {
		t.MemPSIFull = c.MemPSIFull
	}
	if c.SlowProbes > 0 {
		t.SlowProbes = c.SlowProbes
	}
	if c.GoodProbes > 0 {
		t.GoodProbes = c.GoodProbes
	}
	return t
}

func (c HostHealthConf) validate() []string {
	var errs []string
	for f, v := range map[string]string{"probe_timeout": c.ProbeTimeout, "ip_slow": c.IPSlow} {
		if d, err := time.ParseDuration(v); v != "" && (err != nil || d <= 0) {
			errs = append(errs, fmt.Sprintf("health.%s %q: a positive duration such as 2s", f, v))
		}
	}
	if c.MemPSIFull < 0 || c.MemPSIFull > 100 {
		errs = append(errs, "health.mem_psi_full must be a percentage")
	}
	if c.SlowProbes < 0 || c.GoodProbes < 0 {
		errs = append(errs, "health.slow_probes and health.good_probes must not be negative")
	}
	return errs
}

// healthState is one host's gate: the last reading's effect, as counters over probes.
type healthState struct {
	reason     string // "" while healthy
	slow, good int    // slow probes in a row; good probes in a row since the last bad one
	hung       int64  // last hung-task count read (-1: none yet)
	seen       bool
}

func (h *healthState) Healthy() bool  { return h.reason == "" }
func (h *healthState) Reason() string { return h.reason }

// Observe takes one probe. nil means the transport has no canary (a fake host, an old build): no signal, no change.
func (h *healthState) Observe(c *Canary, th HealthThresholds) {
	if c == nil {
		return
	}
	bad := ""
	switch {
	case c.Failed != "":
		bad = "probe failed: " + c.Failed
		h.slow = 0
	default:
		if time.Duration(c.IPMillis)*time.Millisecond > th.IPSlow {
			h.slow++
		} else {
			h.slow = 0
		}
		switch {
		case h.slow >= th.SlowProbes:
			bad = fmt.Sprintf("ip -br link took %dms (over %s) in %d probes in a row", c.IPMillis, th.IPSlow, h.slow)
		case h.seen && c.HungTasks >= 0 && h.hung >= 0 && c.HungTasks > h.hung:
			bad = fmt.Sprintf("%d new hung task reports (kernel count %d)", c.HungTasks-h.hung, c.HungTasks)
		}
		if c.HungTasks >= 0 {
			h.hung = c.HungTasks
		}
		h.seen = true
	}
	switch {
	case bad != "":
		h.reason, h.good = bad, 0
	case h.reason == "":
	case h.slow > 0:
		h.good = 0 // a slow probe is not a good one
	default:
		h.good++
		if h.good >= th.GoodProbes {
			h.reason, h.good = "", 0
		}
	}
}

// canaryBody is run on the host (sh). Every command in it is bounded. It prints "sysctl KEY VALUE", "override KEY
// FILE:LINE:TEXT" and "rdma-shared" lines, and last one JSON line. ip is the rtnl-lock canary; the hung-task count comes
// from the kernel, else from the log if readable. The HOST_CANARY_* variables only exist for tests.
const canaryBody = `for kv in $KMIN; do k=${kv%=*}; m=${kv#*=}
  v=$(timeout 3 sysctl -n "$k" 2>/dev/null) || continue
  echo "sysctl $k $v"
  if [ "$v" -lt "$m" ] 2>/dev/null; then
    timeout 3 grep -HnE "^[[:space:]]*$k[[:space:]]*=" ${HOST_CANARY_SYSCTL_FILES:-/etc/sysctl.conf /etc/sysctl.d/*.conf /run/sysctl.d/*.conf /usr/lib/sysctl.d/*.conf} 2>/dev/null | tail -n 1 | sed "s/^/override $k /"
  fi
done
case $(timeout 3 rdma system show 2>/dev/null) in *"netns shared"*) [ -d /sys/module/ib_core ] && echo rdma-shared ;; esac
[ -r "${HOST_CANARY_PSI_FILE:-/proc/pressure/memory}" ] && awk '/^full/{for(i=1;i<=NF;i++) if($i ~ /^avg60=/){sub("avg60=","",$i); print "psi-mem-full " $i}}' "${HOST_CANARY_PSI_FILE:-/proc/pressure/memory}"
s=$(date +%s%N); timeout ${HOST_CANARY_IP_TIMEOUT:-8} ip -br link >/dev/null 2>&1; rc=$?; e=$(date +%s%N)
h=-1
f=${HOST_CANARY_HUNG_FILE:-/proc/sys/kernel/hung_task_detect_count}
if [ -r "$f" ]; then h=$(cat "$f" 2>/dev/null)
elif o=$(timeout 3 dmesg 2>/dev/null); then h=$(printf %s "$o" | grep -c 'blocked for more than'); fi
echo "{\"rc\":$rc,\"ip_ms\":$(( (e-s)/1000000 )),\"hung\":${h:--1}}"`

// canaryScript is canaryBody told which kernel settings to read (keys are validated names, values numbers).
func canaryScript(mins map[string]int64) string {
	keys := make([]string, 0, len(mins))
	for k := range mins {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var kv []string
	for _, k := range keys {
		kv = append(kv, fmt.Sprintf("%s=%d", k, mins[k]))
	}
	return "KMIN='" + strings.Join(kv, " ") + "'\n" + canaryBody
}

// parseCanary reads the script's output. An ip that timed out (rc 124 or 137) is a failed reading.
func parseCanary(out []byte) (*Canary, error) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var r struct {
		RC   int   `json:"rc"`
		IP   int64 `json:"ip_ms"`
		Hung int64 `json:"hung"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &r); err != nil {
		return nil, fmt.Errorf("canary output %q: %v", out, err)
	}
	c := &Canary{IPMillis: r.IP, HungTasks: r.Hung}
	if r.RC == 124 || r.RC == 137 {
		c.Failed = "ip -br link did not answer in 8s (rtnl lock?)"
	}
	for _, l := range lines[:len(lines)-1] {
		f := strings.Fields(l)
		switch {
		case len(f) == 3 && f[0] == "sysctl":
			if v, err := strconv.ParseInt(f[2], 10, 64); err == nil {
				if c.Sysctl == nil {
					c.Sysctl = map[string]int64{}
				}
				c.Sysctl[f[1]] = v
			}
		case len(f) >= 3 && f[0] == "override":
			if c.Override == nil {
				c.Override = map[string]string{}
			}
			c.Override[f[1]] = strings.Join(f[2:], " ")
		case len(f) == 2 && f[0] == "psi-mem-full":
			c.MemPSIFull, _ = strconv.ParseFloat(f[1], 64)
		case len(f) == 1 && f[0] == "rdma-shared":
			c.RDMAShared = true
		}
	}
	return c, nil
}

// localCanary reads the canary on this machine, bounded by timeout.
func localCanary(ctx context.Context, timeout time.Duration, mins map[string]int64) (*Canary, error) {
	if timeout <= 0 {
		timeout = HostHealthConf{}.Thresholds().ProbeTimeout // a localHost built without thresholds (a test, a fake)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", canaryScript(mins))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if err != nil {
		return &Canary{Failed: fmt.Sprintf("probe timed out or failed after %s: %v", timeout, err), HungTasks: -1}, err
	}
	c, err := parseCanary(out)
	if err != nil {
		return &Canary{Failed: err.Error(), HungTasks: -1}, err
	}
	return c, nil
}

// hostBlock says why host h takes no new lanes or steps because of its health ("" when it may).
func (m *Manager) hostBlock(h string) string {
	if st := m.health[h]; st != nil && !st.Healthy() {
		return fmt.Sprintf("host %s is unhealthy: %s", h, st.Reason())
	}
	if why := m.psiWhy[h]; why != "" {
		return fmt.Sprintf("host %s is not admitted: %s", h, why)
	}
	if why := m.kernelWhy[h]; why != "" {
		return fmt.Sprintf("host %s is not admitted: %s", h, why)
	}
	return ""
}

// admitted is whether host h may take new work as far as its health goes.
func (m *Manager) admitted(h string) bool { return m.hostBlock(h) == "" }

// observeHealth feeds one probe to host name's gate and alarms once when it turns unhealthy.
func (m *Manager) observeHealth(name string, c *Canary) {
	if m.health == nil {
		m.health = map[string]*healthState{}
	}
	st := m.health[name]
	if st == nil {
		st = &healthState{hung: -1}
		m.health[name] = st
	}
	m.observeKernel(name, c)
	was := st.Healthy()
	st.Observe(c, m.inv.Hosts[name].Health.Thresholds())
	switch {
	case was && !st.Healthy():
		m.logf("ALARM host %s: unhealthy, no new lanes or steps until %d good probes: %s", name, m.inv.Hosts[name].Health.Thresholds().GoodProbes, st.Reason())
		(&AlarmLog{Root: m.Root, Logf: m.logf}).Raise(plan.Alarm{At: m.now().UTC(), Alarm: "host-unhealthy", Key: "host-unhealthy/" + name, Subject: name, Host: name, Detail: st.Reason()})
	case !was && st.Healthy():
		m.logf("host %s: healthy again, new lanes and steps resume", name)
	}
}
