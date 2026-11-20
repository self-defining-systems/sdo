package bench

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// Kernel minimums (docs/host-safety.md, gate 4). Kind clusters need many inotify instances and memory maps; a host
// whose sysctl files lower them (a later file wins: /etc/sysctl.conf is applied last) leaves a cluster half started.
// The root helper only ever raises the values (`lanes host adopt|reserve`); admission reads the effective ones.

// DefaultKernelMinimums are the minimums every host has unless its kernel_minimums raises or sets them.
var DefaultKernelMinimums = map[string]int64{
	"fs.inotify.max_user_instances": 8192,
	"fs.inotify.max_user_watches":   1048576,
	"vm.max_map_count":              262144,
}

var sysctlName = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_-]+)+$`)

// KernelMins is the host's minimums: the defaults, with the host's own added or overriding.
func (h Host) KernelMins() map[string]int64 {
	m := map[string]int64{}
	for k, v := range DefaultKernelMinimums {
		m[k] = v
	}
	for k, v := range h.KernelMinimums {
		m[k] = v
	}
	return m
}

func (h Host) kernelValidate() []string {
	var errs []string
	for k, v := range h.KernelMinimums {
		if !sysctlName.MatchString(k) || v <= 0 {
			errs = append(errs, fmt.Sprintf("kernel_minimums %q: a sysctl name such as vm.max_map_count with a positive number", k))
		}
	}
	return errs
}

// kernelLow says which settings read in c are below their minimum, and where a file sets them ("" : none).
func kernelLow(c *Canary, mins map[string]int64) string {
	keys := make([]string, 0, len(c.Sysctl))
	for k := range c.Sysctl {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		min, ok := mins[k]
		if !ok || c.Sysctl[k] >= min {
			continue
		}
		s := fmt.Sprintf("%s is %d, below the minimum %d", k, c.Sysctl[k], min)
		if o := c.Override[k]; o != "" {
			s += " (set by " + o + ")"
		} else {
			s += " (no sysctl file found that sets it)"
		}
		out = append(out, s)
	}
	return strings.Join(out, "; ")
}

// observeKernel applies one reading: the host is not admitted while a setting is below its minimum, an alarm fires when
// that starts (a value that drops while lanes run included), and a shared RDMA netns is a warning.
func (m *Manager) observeKernel(name string, c *Canary) {
	if c == nil || c.Failed != "" {
		return
	}
	if m.kernelWhy == nil {
		m.kernelWhy, m.warns = map[string]string{}, map[string][]string{}
	}
	m.observePSI(name, c)
	was := m.kernelWhy[name]
	why := kernelLow(c, m.inv.Hosts[name].KernelMins())
	m.kernelWhy[name] = why
	switch {
	case why != "" && was == "":
		m.logf("ALARM host %s: kernel setting below its minimum, no new lanes or steps: %s", name, why)
		(&AlarmLog{Root: m.Root, Logf: m.logf}).Raise(plan.Alarm{At: m.now().UTC(), Alarm: "kernel-setting-low", Key: "kernel-setting-low/" + name, Subject: name, Host: name, Detail: why})
	case why == "" && was != "":
		m.logf("host %s: kernel settings are back at their minimums, new lanes and steps resume", name)
	}
	m.warns[name] = nil
	if c.RDMAShared {
		w := "RDMA netns mode is shared with ib_core loaded (container network namespaces can stall; `rdma system set netns exclusive`)"
		m.warns[name] = []string{w}
		if !m.rdmaWarned[name] {
			if m.rdmaWarned == nil {
				m.rdmaWarned = map[string]bool{}
			}
			m.rdmaWarned[name] = true
			m.logf("WARNING host %s: %s", name, w)
		}
	}
}

// observePSI is the memory-pressure gate: while the host's memory pressure ("full" avg60) is over health.mem_psi_full
// it takes no new work. Memory-capped clusters thrashing (their own or another team's) are felt by every lane there.
// There is no hysteresis: avg60 is already a minute's average.
func (m *Manager) observePSI(name string, c *Canary) {
	if m.psiWhy == nil {
		m.psiWhy = map[string]string{}
	}
	max := m.inv.Hosts[name].Health.Thresholds().MemPSIFull
	was := m.psiWhy[name]
	why := ""
	if c.MemPSIFull > max {
		why = fmt.Sprintf("memory pressure (PSI full avg60) is %.0f%%, over %.0f%%", c.MemPSIFull, max)
	}
	m.psiWhy[name] = why
	switch {
	case why != "" && was == "":
		m.logf("ALARM host %s: %s, no new lanes or steps", name, why)
		(&AlarmLog{Root: m.Root, Logf: m.logf}).Raise(plan.Alarm{At: m.now().UTC(), Alarm: "memory-pressure", Key: "memory-pressure/" + name, Subject: name, Host: name, Detail: why})
	case why == "" && was != "":
		m.logf("host %s: memory pressure is back under %.0f%%, new lanes and steps resume", name, max)
	}
}
