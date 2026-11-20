package bench

import (
	"fmt"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// Lane starvation gate (docs/host-safety.md). The host-wide signals can look fine while one lane is starved: another
// tenant thrashes the cpus next to its cpuset, or the lane hits its own memory cap. Probes then get too few samples and
// the run is invalid without anyone knowing. The host probe also reads each lane slice's cgroup pressure (PSI avg60);
// a lane over its threshold gets no new build or run and one alarm fires. What it runs is left alone.

// LanePSI is one lane slice's pressure, avg60 in percent: cpu.pressure "some" and memory.pressure "full".
type LanePSI struct {
	CPUSome float64 `json:"cpu_some"`
	MemFull float64 `json:"mem_full"`
}

// starved says which threshold the reading is over ("" when none).
func (p LanePSI) starved(th HealthThresholds) string {
	switch {
	case p.CPUSome > th.LaneCPUPSISome:
		return fmt.Sprintf("cpu: pressure some avg60 is %.0f%%, over %.0f%%", p.CPUSome, th.LaneCPUPSISome)
	case p.MemFull > th.LaneMemPSIFull:
		return fmt.Sprintf("memory: pressure full avg60 is %.0f%%, over %.0f%%", p.MemFull, th.LaneMemPSIFull)
	}
	return ""
}

// starveState is one lane's gate: why it is starved ("" when not) and the good ticks since the last bad one.
type starveState struct {
	reason string
	good   int
}

const laneKeyPrefix = "lane:"

// laneGate says why lane gets no new steps ("" when it may): crash-looping pods, or starvation.
func (m *Manager) laneGate(lane string) string {
	m.loadHealth()
	if why := m.crash[lane]; why != "" {
		return why
	}
	if why := m.eksWhy[lane]; why != "" {
		return why
	}
	if st := m.starve[lane]; st != nil && st.reason != "" {
		return fmt.Sprintf("lane %s is unhealthy: starved (%s)", lane, st.reason)
	}
	return ""
}

// observeStarvation feeds the host's lane readings to the lanes' gates. A lane with no reading (no slice yet) is left
// as it is. A starved lane is released after good_probes good ticks in a row.
func (m *Manager) observeStarvation(host string, c *Canary) {
	if c == nil || c.Failed != "" {
		return
	}
	th := m.inv.Hosts[host].Health.Thresholds()
	for _, l := range m.sortedLanes() {
		if l.Host != host || l.Gone {
			continue
		}
		p, ok := c.LanePSI[LaneSlice(l.Lane)]
		if !ok {
			continue
		}
		st := m.starve[l.Lane]
		if st == nil {
			st = &starveState{}
			m.starve[l.Lane] = st
		}
		if bad := p.starved(th); bad != "" {
			was := st.reason
			st.reason, st.good = bad, 0
			if was == "" {
				why := m.laneGate(l.Lane)
				m.logf("ALARM %s, no new steps on it (what runs is left alone)", why)
				(&AlarmLog{Root: m.Root, Logf: m.logf}).Raise(plan.Alarm{At: m.now().UTC(), Alarm: "lane-starved", Key: "lane-starved/" + l.Lane, Subject: l.Lane, Host: host, Detail: why})
			}
		} else if st.reason != "" {
			if st.good++; st.good >= th.GoodProbes {
				st.reason, st.good = "", 0
				m.logf("lane %s: pressure is back under its thresholds, new steps resume", l.Lane)
			}
		}
	}
}

func (m *Manager) loadStarve(lane string, r healthRecord) {
	m.starve[lane] = &starveState{reason: r.Reason, good: r.Good}
}

func (m *Manager) saveStarve(recs map[string]healthRecord) {
	for lane, st := range m.starve {
		if st.reason != "" {
			recs[laneKeyPrefix+lane] = healthRecord{Reason: st.reason, Good: st.good}
		}
	}
}
