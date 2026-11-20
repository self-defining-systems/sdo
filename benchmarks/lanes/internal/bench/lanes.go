package bench

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
)

// HostLanes is a host's `lanes:` block: which CPUs lanes may use, and whether measured runs reserve them.
//
//	lanes: {cpus: "4-55", reserve: true}
//
// CPUs is a Linux CPU list ("4-55", "0-7,16-23"): lanes get disjoint whole cores from it, and the rest of the host
// (cpus 0-3 here) is left to the system and to anything that is not a lane. Without it lanes are not limited.
type HostLanes struct {
	CPUs string `json:"cpus,omitempty"`
	// Reserve asks the host's optional root helper (sdo-eval-reserve, deploy/host/) to give the CPUs to the lanes
	// alone while a measured run is active. Without the helper the bench logs "host not reserved" and goes on.
	Reserve bool `json:"reserve,omitempty"`
	// MemoryMin is a memory.min the helper sets on the lanes' parent slice while reserved ("40G").
	MemoryMin string `json:"memory_min,omitempty"`
	// Memory is the most the lanes' memory totals may add up to on the host ("150G"); a lane that would exceed it
	// waits. Default: 85 % of the host's RAM.
	Memory string `json:"memory,omitempty"`
	// State is the host directory of the allocator's ledger (default DefaultLedgerDir: one per user, private to the
	// user, the same for every bench of that user on the machine; change it only for all of them). It must be owned
	// by the user running the bench and not writable by group or others, or the bench refuses to use it.
	State string `json:"state,omitempty"`
	// StateShared trusts State although others can write it (several accounts sharing one ledger): the directory
	// and files are made open to everyone, so every account that can write them can make lanes wait forever or be
	// handed overlapping cores. It needs State.
	StateShared bool `json:"state_shared,omitempty"`
}

// Enabled reports whether lanes on the host are given cores.
func (l HostLanes) Enabled() bool { return l.CPUs != "" }

// CPUList is the lanes' CPUs.
func (l HostLanes) CPUList() ([]int, error) { return ParseCPUList(l.CPUs) }

func (l HostLanes) validate() []string {
	var errs []string
	if l.CPUs != "" {
		if _, err := ParseCPUList(l.CPUs); err != nil {
			errs = append(errs, "lanes.cpus: "+err.Error())
		}
	}
	if l.Reserve && l.CPUs == "" {
		errs = append(errs, "lanes: reserve needs lanes.cpus")
	}
	if _, err := ParseSize(l.MemoryMin); err != nil {
		errs = append(errs, "lanes.memory_min: "+err.Error())
	}
	if _, err := ParseSize(l.Memory); err != nil {
		errs = append(errs, "lanes.memory: "+err.Error())
	}
	if l.State != "" && !path.IsAbs(l.State) {
		errs = append(errs, "lanes.state must be an absolute path")
	}
	if l.StateShared && l.State == "" {
		errs = append(errs, "lanes.state_shared needs lanes.state (the per-user default is never shared)")
	}
	return errs
}

// LaneLimit is what one lane may use: whole physical cores (all their hardware threads) and memory in total.
type LaneLimit struct {
	CPUs   int    `json:"cpus"`
	Memory string `json:"memory"`
}

// DefaultLaneLimit is what a lane of the app at path gets when its spec says nothing. The light apps measured
// 2-4 cores and 4-6 GiB. A GitLab lane is busy on about 8 cores during a rollout and holds 11-13 GiB of anonymous
// memory in a quiet window, but with an operator beside it and its build's page cache it needs more: at 20Gi (two
// workers of 8.5 GiB) a GitLab run with an attached operator had 6 processes OOM-killed inside its nodes, so a
// GitLab lane gets 30Gi (workers of 12.75 GiB).
func DefaultLaneLimit(app string) LaneLimit {
	if strings.Contains(strings.ToLower(path.Base(strings.TrimRight(app, "/"))), "gitlab") {
		return LaneLimit{CPUs: 8, Memory: "30Gi"}
	}
	return LaneLimit{CPUs: 5, Memory: "12Gi"}
}

// LaneLimit is the spec's lane limit, or its app's default.
func (s Spec) LaneLimit() LaneLimit {
	if s.Lane != nil {
		return *s.Lane
	}
	return DefaultLaneLimit(s.App)
}

func (l LaneLimit) validate() []string {
	var errs []string
	if l.CPUs < 1 {
		errs = append(errs, "lane.cpus must be at least 1 (whole cores)")
	}
	if n, err := ParseSize(l.Memory); err != nil || n <= 0 {
		errs = append(errs, fmt.Sprintf("lane.memory %q: a size such as 12Gi", l.Memory))
	}
	return errs
}

// ParseCPUList reads a Linux CPU list: "4-7", "0-3,8,10-11".
func ParseCPUList(s string) ([]int, error) {
	var out []int
	seen := map[int]bool{}
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("empty cpu list")
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		b := a
		if err == nil && isRange {
			b, err = strconv.Atoi(hi)
		}
		if err != nil || a < 0 || b < a {
			return nil, fmt.Errorf("cpu list %q: bad part %q (want 4-7,10)", s, part)
		}
		for c := a; c <= b; c++ {
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
	}
	sort.Ints(out)
	return out, nil
}

// FormatCPUList is the Linux CPU list of cpus ("4-6,8,10-11").
func FormatCPUList(cpus []int) string {
	c := append([]int(nil), cpus...)
	sort.Ints(c)
	var parts []string
	for i := 0; i < len(c); {
		j := i
		for j+1 < len(c) && c[j+1] == c[j]+1 {
			j++
		}
		if j == i {
			parts = append(parts, strconv.Itoa(c[i]))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", c[i], c[j]))
		}
		i = j + 1
	}
	return strings.Join(parts, ",")
}
