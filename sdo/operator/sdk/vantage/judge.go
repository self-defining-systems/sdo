package vantage

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
)

// Condition names what a state probe reading shows.
type Condition string

const (
	// ConditionNetem: a netem qdisc shapes the Pod's traffic (delay, loss).
	ConditionNetem Condition = "netem-shaping"
	// ConditionFDLimitLow: a process's open-files soft limit is a quarter of
	// the healthy baseline's or less, or below LowFDLimit.
	ConditionFDLimitLow Condition = "fd-limit-low"
	// ConditionFDExhausted: a process holds FDUseBound percent of its limit.
	ConditionFDExhausted Condition = "fd-exhausted"
	// ConditionStopped: a process is stopped (SIGSTOP or a tracer).
	ConditionStopped Condition = "process-stopped"
	// ConditionZombies: zombies grew more than ZombieBound over the baseline.
	ConditionZombies Condition = "process-zombies"
	// ConditionCPULimited: the container's CPU limit is new or a quarter of
	// the baseline's or less, or CPU pressure exceeds PressureBound.
	ConditionCPULimited Condition = "cpu-throttled"
	// ConditionMemoryPressure: OOM kills increased, use reached UseBound, or
	// memory pressure exceeds PressureBound.
	ConditionMemoryPressure Condition = "memory-pressure"
	// ConditionIOLimited: an io.max limit the baseline did not have, or IO
	// pressure above PressureBound.
	ConditionIOLimited Condition = "io-throttled"
	// ConditionShmFull: /dev/shm use reached UseBound.
	ConditionShmFull Condition = "shm-full"
	// ConditionSocketBuildup: sockets to one peer, or in CLOSE_WAIT, grew
	// SocketFactor-fold over the baseline and past SocketFloor.
	ConditionSocketBuildup Condition = "socket-buildup"
)

// Judgement bounds.
const (
	LowFDLimit    = 64
	FDUseBound    = 90.0
	ZombieBound   = 5
	PressureBound = 50.0
	UseBound      = 95.0
	SocketFactor  = 5
	SocketFloor   = 50
)

// Judgement is one condition a reading shows, with its evidence.
type Judgement struct {
	Condition Condition
	Evidence  string
}

// Judge reports the conditions a state reading shows, against base (the same
// probe's healthy baseline reading, zero when there is none). Dialing kinds
// are judged by the link rules (ClientLinks), not here. A reading with an
// error shows nothing.
func Judge(reading, base Reading) []Judgement {
	if reading.Error != "" {
		return nil
	}
	var out []Judgement
	add := func(condition Condition, format string, args ...any) {
		out = append(out, Judgement{Condition: condition, Evidence: fmt.Sprintf(format, args...)})
	}
	hasBase := base.Kind == reading.Kind && base.Error == ""
	switch reading.Kind {
	case Qdisc:
		if devices, ok := reading.Fact(FactNetemDevs); ok {
			if old, was := base.Fact(FactNetemDevs); !hasBase || !was || old != devices {
				add(ConditionNetem, "netem shapes the traffic of %s: %s", devices, reading.Facts[FactNetemLines])
			}
		}
	case ProcLimits:
		if soft, ok := reading.Int(FactNofileSoftMin); ok {
			baseSoft, baseOK := base.Int(FactNofileSoftMin)
			switch {
			case hasBase && baseOK && soft*4 <= baseSoft:
				add(ConditionFDLimitLow, "process %s (%s) may open %d files, a quarter or less of the healthy %d", reading.Facts[FactNofileMinPID], reading.Facts[FactNofileMinComm], soft, baseSoft)
			case soft < LowFDLimit:
				add(ConditionFDLimitLow, "process %s (%s) may open only %d files", reading.Facts[FactNofileMinPID], reading.Facts[FactNofileMinComm], soft)
			}
		}
		if pct, ok := reading.Float(FactFDUsePctMax); ok && pct >= FDUseBound {
			add(ConditionFDExhausted, "a process holds %.0f%% of its open-files limit (%s descriptors)", pct, reading.Facts[FactFDsMax])
		}
	case ProcState:
		if n, _ := reading.Int(FactStopped); n > 0 {
			add(ConditionStopped, "%d process(es) stopped: %s", n, reading.Facts[FactStoppedPIDs])
		}
		zombies, _ := reading.Int(FactZombie)
		baseZombies, _ := base.Int(FactZombie)
		if zombies-baseZombies > ZombieBound {
			add(ConditionZombies, "%d zombie processes (healthy %d): %s", zombies, baseZombies, reading.Facts[FactZombiePIDs])
		}
	case CgroupCPU:
		limit, limited := reading.Int(FactCPULimitMillis)
		baseLimit, baseLimited := base.Int(FactCPULimitMillis)
		switch {
		case limited && hasBase && !baseLimited:
			add(ConditionCPULimited, "the container's CPU is limited to %dm (cpu.max %q); the healthy baseline had no limit", limit, reading.Facts[FactCPUMax])
		case limited && hasBase && baseLimited && limit*4 <= baseLimit:
			add(ConditionCPULimited, "the container's CPU limit is %dm, a quarter or less of the healthy %dm", limit, baseLimit)
		default:
			if p, ok := reading.Float(FactPressureSome10); ok && p >= PressureBound {
				add(ConditionCPULimited, "CPU pressure some avg10 %.1f%% (cpu.max %q, throttled in %s%% of periods)", p, reading.Facts[FactCPUMax], reading.Facts[FactCPUThrottledPct])
			}
		}
	case CgroupMemory:
		kills, _ := reading.Int(FactOOMKill)
		baseKills, _ := base.Int(FactOOMKill)
		use, useOK := reading.Float(FactMemoryUsePct)
		pressure, pressureOK := reading.Float(FactPressureSome10)
		switch {
		case hasBase && kills > baseKills:
			add(ConditionMemoryPressure, "%d OOM kill(s) since the healthy baseline (limit %s bytes)", kills-baseKills, reading.Facts[FactMemoryMax])
		case useOK && use >= UseBound:
			add(ConditionMemoryPressure, "memory use is %.1f%% of the %s-byte limit", use, reading.Facts[FactMemoryMax])
		case pressureOK && pressure >= PressureBound:
			add(ConditionMemoryPressure, "memory pressure some avg10 %.1f%%", pressure)
		}
	case CgroupIO:
		limits, limited := reading.Fact(FactIOMax)
		old, _ := base.Fact(FactIOMax)
		if limited && (!hasBase || old != limits) {
			add(ConditionIOLimited, "the container's IO is limited: io.max %q", limits)
		} else if p, ok := reading.Float(FactPressureFull10); ok && p >= PressureBound {
			add(ConditionIOLimited, "IO pressure full avg10 %.1f%%", p)
		}
	case ShmUsage:
		if pct, ok := reading.Float(FactShmUsedPct); ok && pct >= UseBound {
			add(ConditionShmFull, "/dev/shm is %.1f%% full (%s of %s KiB)", pct, reading.Facts[FactShmUsedKB], reading.Facts[FactShmSizeKB])
		}
	case SocketSummary:
		var grown []string
		for key, value := range reading.Facts {
			peer, ok := strings.CutPrefix(key, "peer.")
			if !ok {
				continue
			}
			now, before := 0, 0
			for _, count := range reading.PeerStates(peer) {
				now += count
			}
			for _, count := range base.PeerStates(peer) {
				before += count
			}
			if now >= SocketFloor && now >= SocketFactor*max(before, 1) {
				grown = append(grown, fmt.Sprintf("%s (%s; healthy %d)", peer, value, before))
			}
		}
		closeWait, _ := reading.Int("state.CLOSE_WAIT")
		baseCloseWait, _ := base.Int("state.CLOSE_WAIT")
		if closeWait >= SocketFloor && closeWait >= SocketFactor*max(baseCloseWait, 1) {
			grown = append(grown, fmt.Sprintf("%d sockets in CLOSE_WAIT (healthy %d)", closeWait, baseCloseWait))
		}
		sort.Strings(grown)
		if len(grown) > 0 {
			add(ConditionSocketBuildup, "sockets built up: %s", strings.Join(grown, "; "))
		}
	}
	return out
}

// RuleIDPrefix prefixes the rule ID of each state finding
// ("vantage.<probe>.<condition>").
const RuleIDPrefix = "vantage."

type stateDetector struct {
	spec   sdk.DetectorSpec
	probes []Probe
}

// NewDetector returns a detector that runs the given state probes and reports
// one finding per Pod and condition Judge finds, primary resource the Pod and
// related resource the probe's workload when it names one. A probe without
// any answer is a detector error, never a clear. Dialing probes are refused:
// judge them with NewClientLinkDetector.
func NewDetector(spec sdk.DetectorSpec, probes ...Probe) (sdk.Detector, error) {
	for _, probe := range probes {
		if probe.Kind.Dials() {
			return nil, fmt.Errorf("vantage probe %q dials; judge it with NewClientLinkDetector", probe.ID)
		}
	}
	d := stateDetector{spec: spec, probes: probes}
	return d, ValidateDetector(d)
}

func (d stateDetector) Spec() sdk.DetectorSpec { return d.spec }
func (d stateDetector) VantageProbes() []Probe { return d.probes }

func (d stateDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, probe := range d.probes {
		result, err := Require(view, probe.ID)
		if err != nil {
			return nil, err
		}
		for _, reading := range result.Readings {
			base, _ := result.BaselineFor(reading)
			for _, judged := range Judge(reading, base) {
				finding := sdk.Finding{
					DetectorID: d.spec.ID, RuleID: RuleIDPrefix + probe.ID + "." + string(judged.Condition),
					Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
					Summary:         fmt.Sprintf("Pod %s: %s", reading.Pod, judged.Condition),
					Evidence:        fmt.Sprintf("vantage %s probe %s inside Pod %s (container %s): %s", probe.Kind, probe.ID, reading.Pod, reading.Container, judged.Evidence),
					PrimaryResource: reading.Ref(), Playbooks: append([]string(nil), d.spec.Playbooks...),
					Fingerprint: d.spec.ID + "/" + RuleIDPrefix + probe.ID + "." + string(judged.Condition) + "/" + reading.Pod,
					Metadata:    map[string]any{"probe": probe.ID, "kind": string(probe.Kind), "condition": string(judged.Condition), "pod": reading.Pod, "container": reading.Container},
				}
				if kind, name, ok := strings.Cut(probe.From.Workload, "/"); ok {
					finding.RelatedResources = []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: kind, Namespace: reading.Namespace, Name: name}}
				}
				findings = append(findings, finding)
			}
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
}
