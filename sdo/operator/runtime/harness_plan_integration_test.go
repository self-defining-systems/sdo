//go:build integration

package runtime

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"time"
)

// The harness runs RunWithOptions, the production controller loop, inside the
// simulated world on virtual time. Everything a seed decides is decided here,
// up front, as a plan: the scenario never draws from a shared random source
// while the controller's goroutines race, so one seed gives one plan.

// detectorBehaviour is what a stand-in detector does in the memory host.
type detectorBehaviour string

const (
	behaveNormal detectorBehaviour = "normal"
	behaveError  detectorBehaviour = "error"
	behavePanic  detectorBehaviour = "panic"
	behaveHang   detectorBehaviour = "hang"
	behaveSlow   detectorBehaviour = "slow"
	behaveWrong  detectorBehaviour = "wrong"
)

// agentMode is what the agent stand-in does with one dispatched incident.
type agentMode string

const (
	agentFix     agentMode = "fix"
	agentNothing agentMode = "nothing"
	agentWorsen  agentMode = "worsen"
	agentCrash   agentMode = "crash"
	agentHang    agentMode = "hang"
	agentGarbage agentMode = "garbage"
)

// planDetector is one detector of a memory revision.
type planDetector struct {
	ID        string
	Interval  time.Duration
	Behaviour detectorBehaviour
}

// planRevision is a memory revision: the detectors it activates at At.
type planRevision struct {
	Name      string
	At        time.Duration
	Detectors []planDetector
}

// plan is everything one seed chooses.
type plan struct {
	Seed      uint64
	Revisions []planRevision
	// Breaks lists when each fault object breaks: detector id and offset.
	Breaks []planBreak
	// Quiesce is when every fault stops; the run lasts Settle longer.
	Quiesce time.Duration
	Settle  time.Duration
	// ControllerCrashes are offsets of whole-controller restarts.
	ControllerCrashes []time.Duration
	// WriteCrashes are the ordinals of control-plane writes after which the
	// controller crashes before it can act on the reply.
	WriteCrashes []int
	// Crashes are deaths at the other effect boundaries (see crashBoundary).
	Crashes     []crashPoint
	HostCrashes []time.Duration
	// Agents is the agent mode of the nth dispatch; later dispatches fix.
	Agents []agentMode
	// APIErrorPercent and CommitThenTimeoutPercent are per write call.
	APIErrorPercent, CommitThenTimeoutPercent, ReadErrorPercent int
	// CacheLag is the longest a watch event is held back (a stale read).
	CacheLag time.Duration
	// ListErrorPercent is the chance an informer list fails; WatchExpiredPercent
	// that a resumed watch is told its version is gone (forcing a relist);
	// WatchCutPercent that a watch connection drops a few seconds in.
	ListErrorPercent, WatchExpiredPercent, WatchCutPercent int
	// Programs is the behaviour of each detector's saved program.
	Programs map[string]planProgram
}

// crashPoints lists every planned death.
func (p plan) crashPoints() []crashPoint {
	var out []crashPoint
	for _, n := range p.WriteCrashes {
		out = append(out, crashPoint{crashControlWriteAfter, n})
	}
	return append(out, p.Crashes...)
}

type planBreak struct {
	Detector string
	At       time.Duration
}

func (p plan) String() string {
	return fmt.Sprintf("seed=%d revisions=%d quiesce=%s crashes=%v boundaryCrashes=%s hostCrashes=%v agents=%v apiErr=%d%% timeout=%d%% readErr=%d%% listErr=%d%% watchGone=%d%% watchCut=%d%% lag=%s",
		p.Seed, len(p.Revisions), p.Quiesce, p.ControllerCrashes, describeCrashes(p.crashPoints()), p.HostCrashes, p.Agents, p.APIErrorPercent, p.CommitThenTimeoutPercent, p.ReadErrorPercent, p.ListErrorPercent, p.WatchExpiredPercent, p.WatchCutPercent, p.CacheLag)
}

// planFor derives a plan from a seed. Seed 0 has no faults at all.
func planFor(seed uint64) plan {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	pick := func(n int) int { return rng.IntN(n) }
	seconds := func(lo, hi int) time.Duration { return time.Duration(lo+pick(hi-lo+1)) * time.Second }
	p := plan{Seed: seed, Quiesce: 90 * time.Second, Settle: 150 * time.Second}

	// The first revision always has an ordinary detector so incidents occur.
	behaviours := []detectorBehaviour{behaveNormal, behaveNormal, behaveError, behavePanic, behaveHang, behaveSlow, behaveWrong}
	names := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	next := 0
	detector := func(force detectorBehaviour) planDetector {
		d := planDetector{ID: names[next%len(names)] + fmt.Sprint(next/len(names)), Interval: seconds(1, 4), Behaviour: behaveNormal}
		next++
		if force != "" {
			d.Behaviour = force
		} else if seed != 0 {
			d.Behaviour = behaviours[pick(len(behaviours))]
		}
		return d
	}
	current := []planDetector{detector(behaveNormal), detector("")}
	p.Revisions = append(p.Revisions, planRevision{Name: "r0", Detectors: append([]planDetector(nil), current...)})
	if seed != 0 {
		for i, swaps := 0, 1+pick(3); i < swaps; i++ {
			switch pick(3) {
			case 0: // add a detector
				current = append(append([]planDetector(nil), current...), detector(""))
			case 1: // drop one, keeping the first
				if len(current) > 1 {
					drop := 1 + pick(len(current)-1)
					current = append(append([]planDetector(nil), current[:drop]...), current[drop+1:]...)
				}
			default: // replace one with a fresh detector
				current = append(append([]planDetector(nil), current...), detector(""))
				current = append(current[:1:1], current[2:]...)
			}
			p.Revisions = append(p.Revisions, planRevision{Name: fmt.Sprintf("r%d", i+1), At: seconds(15, 75), Detectors: append([]planDetector(nil), current...)})
		}
	}
	// Faults break every detector's fault object at least once, so each
	// detector has something to find; a normal detector's break opens an incident.
	seen := map[string]bool{}
	for _, r := range p.Revisions {
		for _, d := range r.Detectors {
			if !seen[d.ID] {
				seen[d.ID] = true
				p.Breaks = append(p.Breaks, planBreak{Detector: d.ID, At: seconds(5, 70)})
			}
		}
	}
	if seed == 0 {
		return p
	}
	for i, n := 0, pick(3); i < n; i++ {
		p.ControllerCrashes = append(p.ControllerCrashes, seconds(10, 80))
	}
	for i, n := 0, pick(3); i < n; i++ {
		p.WriteCrashes = append(p.WriteCrashes, 1+pick(60))
	}
	for i, n := 0, pick(3); i < n; i++ {
		p.HostCrashes = append(p.HostCrashes, seconds(10, 80))
	}
	modes := []agentMode{agentFix, agentFix, agentNothing, agentWorsen, agentCrash, agentHang, agentGarbage}
	for i, n := 0, 2+pick(4); i < n; i++ {
		p.Agents = append(p.Agents, modes[pick(len(modes))])
	}
	p.APIErrorPercent = pick(8)
	p.CommitThenTimeoutPercent = pick(6)
	p.ReadErrorPercent = pick(4)
	p.CacheLag = time.Duration(pick(4)) * time.Second
	// Drawn last, so the choices above are the same as before the cluster moved
	// onto the simulated API server.
	boundaries := []crashPoint{{crashControlWriteBefore, 60}, {crashLeaseAfter, 40}, {crashListBefore, 30}, {crashDispatchBefore, 4}, {crashDispatchAfter, 4}}
	for i, n := 0, pick(3); i < n; i++ {
		b := boundaries[pick(len(boundaries))]
		p.Crashes = append(p.Crashes, crashPoint{b.Boundary, 1 + pick(b.Ordinal)})
	}
	p.ListErrorPercent = pick(4)
	p.WatchExpiredPercent = pick(10)
	p.WatchCutPercent = pick(20)
	// Drawn after everything else, so the plan above is the same as before
	// programs were driven.
	programModes := []programBehaviour{programRight, programRight, programRight, programWrong, programGarbage, programError, programPanic, programHang}
	p.Programs = map[string]planProgram{}
	for _, r := range p.Revisions {
		for _, d := range r.Detectors {
			if _, ok := p.Programs[d.ID]; !ok {
				p.Programs[d.ID] = planProgram{Diagnose: programModes[pick(len(programModes))], Mitigate: programModes[pick(len(programModes))]}
			}
		}
	}
	return p
}

// chance is a deterministic coin that does not depend on call order across
// goroutines: it is a function of the seed, the call site's key and how many
// times that key was seen.
func chance(seed uint64, key string, n int, percent int) bool {
	if percent <= 0 {
		return false
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%s/%d", seed, key, n)
	return int(h.Sum64()%100) < percent
}
