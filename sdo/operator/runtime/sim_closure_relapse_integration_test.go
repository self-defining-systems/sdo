//go:build integration

package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// simPersistentDetector is the fastloop health detector with its own clearing
// persistence, which the tracker honours over the controller's default.
type simPersistentDetector struct {
	fastloopDetector
	clearing int
}

func (d simPersistentDetector) Spec() sdk.DetectorSpec {
	spec := d.fastloopDetector.Spec()
	spec.Persistence.Clearing = d.clearing
	return spec
}

// simEvidenceGate makes closure evidence slow in virtual time: a gather blocks
// until the test releases it, while the clock and the step loop keep running.
type simEvidenceGate struct {
	mu      sync.Mutex
	pending []chan struct{}
	started int
}

func (g *simEvidenceGate) gather(ctx context.Context, _ closureEvidenceInput) (closureEvidence, error) {
	release := make(chan struct{})
	g.mu.Lock()
	g.pending = append(g.pending, release)
	g.started++
	g.mu.Unlock()
	select {
	case <-release:
	case <-ctx.Done():
		return closureEvidence{}, ctx.Err()
	}
	return closureEvidence{}, nil
}

func (g *simEvidenceGate) waiting() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.pending)
}

func (g *simEvidenceGate) releaseAll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, release := range g.pending {
		close(release)
	}
	g.pending = nil
}

// gathering reports that the open incident's closure evidence is in flight.
func (w *simWorld) gathering() bool {
	w.controller.mu.Lock()
	defer w.controller.mu.Unlock()
	return w.controller.closurePrep != nil && !w.controller.closurePrep.done
}

// relapse breaks one currently healthy Service, as a foreign actor would.
func (w *simWorld) relapse(rng interface{ IntN(int) int }) string {
	healthy := make([]string, 0, simStreamServices)
	for i := 0; i < simStreamServices; i++ {
		name := simServiceName(i)
		ports, _, _ := unstructuredPorts(w.kube.objects[simServiceRef(name)].Object)
		if ports[0].(map[string]any)["targetPort"] != int64(9999) {
			healthy = append(healthy, name)
		}
	}
	if len(healthy) == 0 {
		return ""
	}
	name := healthy[rng.IntN(len(healthy))]
	w.setPort(name, 9999)
	return name
}

// flapIncidentService breaks the Service the pending closure's own incident
// was about and is now repaired: the finding the recovery trackers hold.
func (w *simWorld) flapIncidentService() string {
	effect, ok := w.controller.PendingClosureEffect()
	if !ok {
		return ""
	}
	for _, finding := range effect.Closure.Request.Findings {
		ref := finding.PrimaryResource
		if ref.Kind != "Service" {
			continue
		}
		if ports, _, _ := unstructuredPorts(w.kube.objects[simServiceRef(ref.Name)].Object); ports[0].(map[string]any)["targetPort"] != int64(9999) {
			w.setPort(ref.Name, 9999)
			return ref.Name
		}
	}
	return ""
}

// The verified-close invariants of simWorld ("never closed verified while a
// health detector still fires", "never committed inside the stability window")
// only bind if health can break after the first clear. This schedule breaks it
// at the three places a closure is still revocable:
//
//   - a blip right after the repair, shorter than the stability window;
//   - a fault landing while slow closure evidence gathers off the step loop;
//   - a fault landing after the closure is persisted, before the controller
//     rechecks it for the broker, either persisting or flapping back to health.
//
// In each, a verified closure of the first incident must not be committed.
func TestSimClosureRelapseSchedules(t *testing.T) {
	sweepSimSeedsN(t, 24, func(t *testing.T, seed uint64) {
		w := newSimWorld(t, seed)
		rr := newSimRNG(seed ^ 0x9e3779b97f4a7c15)
		blipP, evidenceP, beforeP, flapP := 0.3+0.5*rr.Float64(), 0.3+0.5*rr.Float64(), 0.1+0.3*rr.Float64(), 0.3+0.5*rr.Float64()
		// Clearing persistence set by the detector's own spec, sometimes longer
		// than the 15 s stability window: then only the recovery trackers still
		// hold a recent fault against the closure.
		clearing := 1 + rr.IntN(3)
		if rr.Float64() < 0.6 {
			clearing = 18 + rr.IntN(15)
		}
		w.detectors = []sdk.Detector{simPersistentDetector{clearing: clearing}}
		w.client.ReactionChain = w.client.ReactionChain[2:]
		w.responder.behavior = func() simBehavior {
			if rr.Float64() < 0.1 {
				return simFixFirst
			}
			return simFixAll
		}
		gate := &simEvidenceGate{}
		w.configure = func(c *Controller) { c.closureEvidence = gate.gather }
		w.restart()
		w.beforeClosure = func() {
			switch x := rr.Float64(); {
			case x < beforeP:
				w.relapse(rr)
			case x < beforeP+flapP:
				// A flap: health breaks, the controller observes it, and it is
				// healthy again before the recheck. Only the recovery trackers
				// and the stability window still know it happened.
				if name := w.flapIncidentService(); name != "" || rr.Float64() < 0.3 {
					if name == "" {
						name = w.relapse(rr)
					}
					if name == "" {
						return
					}
					w.clock.Advance(time.Second)
					w.step()
					w.setPort(name, 8080)
				}
			}
		}
		t.Cleanup(gate.releaseAll)

		injections := 2 + rr.IntN(3)
		injectAt := make([]int, injections)
		for i := range injectAt {
			injectAt[i] = 2 + rr.IntN(300)
		}
		type blip struct {
			start, end int
			name       string
		}
		var blips []blip
		calls, held, holdFor, relapseAt := 0, 0, 0, -1
		for tick := 0; tick < simStreamMaxTicks; tick++ {
			w.clock.Advance(time.Second)
			for _, at := range injectAt {
				if at == tick {
					w.setPort(simServiceName(rr.IntN(simStreamServices)), 9999)
				}
			}
			kept := blips[:0]
			for _, b := range blips {
				switch {
				case tick == b.start:
					w.setPort(b.name, 9999)
					kept = append(kept, b)
				case tick == b.end:
					w.setPort(b.name, 8080)
				default:
					kept = append(kept, b)
				}
			}
			blips = kept
			w.step()
			if w.gathering() {
				w.await(func() bool { return gate.waiting() > 0 })
				if held == 0 {
					holdFor, relapseAt = rr.IntN(40), -1
					if rr.Float64() < evidenceP {
						relapseAt = rr.IntN(holdFor + 1)
					}
				}
				if held == relapseAt {
					w.relapse(rr)
				}
				if held++; held > holdFor {
					gate.releaseAll()
					w.await(func() bool { return !w.gathering() })
					held = 0
				}
			} else {
				held = 0
			}
			w.drain()
			if w.responder.calls > calls {
				calls = w.responder.calls
				if rr.Float64() < blipP {
					start := tick + 1 + rr.IntN(12)
					blips = append(blips, blip{start: start, end: start + 1 + rr.IntN(5), name: simServiceName(rr.IntN(simStreamServices))})
				}
			}
			if tick > 400 && len(blips) == 0 && (w.idle() || w.endedUnverified()) {
				break
			}
		}
		if !w.idle() && !w.endedUnverified() {
			_, closing := w.controller.PendingIncidentClosure()
			w.controller.mu.Lock()
			prep := w.controller.closurePrep
			w.controller.mu.Unlock()
			w.violate("stalled: incident open=%t closing=%t prep=%v gate=%d faulty=%v closed=%d calls=%d", w.controller.IncidentOpen(), closing, prep != nil, gate.waiting(), w.faulty(), len(w.closed), w.responder.calls)
		}
		if len(w.violations) > 0 {
			t.Fatalf("invariants violated (replay with SDO_SIM_SEED=%d):\n%v", seed, w.violations)
		}
	})
}

// Clearing persistence outlasts the 15 s stability window, and the repaired
// Service flaps once before its closure executes. The window alone would let
// the closure commit; only the recovery trackers, which still hold the
// finding, keep it open until the detector has cleared for its full streak.
func TestSimClosureFlapHeldByRecoveryTrackers(t *testing.T) {
	for _, clearing := range []int{25, 40} {
		t.Run(fmt.Sprint(clearing), func(t *testing.T) {
			w := newSimWorld(t, 61)
			w.client.ReactionChain = w.client.ReactionChain[2:]
			w.responder.behavior = func() simBehavior { return simFixAll }
			w.broker.reject = func() bool { return false }
			w.detectors = []sdk.Detector{simPersistentDetector{clearing: clearing}}
			w.restart()
			flapped := 0
			w.beforeClosure = func() {
				if flapped >= 2 {
					return
				}
				if name := w.flapIncidentService(); name != "" {
					flapped++
					w.clock.Advance(time.Second)
					w.step()
					w.setPort(name, 8080)
				}
			}
			w.setPort(simServiceName(0), 9999)
			for tick := 0; tick < 600 && !w.idle(); tick++ {
				w.clock.Advance(time.Second)
				w.step()
				w.drain()
			}
			verified := 0
			for _, closure := range w.closed {
				if len(closure.FinalDetectorStates) > 0 {
					verified++
				}
			}
			if flapped == 0 || verified == 0 || !w.idle() {
				t.Fatalf("flaps=%d verified closures=%d idle=%t: the scenario no longer reaches a flapped, verified closure", flapped, verified, w.idle())
			}
			if len(w.violations) > 0 {
				t.Fatal(w.violations)
			}
		})
	}
}
