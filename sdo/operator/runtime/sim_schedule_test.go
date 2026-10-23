package runtime

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

// Schedule bytes choose restart frequency followed by ten (tick, Service)
// pairs. The seed independently controls API faults and responder behavior.
// Each tick advances ten virtual seconds to bound instrumented fuzz costs.
// Missing bytes are zero; excess bytes are ignored to keep every run bounded.
func runSimSchedule(t *testing.T, seed uint64, raw []byte) {
	t.Helper()
	raw = raw[:min(len(raw), 21)]
	w := newSimWorld(t, seed)
	at := func(i int) byte {
		if i < len(raw) {
			return raw[i]
		}
		return 0
	}
	restartP := float64(at(0)) / 255 * 0.02
	last := 0
	for tick := 0; tick < simStreamMaxTicks/10; tick++ {
		w.clock.now = w.clock.now.Add(10 * time.Second)
		for i := 0; i < 10; i++ {
			injection := 2 + i*20 + int(at(1+2*i))%10
			if tick == injection {
				w.setPort(simServiceName(int(at(2+2*i))%simStreamServices), 9999)
				last = tick
			}
		}
		if w.rng.Float64() < restartP {
			w.restart()
		}
		w.step()
		w.drain()
		if tick > 210 && (w.idle() || w.endedUnverified()) {
			break
		}
	}
	if !w.idle() && !w.endedUnverified() {
		w.violate("stream stalled after ten scheduled injections (last at %d)", last)
	}
	if len(w.violations) > 0 {
		t.Fatalf("seed=%d schedule=%x: %v", seed, raw, w.violations)
	}
	// A terminal unverified incident can leave health firing. Recover it, then
	// require admission of a new independent fault, rather than accepting a quiet
	// but permanently blocked controller as a terminal stream.
	for i := 0; i < simStreamServices; i++ {
		w.setPort(simServiceName(i), 8080)
	}
	w.responder.behavior = func() simBehavior { return simFixAll }
	w.broker.reject = func() bool { return false }
	for i := 0; i < 10; i++ {
		w.clock.now = w.clock.now.Add(10 * time.Second)
		w.step()
		w.drain()
	}
	before := w.responder.calls
	w.setPort(simServiceName(0), 9999)
	for i := 0; i < 60 && (w.responder.calls == before || !w.idle()); i++ {
		w.clock.now = w.clock.now.Add(10 * time.Second)
		w.step()
		w.drain()
	}
	if w.responder.calls == before || !w.idle() || len(w.violations) > 0 {
		t.Fatalf("next incident stalled: seed=%d schedule=%x calls=%d before=%d violations=%v", seed, raw, w.responder.calls, before, w.violations)
	}
}

func TestSimSchedule(t *testing.T) {
	if raw := os.Getenv("SDO_SIM_SCHEDULE"); raw != "" {
		schedule, err := hex.DecodeString(raw)
		if err != nil {
			t.Fatal(err)
		}
		sweepSimSeedsN(t, 1, func(t *testing.T, seed uint64) { runSimSchedule(t, seed, schedule) })
		return
	}
	sweepSimSeedsN(t, 4, func(t *testing.T, seed uint64) {
		for _, restart := range []byte{0, 255} {
			t.Run(fmt.Sprintf("restart=%d", restart), func(t *testing.T) { runSimSchedule(t, seed, []byte{restart}) })
		}
	})
}

func FuzzSimIncidentSchedule(f *testing.F) {
	f.Add(uint64(1), []byte{0})
	f.Add(uint64(81), []byte{0})
	f.Add(uint64(81), []byte{255, 1, 0, 4, 1, 0, 2})
	f.Add(uint64(21), []byte{128, 99, 3, 33, 2})
	f.Fuzz(func(t *testing.T, seed uint64, schedule []byte) { runSimSchedule(t, seed, schedule) })
}

// The health objective covers one component; a second owned, declared Service
// drift joins the same incident. The responder repairs only the health-visible
// component. Read lag and durable restart timing vary across seeded schedules.
type simPartialHealth struct{ fastloopDetector }

func (simPartialHealth) Detect(ctx context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	snapshot := sdktest.Snapshot{NamespaceName: v.Namespace()}
	for _, service := range v.Services() {
		if service.Name == simServiceName(0) {
			snapshot.ServiceList = append(snapshot.ServiceList, service)
		}
	}
	return (fastloopDetector{}).Detect(ctx, snapshot)
}

func TestSimSchedulePartialRepairCannotVerifyDrift(t *testing.T) {
	sweepSimSeedsN(t, 4, func(t *testing.T, seed uint64) {
		for _, restartAfter := range []int{0, 1, 5} {
			t.Run(fmt.Sprintf("restart_after=%d", restartAfter), func(t *testing.T) {
				w := newSimWorld(t, seed)
				// Isolate the closure invariant from separately tested ambiguous store
				// writes. Keep the seeded informer lag on the responder's one repair.
				w.client.ReactionChain = w.client.ReactionChain[2:]
				w.responder.behavior = func() simBehavior { return simFixFirst }
				w.broker.reject = func() bool { return false }
				w.detectors = []sdk.Detector{simPartialHealth{}}
				w.configure = func(c *Controller) {
					c.ConfigureDrift(DriftControllerConfig{View: func(s sdk.DetectionContext) program.ReadView {
						return LiveReadView{DetectionContext: s, Source: planFakeSource{source: simServiceNamed(simServiceName(1), 8080)}, SourceCommit: "c"}
					}})
				}
				w.restart()
				w.setPort(simServiceName(0), 9999)
				w.setPort(simServiceName(1), 9999)
				w.clock.now = w.clock.now.Add(time.Second)
				w.step()
				// Admission evidence is synthetic and source-grounded, like the watch
				// classifier's owned-field event; no benchmark verdict reaches closure.
				w.controller.queueUnexplainedFinding(w.clock.now, sdk.DriftEvent{
					Ref: simServiceRef(simServiceName(1)), Source: "differs from accepted manifest", ObservedAt: w.clock.now, Classification: sdk.DriftUnexplained,
					Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", Old: "8080", New: "9999", Source: "8080"}},
				})
				w.persist()
				w.drain()
				// Establish that the intended partial repair really happened.
				if faulty := w.faulty(); len(faulty) != 1 || faulty[0] != simServiceName(1) {
					t.Fatalf("partial repair fixture: %v", faulty)
				}
				for tick := 1; tick <= 200; tick++ {
					w.clock.now = w.clock.now.Add(time.Second)
					if tick == restartAfter {
						state := exportState(t, w.controller)
						if !state.IncidentOpen || !state.ResponderDone {
							t.Fatal("restart was not during verification")
						}
						w.restart()
					}
					w.step()
					w.drain()
					if len(w.closed) > 0 {
						break
					}
				}
				if len(w.closed) != 1 || closureOutcome(t, w.closed[0]) != "failed" {
					t.Fatalf("partial repair closed as success or stalled: %+v", w.closed)
				}
				closure := w.closed[0]
				if _, healthy := w.controller.latestEvaluations(w.controller.healthDetectorIDs, closure.ResponderCompletedAt, true); !healthy {
					t.Fatal("fixture never cleared health independently of routing drift")
				}
				if closure.VerifiedAt.Before(closure.ResponderCompletedAt.Add(w.config.VerificationTimeout)) {
					t.Fatal("closed before the existing verification timeout")
				}
				if len(closure.Request.DriftEvents) != 1 {
					t.Fatal("restart lost admitted drift evidence")
				}
				if w.responder.calls != 1 {
					t.Fatalf("unexpected extra responder: calls=%d", w.responder.calls)
				}
				stored, err := w.outcomes.Get(context.Background(), closure.Request.IncidentID)
				if err != nil || stored.Record.GetClassification() != "failed" || len(stored.Record.GetDriftEvents()) != 1 {
					t.Fatalf("durable failed outcome lost drift: %v %v", stored, err)
				}
				if len(w.violations) > 0 {
					t.Fatal(w.violations)
				}
			})
		}
	})
}
