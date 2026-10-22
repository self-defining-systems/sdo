package runtime

import (
	"encoding/hex"
	"fmt"
	"os"
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
