package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestWakeSpinGuardFailsOnImmediatePassesAtOneInstant(t *testing.T) {
	var guard wakeSpinGuard
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var err error
	passes := 0
	for err == nil && passes < 10*wakeSpinPasses {
		err = guard.pass(now, 0, now, "gate probe")
		passes++
	}
	var spin ErrWakeSpin
	if !errors.As(err, &spin) {
		t.Fatalf("no spin error after %d passes at one instant: %v", passes, err)
	}
	if spin.Reason != "gate probe" || passes != wakeSpinPasses {
		t.Fatalf("spin = %+v after %d passes, want reason gate probe after %d", spin, passes, wakeSpinPasses)
	}
}

func TestWakeSpinGuardResetsWhenAPassWaits(t *testing.T) {
	var guard wakeSpinGuard
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for round := 0; round < 5; round++ {
		for i := 0; i < wakeSpinPasses-1; i++ {
			if err := guard.pass(now, 0, now, "detector schedule"); err != nil {
				t.Fatalf("round %d pass %d: %v", round, i, err)
			}
		}
		if err := guard.pass(now, time.Second, now.Add(time.Second), "detector schedule"); err != nil {
			t.Fatalf("waiting pass: %v", err)
		}
	}
}

func TestWakeSpinGuardAllowsImmediatePassesSpreadOverTime(t *testing.T) {
	var guard wakeSpinGuard
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5*wakeSpinPasses; i++ {
		now = now.Add(wakeSpinWindow)
		if err := guard.pass(now, 0, now, "detector schedule"); err != nil {
			t.Fatalf("pass %d spread over %s: %v", i, time.Duration(i)*wakeSpinWindow, err)
		}
	}
}

// A due gate probe of a detector the pass cannot evaluate (a memory revision
// removed it) must be consumed by that pass, or NextWake reports it due forever
// and the run loop spins.
func TestStepConsumesGateProbeOfRemovedDetector(t *testing.T) {
	detector := controllerDetector("kept", time.Hour)
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), time.Unix(0, 0), nil); err != nil {
		t.Fatalf("first step: %v", err)
	}
	due := time.Unix(10, 0)
	// forgetGate drops every probe while no incident is open.
	controller.mu.Lock()
	controller.incidentOpen = true
	controller.mu.Unlock()
	controller.gateProbes["removed"] = due
	if got := controller.NextWake(); !got.Equal(due) {
		t.Fatalf("NextWake = %s, want the gate probe at %s", got, due)
	}
	if err := controller.Step(context.Background(), due, nil); err != nil {
		t.Fatalf("step at the probe time: %v", err)
	}
	if got, why := controller.nextWake(); !got.After(due) {
		t.Fatalf("NextWake after the pass = %s (%s), still due at %s", got, why, due)
	}
}
