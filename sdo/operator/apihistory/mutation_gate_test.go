package apihistory

import (
	"strings"
	"testing"
	"time"
)

func TestStoppedScriptDoesNotBlockFence(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gate.ScriptJob = "runner"
	lease, err := gate.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.file.Close(); err != nil {
		t.Fatal(err)
	} // Runner died before Finish.
	gate.ScriptJobStopped = func(job string) bool { return job == "runner" }
	observation, err := gate.TryObserve()
	if err != nil {
		t.Fatal(err)
	}
	defer observation.Close()
	if err := observation.Fence(time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
}

// The responder's compact output cuts the message at 120 bytes.
func TestVerificationWarningLeadsWithTheRetryInstruction(t *testing.T) {
	message := VerificationWarning(time.Unix(1, 1)).Message
	if !strings.Contains(message[:110], "retry if `sdo detect` shows recovered: no") {
		t.Fatal(message)
	}
}

func TestASealedOrFencedIncidentNeverSaysWritesAreAccepted(t *testing.T) {
	for name, end := range map[string]func(*MutationObservation) error{
		"sealed": (*MutationObservation).Seal,
		"fenced": func(o *MutationObservation) error { return o.Fence(time.Unix(1, 0)) },
	} {
		gate, err := PrepareMutationGate(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := gate.Withdraw("recovery withdrawn", "alert"); err != nil {
			t.Fatal(err)
		}
		if reason, alert := gate.Withdrawn(); reason != "recovery withdrawn" || alert != "alert" {
			t.Fatal(reason, alert)
		}
		observation, err := gate.TryObserve()
		if err != nil || observation == nil {
			t.Fatal(err)
		}
		if err := end(observation); err != nil {
			t.Fatal(err)
		}
		observation.Close()
		if reason, _ := gate.Withdrawn(); reason != "" {
			t.Errorf("%s: %q", name, reason)
		}
	}
}
