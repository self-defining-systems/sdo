package apihistory

import (
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
